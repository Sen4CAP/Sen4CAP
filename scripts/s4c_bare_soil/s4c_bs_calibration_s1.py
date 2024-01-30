#!/usr/bin/env python
import argparse
from glob import glob
import datetime
from math import nan
from datetime import date, datetime, time
from time import strftime
import random
import pandas as pd
import numpy as np
import json
from json import JSONDecodeError
import os
import re
from pyarrow import ipc
import datetime as dt
import time


ID_COL_NAME = "NewID"

markers_sar_main = ["ASC_VV_BCK_MEAN","ASC_VV_COHE_MEAN","ASC_VH_BCK_MEAN","ASC_VH_COHE_MEAN","ASC_RATIO_BCK_MEAN",
                    "DESC_VV_BCK_MEAN","DESC_VV_COHE_MEAN","DESC_VH_BCK_MEAN","DESC_VH_COHE_MEAN","DESC_RATIO_BCK_MEAN"]

class DateValueIndexes(object): 
    def __init__(self):
        self.idxs = []

    def add_index(self, idx):
        self.idxs.append(idx)

class SelectedColumns(object):
    def __init__(self, col_names, global_col_indices, id_col_global_idx, id_col_name = ID_COL_NAME):
        
        # col_names.sort()
        self.columns = col_names
        self.global_col_indices = global_col_indices
        self.id_col_global_idx = id_col_global_idx
        
        self.id_col_name = id_col_name
        
        self.mean_indices = []
        self.all_unique_dates = []
        cur_idx = 1 # We start from 1 as on the first position in data will be always the ID (see get_all_global_col_indices)
        self.dict_cols_indices = dict()
        self.dict_cols_dates = dict()
        for col in col_names:
            for s1_marker_name in markers_sar_main:
                if s1_marker_name in col:
                    renamed_col = col
                    # remove any undesired prefix
                    if renamed_col.startswith("XX_"):
                        renamed_col = renamed_col[len("XX_"):]
                    # get the date until the first _
                    idx = renamed_col.index('_')
                    date_time_obj = None
                    # If other markers are also needed, see also the MarkerColumnInfos class in services
                    if idx > 0:
                        date_str = renamed_col[:idx]
                        renamed_col = renamed_col[idx+1:]
                        if date_str.startswith("W") : 
                            date_str = date_str[len("W"):]
                            year = int(date_str[:4])
                            week = int(date_str[4:6])
                            date_time_obj = dt.date.fromisocalendar(year, week, 1)
                        else:
                            date_time_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()

                        self.update_col_infos(col, renamed_col, cur_idx, date_time_obj)

            cur_idx = cur_idx+1

        self.all_column_names = [self.id_col_name] + self.columns

        self.update_dates_indexes()

        print("Mean indices: {}".format(self.mean_indices))
            
    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

    def update_col_infos(self, col, renamed_column, cur_idx, date_time_obj) :
        self.mean_indices.append(cur_idx)
        if not date_time_obj in self.all_unique_dates:
            self.all_unique_dates.append(date_time_obj)

        if renamed_column in self.dict_cols_indices.keys():
            self.dict_cols_indices[renamed_column].append(cur_idx)
            self.dict_cols_dates[renamed_column].append(date_time_obj)
        else :
            self.dict_cols_indices[renamed_column] = [cur_idx]
            self.dict_cols_dates[renamed_column] = [date_time_obj]

    def update_dates_indexes(self) :
        self.cols_indexes = dict()
        for renamed_col in self.dict_cols_indices.keys():
            arr_idxs = [DateValueIndexes() for j in range(len(self.all_unique_dates))]
            i = 0
            marker_dates = self.dict_cols_dates[renamed_col]
            for marker_date in marker_dates:
                idx_date = self.all_unique_dates.index(marker_date)
                arr_idxs[idx_date].add_index(self.dict_cols_indices[renamed_col][i])
                i = i + 1
            self.cols_indexes[renamed_col] = arr_idxs

def get_selected_columns(columns) : 
    # print ("Schema: {}".format(reader.schema))
    col_names = []
    cur_idx = 0
    global_col_indices = []
    id_col_global_idx = -1
    for name in columns:
        if name == ID_COL_NAME:
            id_col_global_idx = cur_idx
        else:
            if "_MEAN" in name:
                col_names.append(name)
                global_col_indices.append(cur_idx)
        cur_idx = cur_idx+1

    # print("Selected columns: {}".format(col_names))
    return SelectedColumns(col_names, global_col_indices, id_col_global_idx, ID_COL_NAME)

def handle_batch_record(selCols, all_cropfields, training_S1):
    for cropfield_descr in all_cropfields:
        # print("cropfield_descr: {}".format(cropfield_descr))
        # mean_vals = cropfield_descr[selCols.mean_indices]
        new_id = cropfield_descr[selCols.id_col_global_idx].astype(int)
        # Get all indexes from S2 calibration data for this new id
        newid_indexes = training_S1.loc[training_S1.NewID==new_id].index
        for renamed_col in selCols.dict_cols_indices.keys():
            dates_s = selCols.all_unique_dates
            date_idxs = selCols.cols_indexes[renamed_col]

            for newid_idx in newid_indexes:
                # get the date from S2 calibration data for the current index
                d = datetime.strptime(training_S1.dates[newid_idx],'%Y-%m-%d').date()
                # search in the S1 markers the index of closest date to the current S2 date
                d_m = min(range(len(dates_s)), key=lambda ii: abs(dates_s[ii]- d))
                date_idx = date_idxs[d_m]
                val = None    
                if len(date_idx.idxs) > 0 :
                    # get the real idx in the renamed col values
                    idx_value = date_idx.idxs[0]
                    val = cropfield_descr[idx_value]
                training_S1.at[newid_idx,f'{renamed_col}'] = val
                training_S1.at[newid_idx,'datesS1'] = dates_s[d_m]

    return training_S1

def handle_ipc_file(input, training_S1) :
    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names)
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
            
    rowcnt = 0
    for i in range(0, reader.num_record_batches):
        time1 = time.time()
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for name in all_column_names:
            columns_to_select.append(b.column(schema.get_field_index(name)))

        rb = b.from_arrays(columns_to_select, all_column_names)
        pd = rb.to_pandas()
        all_cropfields = pd.to_numpy()

        training_S1 = handle_batch_record(selCols, all_cropfields, training_S1)

        time2 = time.time()
        print("Execution for batch {}/{} for {} entries took: {} s"
                .format(i, reader.num_record_batches, len(all_cropfields), time2 - time1))

    return training_S1
        
def handle_csv_file(input, training_S1) :
    with open(input, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.reader(read_obj)
        header = next(csv_reader)
        selCols = get_selected_columns(header, tiles_filter)
        all_column_names = selCols.get_all_columns()
        
        N = 1000
        # Extract the relevant columns
        if header != None:
            while True:
                gen = islice(read_obj,N)
                arr = np.genfromtxt(gen, delimiter=',', usecols=selCols.get_all_global_col_indices(), encoding=None)
                all_cropfields = np.array(arr.tolist())
                if len(all_cropfields) > 0 and not hasattr(all_cropfields[0], "__len__"):
                    all_cropfields = [all_cropfields]
                # all_cropfields = arr.view(np.float).reshape(arr.shape + (-1,))
                training_S1 = handle_batch_record(selCols, all_cropfields, training_S1)
                if arr.shape[0]<N:
                    break
    
    return training_S1

def handle_json_file(input, training_S1) :
    re = {}
    with open(input,"r") as file:
        re = json.load(file)

    result = re["data"]
    for i in result['parcels']:
        newid = i['id']
        #print(i['id'])
        idx_i = training_S1.loc[training_S1.NewID==newid].index

        dates_s = [datetime.strptime(f,'%Y-%m-%d') for f in result["dates"]]
        for idx in idx_i:
            d=datetime.strptime(training_S1.dates[idx],'%Y-%m-%d')
            d_m = min(range(len(dates_s)), key=lambda ii: abs(dates_s[ii]- d))
            training_S1.at[idx,'datesS1'] = dates_s[d_m]
            for m in markers_sar_main:
                training_S1.at[idx,f'{m}'] = i['markers'][m][d_m]

    return training_S1

def handle_file(input, output, training_S1):
    lcinput = input.lower()
    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        training_S1 = handle_ipc_file(input, training_S1)
    elif lcinput.endswith('.csv'):
        print("Handling csv file {}".format(input))
        training_S1 = handle_csv_file(input, training_S1)
    elif lcinput.endswith('.json'):
        print("Handling json file {}".format(input))
        training_S1 = handle_json_file(input, training_S1)        
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

    training_S1.to_csv(output,index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S1"
    )
    parser.add_argument("-i", "--input", help="Input S1 markers files", required=True)
    parser.add_argument("-b", "--s2-bs-calib", help="Input S2 bare soil calibration file", required=True)
    parser.add_argument("-o", "--output", help="Output file containing calibration values", required=True)
    
    args = parser.parse_args()

    # load the S2 Bare soil calibration CSV
    s2_bs_calib_content = pd.read_csv(args.s2_bs_calib)

    input = args.input
    training_S1 = s2_bs_calib_content.copy()

    time1 = time.time()

    handle_file(args.input, args.output, training_S1)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()
