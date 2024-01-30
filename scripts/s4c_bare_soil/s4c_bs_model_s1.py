#!/usr/bin/env python
import argparse
from glob import glob
import json
from json import JSONDecodeError
import os
import re
from pyarrow import ipc
import datetime as dt

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
import glob
import pandas as pd
import subprocess, platform, os, glob,sys

import datetime
from math import nan
from datetime import date, datetime, time, timedelta
from time import strftime
import random
import pickle
import time

import matplotlib.pyplot as plt
import seaborn as sns

from functools import partial
from multiprocessing import Pool,cpu_count

from tqdm import tqdm

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
        self.all_dates = []
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

        self.all_unique_dates.sort()
        self.all_column_names = [self.id_col_name] + self.columns

        self.update_dates_indexes()

        print("Mean indices: {}".format(self.mean_indices))
            
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

    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

    def update_col_infos(self, col, renamed_column, cur_idx, date_time_obj) :
        self.mean_indices.append(cur_idx)
        self.all_dates.append(date_time_obj)
        if not date_time_obj in self.all_unique_dates :
            self.all_unique_dates.append(date_time_obj)

        if renamed_column in self.dict_cols_indices.keys():
            self.dict_cols_indices[renamed_column].append(cur_idx)
            self.dict_cols_dates[renamed_column].append(date_time_obj)
            self.dict_cols_dates[renamed_column].sort()
        else :
            self.dict_cols_indices[renamed_column] = [cur_idx]
            self.dict_cols_dates[renamed_column] = [date_time_obj]

class CropFieldEntryWrapper(object) : 
    def __init__(self, sel_cols, cropfield_descr, classifier):
        self.sel_cols = sel_cols
        self.cropfield_descr = cropfield_descr
        self.classifier = classifier

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

def handle_cropfield_entry(selCols, cropfield_descr, classifier):
    nb_values = len(selCols.all_unique_dates) 
    values = np.empty(nb_values, dtype=object)

    # print("cropfield_descr: {}".format(cropfield_descr))
    # mean_vals = cropfield_descr[selCols.mean_indices]
    newid = cropfield_descr[selCols.id_col_global_idx].astype(int)
    df_results_1 = pd.DataFrame()
    df_marker = pd.DataFrame()
    df_marker['dates'] = selCols.all_unique_dates
    # iterated each renamed unique column 
    for renamed_col in selCols.dict_cols_indices.keys():
        date_idxs = selCols.cols_indexes[renamed_col]
        i = 0
        for date in selCols.all_unique_dates:
            date_idx = date_idxs[i]
            if len(date_idx.idxs) > 0 :
                values[i] = cropfield_descr[date_idx.idxs[0]]
            else:
                values[i] = None
            i = i + 1        
        df_marker[renamed_col] = values.tolist()

    df_marker = df_marker.dropna()
    df_dates = df_marker.copy()
    df_marker = df_marker.drop(['dates'],axis=1)

    if len(df_marker)==0:
        df_results_1['dates'] = np.nan
        df_results_1['pred'] =  np.nan
        df_results_1['conf'] =  np.nan
        df_results_1['NewID'] = newid

    else: 

        df_results_1['dates'] = df_dates['dates']
        df_results_1['pred'] =  classifier.predict(df_marker)
        df_results_1['conf'] =  classifier.predict_proba(df_marker).max(axis=1)
        df_results_1['NewID'] = newid

    return df_results_1

def handle_cropfield_entry_wrp(cropfield_entry_wrp):
    return handle_cropfield_entry(cropfield_entry_wrp.sel_cols, 
                cropfield_entry_wrp.cropfield_descr, cropfield_entry_wrp.classifier)

def handle_batch_record(selCols, all_cropfields, classifier, df_results_all, thread_pool):
    df_results = pd.DataFrame()
    crop_field_entries_wrps = []
    for cropfield_descr in all_cropfields:
        ret = handle_cropfield_entry(selCols, cropfield_descr, classifier)
        df_results = pd.concat([df_results, ret])
    #    crop_field_entries_wrps.append(CropFieldEntryWrapper(selCols, cropfield_descr, classifier))

    # all_df = thread_pool.map(partial(handle_cropfield_entry_wrp), crop_field_entries_wrps )

    # TODO: See if the results couldn't be merged directly in df_results_all
    #for ret_df in all_df:
    #    df_results = pd.concat([df_results,ret_df])
    
    df_results_all = pd.concat([df_results_all,df_results])
    
    return df_results_all

def handle_ipc_file(input, classifier, df_results_all) :
    pool_size = cpu_count()
    thread_pool = Pool(pool_size)

    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names)
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
            
    rowcnt = 0
    for i in tqdm(range(0, reader.num_record_batches)):
        time1 = time.time()
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for name in all_column_names:
            columns_to_select.append(b.column(schema.get_field_index(name)))

        rb = b.from_arrays(columns_to_select, all_column_names)
        pd = rb.to_pandas()
        all_cropfields = pd.to_numpy()

        df_results_all = handle_batch_record(selCols, all_cropfields, classifier, df_results_all, thread_pool)

        time2 = time.time()
        print("Execution for batch {}/{} for {} entries took: {} s"
                .format(i, reader.num_record_batches, len(all_cropfields), time2 - time1))

    thread_pool.close()

    return df_results_all
        
def handle_csv_file(input, classifier, df_results_all) :
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
                df_results_all = handle_batch_record(selCols, all_cropfields, classifier, df_results_all)
                if arr.shape[0]<N:
                    break
    
    return df_results_all

def handle_json_file(input, classifier, df_results_all) :
    # ## apply model for each dates available
    # ### S1 model - results
    re = {}
    with open(input,"r") as file:
        re = json.load(file)

    result = re["data"]
    parcels = result['parcels']
    tt = len(parcels)

    for p in range(0,tt,1000):
        re_p = parcels[p:p+1000]
        df_results = pd.DataFrame()
        for i in re_p:
            df_results_1 = pd.DataFrame()
            df_marker = pd.DataFrame()
            df_marker['dates'] = result["dates"]

            newid = i['id']

            for m in markers_sar_main:
                df_marker[m] = i['markers'][m]

            df_marker = df_marker.dropna()
            df_dates = df_marker.copy()
            df_marker = df_marker.drop(['dates'],axis=1)
            
            if len(df_marker)==0:
                df_results_1['dates'] = np.nan
                df_results_1['pred'] =  np.nan
                df_results_1['conf'] =  np.nan
                df_results_1['NewID'] = newid

            else: 

                df_results_1['dates'] = df_dates['dates']
                df_results_1['pred'] =  classifier.predict(df_marker)
                df_results_1['conf'] =  classifier.predict_proba(df_marker).max(axis=1)
                df_results_1['NewID'] = newid

            df_results = pd.concat([df_results,df_results_1])
        
        df_results_all = pd.concat([df_results_all,df_results])

    return df_results_all

def handle_file(input, output, classifier):
    lcinput = input.lower()
    df_results_all = pd.DataFrame()
    df_results_all['NewID'] = np.nan
    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        df_results_all = handle_ipc_file(input, classifier, df_results_all)
    elif lcinput.endswith('.csv'):
        print("Handling csv file {}".format(input))
        df_results_all = handle_csv_file(input, classifier, df_results_all)
    elif lcinput.endswith('.json'):
        print("Handling json file {}".format(input))
        df_results_all = handle_json_file(input, classifier, df_results_all)        
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

    df_results_all['NewID'] = df_results_all['NewID'].astype('int')
    df_results_all.to_csv(output, index=False)
    print(output)

# ## Create RF models
# ## Import training dataset
## Model Sentinel-2 All 
def create_model(training_dtfile, output_model_file, n_estimators, output_fig_importance, period_calib, site_name) :
    model_exist = False
    clf = None
    if model_exist :
        clf = pickle.load(open(output_model_file, 'rb'))
        print('model imported')
    else:
        training_dt = pd.read_csv(training_dtfile)
        training = training_dt[np.isfinite(training_dt[markers_sar_main]).all(1)]

        print(f'Total number of values in the training dataset: {len(training)}')
        y = training['cat']
        x = training[markers_sar_main]

        clf = RandomForestClassifier(n_estimators=n_estimators,random_state=11)
        clf.fit(x,y)

        pickle.dump(clf, open(output_model_file, 'wb'))

        if output_fig_importance is not None:
            feature_imp = pd.Series(clf.feature_importances_,index=markers_sar_main).sort_values(ascending=False)
            # feature_imp

            # Creating a bar plot
            plt.figure(figsize=(7, 5))
            fig = sns.barplot(x=feature_imp, y=feature_imp.index)
            # Add labels to your graph
            fig.set(xlabel='Feature Importance Score', ylabel='Features', 
                    title="Visualizing Important Features : " + site_name + " " + period_calib)
            plt.legend()
            # plt.show()
            plt.tight_layout()
            fig.figure.savefig(output_fig_importance)
                
    return clf

def apply_model(df_results_all, classifier) :
    print("")

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input", help="Input S1 markers files", required=True)
    parser.add_argument("-b", "--s1-bs-calib", help="Input S1 bare soil calibration file", required=True)
    parser.add_argument("-o", "--output", help="Output file containing S1 results", required=True)
    parser.add_argument("-m", "--output-model", help="Output file containing the created model", required=True)
    parser.add_argument("-f", "--output-fig-importance", help="Output features importance score figure", required=False)
    parser.add_argument("-n", "--estimators-number", help="Number of estimators", type=int, required=False, default=30)
    
    parser.add_argument("-s", "--start-date", help="Start date", required=False)
    parser.add_argument("-e", "--end-date", help="End date", required=False)
    parser.add_argument("-a", "--site", help="Site short name", required=False)
    
    args = parser.parse_args()

    period_calib = ""
    if args.start_date is not None and args.end_date is not None:
        period_calib = args.start_date.replace("-", "") + "_" + args.end_date.replace("-", "")

    site_name = ""
    if args.site is not None:
        site_name = args.site

    # Create model from the S2 Bare soil calibration CSV
    classifier = create_model(args.s1_bs_calib, args.output_model, args.estimators_number, args.output_fig_importance,
                             period_calib, site_name)

    time1 = time.time()

    handle_file(args.input, args.output, classifier)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



