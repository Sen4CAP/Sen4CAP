#!/usr/bin/env python
import argparse
from glob import glob
import datetime
from math import nan
from datetime import date, datetime, time
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
from functools import partial
from multiprocessing import Pool,cpu_count
from statistics import mean
import math
import sys

ID_COL_NAME = "NewID"

features_BS = ['mean_NDVI','mean_NDTI'] ## No need to use mean_NDWI for Belgium so I removed it 
features_NBS = ['mean_NDVI','mean_NDTI','mean_FCOVER'] ## No need to use mean_NDWI for Belgium so I removed it 

bi_to_filter = ['FAPAR', 'FCOVER','LAI', 'NDVI']
refl_bands_to_filter = ['B2', 'B3', 'B4', 'B8', 'B11', 'B12','mean_LAI', 'mean_NDVI']

bands_forced_order = ["mean_FAPAR","mean_FCOVER","mean_LAI","mean_L2A_B2","mean_L2A_B3","mean_L2A_B4","mean_L2A_B8","mean_L2A_B11",
                      "mean_L2A_B12","mean_NDVI"]

class Config(object):
    def __init__(self, args):

        self.tiles = args.tiles 

        self.thr_bs_ndvi = args.thr_bs_ndvi
        self.thr_nbs_ndvi = args.thr_nbs_ndvi
        self.thr_bs_ndwi = args.thr_bs_ndwi
        self.thr_nbs_ndwi = args.thr_nbs_ndwi
        self.thr_bs_ndti = args.thr_bs_ndti
        self.thr_nbs_ndti = args.thr_nbs_ndti
        self.thr_nbs_fcover = args.thr_nbs_fcover

        lpis_csv_regex = re.compile('(decl_.*_\d{4}\.csv$)')
        self.lpis_csv = ""
        found_lpis_csv = False
        for root, dirs, files in os.walk(args.lpis):
            for file in files:
                if lpis_csv_regex.match(file):
                    self.lpis_csv = os.path.join(root, file)
                    found_lpis_csv = True
                    break
            if found_lpis_csv == True:
                break

        if len(self.lpis_csv) == 0:
            print("Cannot find the declarations csv in the LPIS directory {}. Exiting ...".format(args.lpis))
            sys.exit(1)

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
        
        dates = []
        self.id_col_name = id_col_name
        
        self.mean_indices = []
        cur_idx = 1 # We start from 1 as on the first position in data will be always the ID (see get_all_global_col_indices)
        self.dict_cols_indices = dict()
        self.dict_cols_dates = dict()
        for col in col_names:
            # get the date until the first _
            idx = col.index('_')
            date_time_obj = None
            if idx > 0:
                date_str = col[:idx]
                date_time_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()
                if not date_time_obj in dates:
                    dates.append(date_time_obj)  

            if "mean_" in col:
                if "_L2A_" in col:
                    for band in refl_bands_to_filter:
                        if band in col:
                            self.update_col_infos(col, "mean_L2A_" + band, cur_idx, date_time_obj)
                else:
                    for bi in bi_to_filter:
                        if bi in col:
                            self.update_col_infos(col, "mean_" + bi, cur_idx, date_time_obj)

            cur_idx = cur_idx+1

        self.unique_dates = dates
        self.all_column_names = [self.id_col_name] + self.columns

        self.update_dates_indexes()

        if set(bands_forced_order) != set(self.dict_cols_indices.keys()) :
            print("Forces column order differ from the actual columns. Exiting ...")
            sys.exit(2)

        print("Mean indices: {}".format(self.mean_indices))
            
    def update_dates_indexes(self) :
        self.cols_indexes = dict()
        for renamed_col in self.dict_cols_indices.keys():
            arr_idxs = [DateValueIndexes() for j in range(len(self.unique_dates))]
            i = 0
            marker_dates = self.dict_cols_dates[renamed_col]
            for marker_date in marker_dates:
                idx_date = self.unique_dates.index(marker_date)
                arr_idxs[idx_date].add_index(self.dict_cols_indices[renamed_col][i])
                i = i + 1
            self.cols_indexes[renamed_col] = arr_idxs

    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

    def update_col_infos(self, col, renamed_column, cur_idx, date_time_obj) :
        self.mean_indices.append(cur_idx)

        if renamed_column in self.dict_cols_indices.keys():
            self.dict_cols_indices[renamed_column].append(cur_idx)
            self.dict_cols_dates[renamed_column].append(date_time_obj)
        else :
            self.dict_cols_indices[renamed_column] = [cur_idx]
            self.dict_cols_dates[renamed_column] = [date_time_obj]

class CropFieldEntryWrapper(object) : 
    def __init__(self, sel_cols, cropfield_descr):
        self.sel_cols = sel_cols
        self.cropfield_descr = cropfield_descr

def get_selected_columns(columns, tiles_filter) : 
    # print ("Schema: {}".format(reader.schema))
    col_names = []
    cur_idx = 0
    global_col_indices = []
    id_col_global_idx = -1
    for name in columns:
        if name == ID_COL_NAME:
            id_col_global_idx = cur_idx
        else:
            if "_mean_" in name:
                if "_L2A_" in name:
                    for band in refl_bands_to_filter:
                        if band in name:
                            if len(tiles_filter) > 0:
                                for tile in tiles_filter:
                                    if tile in name:
                                        col_names.append(name)
                                        global_col_indices.append(cur_idx)
                            else :
                                col_names.append(name)
                                global_col_indices.append(cur_idx)
                else:
                    for bi in bi_to_filter:
                        if bi in name:
                            col_names.append(name)
                            global_col_indices.append(cur_idx)
        cur_idx = cur_idx+1

    # print("Selected columns: {}".format(col_names))
    return SelectedColumns(col_names, global_col_indices, id_col_global_idx, ID_COL_NAME)

def compute_mean(input_arr) :
    sum = 0.0
    vald_cnt = 0
    for i in range(len(input_arr)):
        if not math.isnan(input_arr[i]):
            sum += input_arr[i]
            vald_cnt = vald_cnt + 1
    if vald_cnt == 0:
        return np.nan
    return (sum / vald_cnt)

def handle_cropfield_entry(selCols, cropfield_descr):
    nb_values = len(selCols.unique_dates) 
    values = np.empty(nb_values, dtype=object)
    traint_i = pd.DataFrame()
    traint_i['dates'] = selCols.unique_dates
    traint_i['NewID'] = cropfield_descr[selCols.id_col_global_idx].astype(int)
    # iterated each renamed unique column 
    for renamed_col in bands_forced_order:
        date_idxs = selCols.cols_indexes[renamed_col]
        i = 0
        for date in selCols.unique_dates:
            date_idx = date_idxs[i]
            nb_idxs = len(date_idx.idxs)
            if nb_idxs > 0 :
                if nb_idxs == 1:
                    values[i] = cropfield_descr[date_idx.idxs[0]]
                else :
                    vals = cropfield_descr[date_idx.idxs]
                    values[i] = compute_mean(vals)
            else:
                values[i] = None
            i = i + 1        

        traint_i[renamed_col] = values.tolist()
    
    return traint_i

def handle_cropfield_entry_wrp(cropfield_entry_wrp):
    return handle_cropfield_entry(cropfield_entry_wrp.sel_cols, cropfield_entry_wrp.cropfield_descr)

def handle_batch_record(selCols, all_cropfields, thread_pool):
    traint_all = pd.DataFrame()
    crop_field_entries_wrps = []
    for cropfield_descr in all_cropfields:
        crop_field_entries_wrps.append(CropFieldEntryWrapper(selCols, cropfield_descr))

    return pd.concat(thread_pool.map(partial(handle_cropfield_entry_wrp), crop_field_entries_wrps ))
    #for ret_df in all_df:
#    traint_all = pd.concat([traint_all, all_df])
    
#    return traint_all

def handle_ipc_file(input, out, tiles_filter, out_traint_all) :
    pool_size = cpu_count()
    thread_pool = Pool(pool_size)

    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names, tiles_filter)
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
            
    schema = reader.schema
    columns_schema_indexes = []        
    for name in all_column_names:
        columns_schema_indexes.append(schema.get_field_index(name))

    rowcnt = 0
    for i in range(0, reader.num_record_batches):
        time1 = time.time()
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for idx in columns_schema_indexes:
            columns_to_select.append(b.column(idx))

        rb = b.from_arrays(columns_to_select, all_column_names)
        batch_pd = rb.to_pandas()
        all_cropfields = batch_pd.to_numpy()
        
        traint_all1 = handle_batch_record(selCols, all_cropfields, thread_pool)

        out_traint_all = pd.concat([out_traint_all, traint_all1])

        time2 = time.time()
        print("Execution for batch {}/{} for {} entries took: {} s"
                .format(i, reader.num_record_batches, len(all_cropfields), time2 - time1))

    thread_pool.close()

    return out_traint_all
        
def handle_csv_file(input, out, tiles, out_traint_all) :
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
                out_traint_all = handle_batch_record(selCols, all_cropfields, out_traint_all)
                if arr.shape[0]<N:
                    break
    
    return out_traint_all

def handle_json_file(input, out, tiles, out_traint_all) :
    for tile in tiles:
        markers_opt_main = [ 'mean_FAPAR', 'mean_FCOVER','mean_LAI',f'mean_L2A_'+tile+'_B2', f'mean_L2A_'+tile+'_B3', f'mean_L2A_'+tile+'_B4',f'mean_L2A_'+tile+'_B8',f'mean_L2A_'+tile+'_B11', f'mean_L2A_'+tile+'_B12','mean_LAI', 'mean_NDVI']
        print(tile)
        re = {}
        with open(input,"r") as file:
            re = json.load(file)

        result = re["data"]
        parcels = result['parcels']
        tt = len(parcels)
        print('total number of parcels in the tile: ',tt)
        p = 0
        while p < tt:
            if p > tt-2000 :
                re_p = result['parcels'][p:tt]
            else:
                re_p = result['parcels'][p:(p+2000)]

            traint_all1 = pd.DataFrame()
            for i in re_p:
                newid = i['id']
                # print(i['id'])
                traint_i = pd.DataFrame()
                
                traint_i['dates'] = result["dates"]
                for m in markers_opt_main:
                    m_b = m.replace(tile+'_',"")
                    traint_i[m_b] = np.array(i['markers'][m], dtype=np.float32)
                traint_i['NewID'] = i['id']
                # print(i['markers'][m])

                traint_all1 = pd.concat([traint_all1,traint_i])
            out_traint_all = pd.concat([out_traint_all,traint_all1])
            p=p+2000
    
    return out_traint_all

def handle_file(config, input, output, lpis_csv):
    lcinput = input.lower()
    traint_all = pd.DataFrame()
    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        traint_all = handle_ipc_file(input, output, config.tiles, traint_all)
    elif lcinput.endswith('.csv'):
        print("Handling csv file {}".format(input))
        traint_all = handle_csv_file(input, output, config.tiles, traint_all)
    elif lcinput.endswith('.json'):
        print("Handling json file {}".format(input))
        traint_all = handle_json_file(input, output, config.tiles, traint_all)        
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(3)

    extract_calibration_data_s2(config, traint_all, lpis_csv, output)

def extract_calibration_data_s2(config, traint_all, lpis_csv, output) :
    # Normally, this is not needed when using arrow IPC
    traint_all = traint_all.groupby(['NewID','dates']).mean().reset_index()

    calibration_id = lpis_csv[(lpis_csv['S2Pix']>=50)& (lpis_csv['eaa']==1)]
    len(calibration_id)

    # traint_all['mean_NDWI'] = (traint_all[f'mean_L2A_B8'] - traint_all[f'mean_L2A_B11']) / (traint_all[f'mean_L2A_B11']+traint_all[f'mean_L2A_B8'])
    traint_all['mean_NDTI'] = (traint_all[f'mean_L2A_B11']-traint_all[f'mean_L2A_B12'])/(traint_all[f'mean_L2A_B11']+traint_all[f'mean_L2A_B12'])
    traint_all['mean_BSI'] = ((traint_all[f'mean_L2A_B11']+traint_all[f'mean_L2A_B4']) - (traint_all[f'mean_L2A_B8']+
                                traint_all[f'mean_L2A_B2'])) / ((traint_all[f'mean_L2A_B11']+traint_all[f'mean_L2A_B4']) + 
                                (traint_all[f'mean_L2A_B8']+traint_all[f'mean_L2A_B2']))

    traint_all = traint_all[traint_all.NewID.isin(calibration_id.NewID)]
    traint_BS = traint_all.copy()

    # ### Thresholds application

    # traint_BS['cat'] = np.nan
    # traint_BS.loc[(traint_BS['mean_NDVI'] < thr_bs_ndvi*1000) & (traint_BS['mean_NDTI'] <= thr_bs_ndti) & (traint_BS['mean_NDWI'] < thr_bs_ndwi),'cat'] = 'BS' 
    # traint_BS.loc[ ((traint_BS['mean_NDVI'] > thr_nbs_ndvi*1000) & (traint_BS['mean_NDTI'] > thr_nbs_ndti) & (traint_BS['mean_FCOVER'] > thr_nbs_fcover*1000)) | ((traint_BS['mean_NDVI'] < thr_bs_ndvi*1000) & (traint_BS['mean_NDWI'] > thr_nbs_ndmi)),'cat'] = 'NBS' 


    #df_training = traint_BS[traint_BS['cat'].notnull()]
    #print(traint_all)

    traint_BS['cat'] = np.nan
    traint_BS.loc[(traint_BS['mean_NDVI'] < config.thr_bs_ndvi*1000) & (traint_BS['mean_NDTI'] <= config.thr_bs_ndti), 'cat'] = 'BS' 
                  # & (traint_BS['mean_NDWI'] < config.thr_bs_ndwi),'cat'] = 'BS' 
    traint_BS.loc[(traint_BS['mean_NDVI'] > config.thr_nbs_ndvi*1000) & (traint_BS['mean_NDTI'] > config.thr_nbs_ndti) & 
                  (traint_BS['mean_FCOVER'] > config.thr_nbs_fcover*1000),'cat'] = 'NBS' 

    if 'mean_NDWI' in features_NBS :
        traint_BS.loc[((traint_BS['mean_NDVI'] < config.thr_bs_ndvi*1000) & (traint_BS['mean_NDWI'] > config.thr_nbs_ndwi)),'cat'] = 'NBS_Water' 

    df_training = traint_BS[traint_BS['cat'].notnull()]

    # ### Count observations in each categories 

    n_bs = df_training['cat'].value_counts()
    #n_nbs = training_dt['cat'].value_counts()
    print(f'Number of values in categories :')
    print(n_bs)

    num_id = len(df_training.NewID.unique())
    print(f'Number of unique parcels {num_id}')
    df_training.to_csv(output, index=False)
    print('END')
    
    if num_id == 0:
        print("ERROR: No parcels generated in the output. Please check that the input MDB1 product was created with the provided declarations set!")
        sys.exit(4)

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input", help="Input extracted features (mean, stddev etc.) for the desired BI", required=True)
    parser.add_argument("-o", "--output", help="Output file containing calibration values", required=True)
    parser.add_argument("-l", "--lpis", help="LPIS product path", required=True)
    parser.add_argument("-t", "--tiles", help="tiles filter", required=True, nargs="*")

    parser.add_argument("--thr-bs-ndvi", help="Threshold bare soil NDVI", required=False, type = float, default=0.15)
    parser.add_argument("--thr-nbs-ndvi", help="Threshold non bare soil NDVI", required=False, type = float, default=0.45)
    
    parser.add_argument("--thr-bs-ndwi", help="Threshold bare soil NDWI", required=False, type = float, default=0.)
    parser.add_argument("--thr-nbs-ndwi", help="Threshold bare soil NDWI", required=False, type = float, default=0.3)
    
    parser.add_argument("--thr-bs-ndti", help="Threshold bare soil NDTI", required=False, type = float, default=0.1)
    parser.add_argument("--thr-nbs-ndti", help="Threshold bare soil NDTI", required=False, type = float, default=0.25)

    parser.add_argument("--thr-nbs-fcover", help="Threshold bare soil fCover", required=False, type = float, default=0.01)

    args = parser.parse_args()

    config = Config(args)

    # load the LPIS CSV
    lpis_csv_content = pd.read_csv(config.lpis_csv)

    time1 = time.time()

    handle_file(config, args.input, args.output, lpis_csv_content)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))
    
if __name__ == "__main__":
    main()
