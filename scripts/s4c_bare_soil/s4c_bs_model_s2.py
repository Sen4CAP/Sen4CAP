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
import random
import pickle
import time

import matplotlib.pyplot as plt
import seaborn as sns

from functools import partial
# from multiprocessing.pool import ThreadPool as TP
from multiprocessing import Pool as TP
from multiprocessing import cpu_count

ID_COL_NAME = "NewID"

bi_to_filter = ['FAPAR', 'FCOVER','LAI', 'NDVI']
refl_bands_to_filter = ['B2', 'B3', 'B4', 'B8', 'B11', 'B12','mean_LAI', 'mean_NDVI']

bands_forced_order = ["mean_FAPAR","mean_FCOVER","mean_LAI","mean_L2A_B2","mean_L2A_B3","mean_L2A_B4","mean_L2A_B8","mean_L2A_B11",
                      "mean_L2A_B12","mean_NDVI"]

#class DatesIndexesMapping() :
#    def __init__(self, unique_dates, marker_dates, marker_indexes):
#        self.indexes = [None] * len(unique_dates)
#        i = 0
#        for unique_date in unique_dates:

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
        
        unique_dates = []
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
                if not date_time_obj in unique_dates:
                    unique_dates.append(date_time_obj)  

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

        self.unique_dates = unique_dates
        self.all_column_names = [self.id_col_name] + self.columns

        self.update_dates_indexes()

        if set(bands_forced_order) != set(self.dict_cols_indices.keys()) :
            print("Forces column order differ from the actual columns. Exiting ...")
            sys.exit(1)

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
    def __init__(self, sel_cols, cropfield_descr, newid, newid_exists):
        self.sel_cols = sel_cols
        self.cropfield_descr = cropfield_descr
        self.newid = newid
        self.newid_exists = newid_exists

class ClassificationDataFrameMarkerWrapper(object) : 
    def __init__(self, df_marker, classifier, newid):
        self.df_marker = df_marker
        self.classifier = classifier
        self.newid = newid

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
                                    if band in name:
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

def handle_cropfield_entry(selCols, cropfield_descr, newid, newid_exists):
    nb_values = len(selCols.unique_dates) 
    values = np.empty(nb_values, dtype=object)

    df_marker = pd.DataFrame()
    df_marker['dates'] = selCols.unique_dates
    if not newid_exists:
        df_marker['NewID'] = newid
    # iterated each renamed unique column 
    for renamed_col in bands_forced_order:
        date_idxs = selCols.cols_indexes[renamed_col]
        i = 0
        for date in selCols.unique_dates:
            date_idx = date_idxs[i]
            if len(date_idx.idxs) > 0 :
                values[i] = cropfield_descr[date_idx.idxs[0]]
            else:
                values[i] = None
            i = i + 1        

        df_marker[renamed_col] = values.tolist()
    
    return df_marker

def df_marker_predict(df_marker, classifier, newid):
    # df_marker['mean_NDWI'] = (df_marker[f'mean_L2A_B8']-df_marker[f'mean_L2A_B11'])/(df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B11'])
    df_marker['mean_NDTI'] = (df_marker[f'mean_L2A_B11']-df_marker[f'mean_L2A_B12'])/(df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B12'])
    df_marker['mean_BSI'] = ((df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B4']) - (df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B2'])) / ((df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B4']) + (df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B2']))

    df_dates = df_marker.copy()
    df_marker = df_marker.drop(['dates','NewID'],axis=1)
    
    df_results_1 = pd.DataFrame()
    if len(df_marker)==0:
        df_results_1['dates'] = np.nan
        df_results_1['pred'] =  np.nan
        df_results_1['conf'] =  np.nan
        df_results_1['NewID'] = np.nan

    else: 

        df_results_1['dates'] = df_dates['dates']
        df_results_1['pred'] =  classifier.predict(df_marker)
        df_results_1['conf'] =  classifier.predict_proba(df_marker).max(axis=1)
        df_results_1['NewID'] = newid

    return df_results_1

def df_marker_predict_wrp(cropfield_entry_wrp):
    return df_marker_predict(cropfield_entry_wrp.df_marker, cropfield_entry_wrp.classifier, cropfield_entry_wrp.newid)

def handle_cropfield_entry_wrp(cropfield_entry_wrp):
    return handle_cropfield_entry(cropfield_entry_wrp.sel_cols, cropfield_entry_wrp.cropfield_descr, 
                                  cropfield_entry_wrp.newid, cropfield_entry_wrp.newid_exists)

def handle_batch_record(selCols, all_cropfields, classifier, df_results_all, df_marker_all, result_all_ids, thread_pool):
    df_results = pd.DataFrame()

    cropfield_entry_wrappers = []
    for cropfield_descr in all_cropfields:
        newid = cropfield_descr[selCols.id_col_global_idx].astype(int)
        newid_exists = newid in result_all_ids
        cropfield_entry_wrappers.append(CropFieldEntryWrapper(selCols, cropfield_descr, newid, newid_exists))

    all_df = thread_pool.map(partial(handle_cropfield_entry_wrp), cropfield_entry_wrappers )
    i = 0

    new_df_markers = []
    for ret_df in all_df:
        cropfield_descr = all_cropfields[i]
        i = i + 1
        newid = cropfield_descr[selCols.id_col_global_idx].astype(int)
        if newid in result_all_ids:
            df_marker_old = df_marker_all[df_marker_all.NewID==newid]
            df_marker_new = pd.concat([df_marker_old,ret_df])
            df_marker = df_marker_new.fillna(-1).groupby(['NewID','dates']).mean().reset_index()  
        else:
            df_marker = ret_df

        df_marker = df_marker.replace(-1,np.nan)
        df_marker = df_marker.dropna()
        df_marker_all = pd.concat([df_marker_all,df_marker])

        new_df_markers.append(ClassificationDataFrameMarkerWrapper(df_marker, classifier, newid))
        
        result_all_ids.add(newid)

    all_df1 = thread_pool.map(partial(df_marker_predict_wrp), new_df_markers)
    for df_results_1 in all_df1:
        df_results = pd.concat([df_results,df_results_1])
    
    df_results_all = pd.concat([df_results_all,df_results]) 
    
    return df_results_all, df_marker_all

def handle_ipc_file(input, out, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids) :
    pool_size = cpu_count()
    thread_pool = TP(pool_size)

    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names, tiles_filter)
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

        # print("Columns: {}, num_cols = {}, rows = {}".format(b.schema, b.num_columns, b.num_rows))
        # print("{}".format(b.column(0)))
        rb = b.from_arrays(columns_to_select, all_column_names)
        pd = rb.to_pandas()
        all_cropfields = pd.to_numpy()
        
        df_results_all, df_marker_all = handle_batch_record(selCols, all_cropfields, classifier, 
                                                            df_results_all, df_marker_all, result_all_ids, thread_pool)

        time2 = time.time()
        print("Execution for batch {}/{} for {} entries took: {} s"
                .format(i, reader.num_record_batches, len(all_cropfields), time2 - time1))

    thread_pool.close()

    return df_results_all, df_marker_all
        
def handle_csv_file(input, out, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids) :
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
                df_results_all = handle_batch_record(selCols, all_cropfields, classifier, df_results_all, df_marker_all)
                if arr.shape[0]<N:
                    break
    
    return df_results_all, df_marker_all

def handle_json_file(input, out, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids) :
    # ## apply model for each dates available
    # ### S2 model - results
    df_results_all['NewID'] = np.nan
    product_type='s4c_mdb1'

    for tile in tiles_filter:
        print(tile)
        markers_opt_main = [ 'mean_FAPAR', 'mean_FCOVER','mean_LAI',f'mean_L2A_'+tile+'_B2', f'mean_L2A_'+tile+'_B3', f'mean_L2A_'+tile+'_B4',f'mean_L2A_'+tile+'_B8',f'mean_L2A_'+tile+'_B11', f'mean_L2A_'+tile+'_B12','mean_NDVI']
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

                newid = i['id']
                df_results_1 = pd.DataFrame()

                if newid in df_results_all.NewID:
                    df_marker_old = df_marker_all[df_marker_all.NewID==newid]
                    df_marker_tile = pd.DataFrame()
                    df_marker_tile['dates'] = result["dates"]
                    for m in markers_opt_main:
                        m_b = m.replace(tile+'_',"")
                        df_marker_tile[m_b] = i['markers'][m]
                        
                    df_marker_new = pd.concat([df_marker_old,df_marker_tile])
                    df_marker = df_marker_new.fillna(-1).groupby(['NewID','dates']).mean().reset_index()
                else:
                    df_marker = pd.DataFrame()
                    df_marker['dates'] = result["dates"]

                    for m in markers_opt_main:
                        m_b = m.replace(tile+'_',"")
                        df_marker[m_b] = i['markers'][m]
                
                df_marker['NewID'] = newid
                df_marker = df_marker.replace(-1,np.nan)
                df_marker = df_marker.dropna()
                df_marker_all = pd.concat([df_marker_all,df_marker])

                # df_marker['mean_NDWI'] = (df_marker[f'mean_L2A_B8']-df_marker[f'mean_L2A_B11'])/(df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B11'])
                df_marker['mean_NDTI'] = (df_marker[f'mean_L2A_B11']-df_marker[f'mean_L2A_B12'])/(df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B12'])
                df_marker['mean_BSI'] = ((df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B4']) - (df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B2'])) / ((df_marker[f'mean_L2A_B11']+df_marker[f'mean_L2A_B4']) + (df_marker[f'mean_L2A_B8']+df_marker[f'mean_L2A_B2']))
            
                df_dates = df_marker.copy()
                df_marker = df_marker.drop(['dates','NewID'],axis=1)
                
                if len(df_marker)==0:
                    df_results_1['dates'] = np.nan
                    df_results_1['pred'] =  np.nan
                    df_results_1['conf'] =  np.nan
                    df_results_1['NewID'] = np.nan

                else: 

                    df_results_1['dates'] = df_dates['dates']
                    df_results_1['pred'] =  classifier.predict(df_marker)
                    df_results_1['conf'] =  classifier.predict_proba(df_marker).max(axis=1)
                    df_results_1['NewID'] = newid

                df_results = pd.concat([df_results,df_results_1])
                
            df_results_all = pd.concat([df_results_all,df_results])


    df_results_all = df_results_all.sort_values(['NewID', 'dates'])
    df_results_all = df_results_all.drop_duplicates(subset=['NewID','dates'], keep='last')
    return df_results_all

def handle_file(input, output, classifier, tiles_filter):
    lcinput = input.lower()
    df_results_all = pd.DataFrame()
    df_results_all['NewID'] = np.nan
    df_results_all = df_results_all.astype({"NewID": int})
    result_all_ids = set()
    df_marker_all = pd.DataFrame()

    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        df_results_all, df_marker_all = handle_ipc_file(input, output, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids)
    elif lcinput.endswith('.csv'):
        print("Handling csv file {}".format(input))
        df_results_all, df_marker_all = handle_csv_file(input, output, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids)
    elif lcinput.endswith('.json'):
        print("Handling json file {}".format(input))
        df_results_all, df_marker_all = handle_json_file(input, output, classifier, tiles_filter, df_results_all, df_marker_all, result_all_ids)        
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

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
        print("Number of values in categories : {}".format(training_dt['cat'].value_counts()))

        num_id = len(training_dt.NewID.unique())
        print(f'Number of unique parcels {num_id}')
        col_var = [col for col in training_dt if col.startswith(('mean')) ]
        print(col_var)

        training = training_dt[np.isfinite(training_dt[col_var]).all(1)]

        print(f'Total number of values in the training dataset: {len(training)}')
        y = training['cat']
        x = training[col_var]
        print('start model creation')
        clf = RandomForestClassifier(n_estimators=n_estimators,random_state=11)
        clf.fit(x,y)

        pickle.dump(clf, open(output_model_file, 'wb'))

        if output_fig_importance is not None:
            feature_imp = pd.Series(clf.feature_importances_,index=col_var).sort_values(ascending=False)
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
        
        print('model created')
        
    return clf

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input", help="Input S2 markers file", required=True)
    parser.add_argument("-b", "--s2-bs-calib", help="Input S2 bare soil calibration file", required=True)
    parser.add_argument("-o", "--output", help="Output file containing S2 results", required=True)
    parser.add_argument("-m", "--output-model", help="Output file containing the created model", required=True)
    parser.add_argument("-f", "--output-fig-importance", help="Output features importance score figure", required=False)
    parser.add_argument("-t", "--tiles", help="tiles filter", required=True, nargs="*")
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
    classifier = create_model(args.s2_bs_calib, args.output_model, args.estimators_number,
                              args.output_fig_importance, period_calib, site_name)

    time1 = time.time()

    handle_file(args.input, args.output, classifier, args.tiles)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



