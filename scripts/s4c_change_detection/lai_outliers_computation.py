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
import glob
import pandas as pd
import subprocess, platform, os, glob,sys

import datetime
from math import nan
from datetime import date, datetime, time, timedelta
import random
import time

from functools import partial
from multiprocessing import Pool,cpu_count
import requests
from tqdm import tqdm
from functools import partial
# from multiprocessing.pool import ThreadPool as TP
from multiprocessing import Pool as TP
from multiprocessing import cpu_count

import dateutil.parser


lai_marker = "mean_LAI"
bands_forced_order = [lai_marker]

ID_COL_NAME = "NewID"
CTNUML4A_COL_NAME = "CTnumL4A"

# bi_to_filter = ['FAPAR', 'FCOVER','LAI', 'NDVI']
bi_to_filter = ['LAI']
refl_bands_to_filter = []
# refl_bands_to_filter = ['B2', 'B3', 'B4', 'B8', 'B11', 'B12','mean_LAI', 'mean_NDVI']

class DateValueIndexes(object): 
    def __init__(self):
        self.idxs = []

    def add_index(self, idx):
        self.idxs.append(idx)

def get_date_from_col_name(col_name) :
    idx = col_name.index('_')
    date_time_obj = None
    if idx > 0:
        date_str = col_name[:idx]
        date_time_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()

    return date_time_obj

class SelectedColumns(object):
    def __init__(self, col_names, id_col_global_idx, id_col_name = ID_COL_NAME):
        
        self.columns = col_names
        self.id_col_global_idx = id_col_global_idx
        
        unique_dates = []
        self.id_col_name = id_col_name
        
        cur_idx = 1 # We start from 1 as on the first position in data will be always the ID
        self.dict_cols_indices = dict()
        self.dict_cols_dates = dict()
        for col in col_names:
            # get the date until the first _
            idx = col.index('_')
            date_time_obj = get_date_from_col_name(col)
            if date_time_obj and not date_time_obj in unique_dates:
                unique_dates.append(date_time_obj)  

            if "mean_" in col:
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
        
    def update_col_infos(self, col, renamed_column, cur_idx, date_time_obj) :
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
    def __init__(self, df_marker, newid):
        self.df_marker = df_marker
        self.newid = newid

def is_valid_date(date_time_obj, start_date, end_date) :
    is_valid_date = True
    if start_date :
        if date_time_obj < start_date:
            is_valid_date = False
    if end_date :
        if date_time_obj > end_date:
            is_valid_date = False
    return is_valid_date

def get_selected_columns(columns, start_date, end_date, tiles_filter) : 
    # print ("Schema: {}".format(reader.schema))
    col_names = []
    cur_idx = 0
    id_col_global_idx = -1
    for name in columns:
        if name == ID_COL_NAME:
            id_col_global_idx = cur_idx
        else:
            if "_mean_" in name:
                date_time_obj = get_date_from_col_name(name)
                if not is_valid_date(date_time_obj, start_date, end_date) :
                   continue     

                for bi in bi_to_filter:
                    if bi in name:
                        col_names.append(name)
        cur_idx = cur_idx+1

    # print("Selected columns: {}".format(col_names))
    return SelectedColumns(col_names, id_col_global_idx, ID_COL_NAME)

def handle_cropfield_entry(selCols, cropfield_descr, newid, ctnuml4a):
    nb_values = len(selCols.unique_dates) 
    values = np.empty(nb_values, dtype=object)

    df_marker = pd.DataFrame()
    df_marker['dates'] = selCols.unique_dates
    df_marker['NewID'] = newid
    df_marker['CTnumL4A'] = ctnuml4a
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

def handle_cropfield_entry_wrp(cropfield_entry_wrp):
    return handle_cropfield_entry(cropfield_entry_wrp.sel_cols, cropfield_entry_wrp.cropfield_descr, 
                                  cropfield_entry_wrp.newid, cropfield_entry_wrp.newid_exists)

def handle_batch_record(selCols, all_cropfields, decl_ctnuml4a, ids_map, df_results_dict, thread_pool):

    for cropfield_descr in all_cropfields:
        newid_mdb = cropfield_descr[selCols.id_col_global_idx].astype(int)
        newid = newid_mdb
        if ids_map and len(ids_map) > 0:
            newid = ids_map.get(newid_mdb)
            if not newid : 
                continue

        ctnuml4a = decl_ctnuml4a.get(newid)
        if ctnuml4a is None:
            continue
        ret = handle_cropfield_entry(selCols, cropfield_descr, newid, ctnuml4a)
        df_results_dict[newid] = ret
    
    return df_results_dict

def handle_ipc_file(ipc_file, decl_ctnuml4a, ids_map, start_date, end_date, df_results_all) :
    time1 = time.time()

    pool_size = cpu_count()
    thread_pool = TP(pool_size)

    reader = ipc.open_file(ipc_file)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names, start_date, end_date, [])
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
    
    df_results_dict = dict()
    batches_in_perc_rng = int(reader.num_record_batches / 100)
    for i in range(0, reader.num_record_batches):
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for name in all_column_names:
            columns_to_select.append(b.column(schema.get_field_index(name)))

        # print("Columns: {}, num_cols = {}, rows = {}".format(b.schema, b.num_columns, b.num_rows))
        # print("{}".format(b.column(0)))
        rb = b.from_arrays(columns_to_select, all_column_names)
        batch_pd = rb.to_pandas()
        all_cropfields = batch_pd.to_numpy()
        
        handle_batch_record(selCols, all_cropfields, decl_ctnuml4a, ids_map, df_results_dict, thread_pool)

        if (i % batches_in_perc_rng) == 0:
            print("{}% parcels completed".format(int(i / batches_in_perc_rng)))

    if (reader.num_record_batches % 100) != 0 :
       print("100% parcels completed")

    thread_pool.close()
    df_results = pd.concat(df_results_dict.values())
    df_results_all = pd.concat([df_results_all, df_results])

    time2 = time.time()
    print("Execution for handling {} entries in {} IPC file batches took: {} s"
            .format(len(all_cropfields), reader.num_record_batches, time2 - time1))

    return df_results_all, df_results_dict
        
def handle_file(input, decl_ctnuml4a, ids_map, start_date, end_date):
    lcinput = input.lower()
    df_results_all = pd.DataFrame()
    df_results_all['NewID'] = np.nan
    df_results_all = df_results_all.astype({"NewID": int})    

    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        df_results_all, df_results_dict = handle_ipc_file(input, decl_ctnuml4a, ids_map, start_date, end_date, df_results_all)
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

    return df_results_all, df_results_dict

def get_real_col_name(lpis_csv, standard_col_name) :
    ret_col_name = standard_col_name
    lpis_csv_col_names = list(lpis_csv.columns.values)
    for col_name in lpis_csv_col_names:
        if col_name.lower() == standard_col_name.lower() :
            ret_col_name = col_name
            print("{} was found in the csv header as {}".format(standard_col_name, ret_col_name))
            break
    return ret_col_name
    
def get_ctnuml4a(lpis_csv):
    ctnuml4a_col_name = get_real_col_name(lpis_csv, CTNUML4A_COL_NAME)
    
    df = lpis_csv[["NewID", ctnuml4a_col_name]]
    # df.set_index("NewID")
    ret_dict = dict(zip(df['NewID'], df[ctnuml4a_col_name]))
    return ret_dict

def ref_dictionary_creation(df_results_all, ctnuml4a):
    dict_ref = {}
    if df_results_all.empty :
        return dict_ref
    for ct in ctnuml4a:
        val_plot = df_results_all.loc[df_results_all['CTnumL4A']==ct]
        if val_plot.empty : 
            continue
        
        val_dates_ct = val_plot.groupby(['dates'],as_index=False)[lai_marker].mean()
        val_dates_ct['std'] = val_plot.groupby(['dates'],as_index=False)[lai_marker].std()[lai_marker]
        df_ref = val_dates_ct.loc[:,('dates',lai_marker,'std')]
        df_ref = df_ref.rename(columns={lai_marker:'mean','std':'std'})
        dict_ref[ct] = df_ref

    return dict_ref

def count_lai_outliers(df, df_ref, nb_stdev):
 
    df['ref_LAI - stdev'] = df_ref['mean'] - nb_stdev * df_ref['std']
    df['ref_LAI + stdev'] = df_ref['mean'] + nb_stdev * df_ref['std']
 
    # Create a new DataFrame to store the counts
    counts_df = pd.DataFrame(columns=["count","consec_count", "total_non_nan"])
 
    # Loop over each parcel in the DataFrame
    # Get the LAI values for the current parcel
    parcel_lai = df
    #print(parcel_lai)
    # Calculate the reference range for each date
    ref_lai_min = df['ref_LAI - stdev']
    ref_lai_max = df['ref_LAI + stdev']
    #print(ref_lai_max)
    # Calculate the counts for the current parcel
    count = 0
    consec_count = 0
    prev_out_of_range = False
    total_non_nan = 0
    for i in range(len(parcel_lai)):
        #print('i:',i)
        if pd.isna(parcel_lai['mean'][i]):
            continue
        elif (parcel_lai['mean'][i] < ref_lai_min[i]) or (parcel_lai['mean'][i] > ref_lai_max[i]):
            count += 1
            if prev_out_of_range:
                consec_count += 1
            else:
                prev_out_of_range = True
        else:
            prev_out_of_range = False
 
        total_non_nan = parcel_lai['mean'].count()
 
        # Append the counts to the counts DataFrame
    parcel_counts_df = pd.DataFrame({ "count": [count],
                                        "consec_count": [consec_count],
                                        "total_non_nan": [total_non_nan],})
    counts_df = pd.concat([counts_df, parcel_counts_df], ignore_index=True)
    id_not_nan = counts_df['total_non_nan'] != 0
 
    counts_df = counts_df[id_not_nan]
 
    counts_df['ratio_obs'] = (counts_df['count']/counts_df['total_non_nan'])*100
      
    return counts_df

def compute_lai_outliers(df_results_dict, decl_ctnuml4a, dict_ref) :
    time1 = time.time()
    print("Computing LAI Outliers ...")

    df_outliers_all = pd.DataFrame()
    df_outliers_all['NewID'] = np.nan
    df_outliers_all = df_outliers_all.astype({"NewID": int})    

    outliers_list = []
    
    newids_cnt = len(decl_ctnuml4a.keys())
    ids_in_perc_rng = int(newids_cnt / 100)
    
    i = 0
    for newid in decl_ctnuml4a:
        if newid in df_results_dict:
            result = df_results_dict[newid]
            if not result.empty:
                result = result.rename(columns={lai_marker:'mean'})
                ctnuml4a = decl_ctnuml4a[newid]
                outlier = count_lai_outliers(result,dict_ref[ctnuml4a], 1.5)
                outlier["NewID"] = newid
        
                # print(outlier)
                outliers_list.append(outlier)

        if (i % ids_in_perc_rng) == 0:
            print("{}% lai outliers parcels completed".format(int(i / ids_in_perc_rng)))
        
        i = i + 1

    if (newids_cnt % 100) != 0 :
       print("100% lai outliers parcels completed")

    df_outliers = pd.concat(outliers_list)
    df_outliers_all = pd.concat([df_outliers_all, df_outliers])
    df_outliers_all.sort_values("NewID", inplace=True)

    time2 = time.time()
    print("Computing LAI Outliers Execution took: {} s" .format(time2 - time1))

    return df_outliers_all

def get_mapping(mapping_file, decl_newid, mdb_newid):
    ret_dict = dict()
    if mapping_file and decl_newid and mdb_newid:
        if decl_newid != mdb_newid :
            mapping = pd.read_csv(mapping_file)
            df = mapping[[decl_newid, mdb_newid]]
            # create a mapping from MDB ID to Decl ID 
            ret_dict = dict(zip(df[mdb_newid], df[decl_newid]))
    return ret_dict

def main():
    parser = argparse.ArgumentParser(
        description="Performs the LAI Outliers from MDB 1 IPC file"
    )
    parser.add_argument("-i", "--input", help="Input MDB 1 IPC file", required=True)
    parser.add_argument("-o", "--output", help="Output csv file", required=True)
    parser.add_argument("-l", "--lpis-csv", required=True, help="The LPIS CSV file")
    parser.add_argument("-b", "--start-date", help="Start date", required=False)
    parser.add_argument("-e", "--end-date", help="End date", required=False)
    parser.add_argument("-m", "--mapping-file", help="Mapping file", required=False)
    parser.add_argument("--decl-newid", help="The NewID column name in the declarations file", required=False, default="NewID")
    parser.add_argument("--markers-newid", help="The NewID column name in the MDB file", required=False)

    args = parser.parse_args()

    start_date = None
    end_date = None
    if args.start_date:
        start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    if args.end_date:
        end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date() + dt.timedelta(days=1)

    time1 = time.time()

    ids_map = get_mapping(args.mapping_file, args.decl_newid, args.markers_newid)

    lpis_csv = pd.read_csv(args.lpis_csv)
    decl_ctnuml4a = get_ctnuml4a(lpis_csv)
    
    df_results_all,df_results_dict = handle_file(args.input, decl_ctnuml4a, ids_map, start_date, end_date)

    ctnuml4a = sorted(set(decl_ctnuml4a.values()))
    dict_ref = ref_dictionary_creation(df_results_all, ctnuml4a)

    df_stabs_all = compute_lai_outliers(df_results_dict, decl_ctnuml4a, dict_ref)

    df_stabs_all.to_csv(args.output, index=False)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()

