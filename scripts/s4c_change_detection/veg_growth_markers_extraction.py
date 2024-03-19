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
from multiprocessing import Pool,cpu_count
import requests
from tqdm import tqdm
from functools import partial
# from multiprocessing.pool import ThreadPool as TP
from multiprocessing import Pool as TP
from multiprocessing import cpu_count


ID_COL_NAME = "NewID"

markers_sar_main = ["s2_mean_ndvi"]

class SelectedColumns(object):
    def __init__(self, col_names, global_col_indices, id_col_global_idx, id_col_name = ID_COL_NAME):
        
        # col_names.sort()
        self.columns = col_names
        self.global_col_indices = global_col_indices
        self.id_col_global_idx = id_col_global_idx
        
        self.id_col_name = id_col_name
        
        self.mean_indices = []
        self.all_dates = []
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
                    idx = renamed_col.index('_s2')
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
                            date_time_obj = dt.datetime.strptime(date_str, '%Y_%m_%d').date()

                        self.update_col_infos(col, renamed_col, cur_idx, date_time_obj)

            cur_idx = cur_idx+1

        self.all_column_names = [self.id_col_name] + self.columns

        print("Mean indices: {}".format(self.mean_indices))
            
    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

    def update_col_infos(self, col, renamed_column, cur_idx, date_time_obj) :
        self.mean_indices.append(cur_idx)
        self.all_dates.append(date_time_obj)

        if renamed_column in self.dict_cols_indices.keys():
            self.dict_cols_indices[renamed_column].append(cur_idx)
            self.dict_cols_dates[renamed_column].append(date_time_obj)
        else :
            self.dict_cols_indices[renamed_column] = [cur_idx]
            self.dict_cols_dates[renamed_column] = [date_time_obj]


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
            if "_MEAN" in name or "_mean_" in name:
                col_names.append(name)
                global_col_indices.append(cur_idx)
        cur_idx = cur_idx+1

    # print("Selected columns: {}".format(col_names))
    return SelectedColumns(col_names, global_col_indices, id_col_global_idx, ID_COL_NAME)

def handle_batch_record(selCols, all_cropfields, dict_curves):
    for cropfield_descr in all_cropfields:
        new_id = cropfield_descr[selCols.id_col_global_idx].astype(int)
        # Get all indexes from S2 calibration data for this new id
        for renamed_col in selCols.dict_cols_indices.keys():
            vals = cropfield_descr[selCols.dict_cols_indices[renamed_col]] 
            curve_i = area_curve(vals)
            dict_curves[new_id] = curve_i

    return dict_curves

def handle_ipc_file(input, training_S1) :
    reader = ipc.open_file(input)
    
    print("Having a number of {} columns ...".format(len(reader.schema.names)))
    selCols = get_selected_columns(reader.schema.names)
    all_column_names = selCols.get_all_columns()
    print ("Column names to select: {}".format(all_column_names))
            
    # veg_all = pd.DataFrame()
    dict_curves = dict()
    rowcnt = 0
    for i in range(0, reader.num_record_batches):
        time1 = time.time()
        b = reader.get_batch(i)
        schema = b.schema
        columns_to_select = []        
        for name in all_column_names:
            columns_to_select.append(b.column(schema.get_field_index(name)))

        rb = b.from_arrays(columns_to_select, all_column_names)
        batch_pd = rb.to_pandas()
        all_cropfields = batch_pd.to_numpy()

        dict_curves = handle_batch_record(selCols, all_cropfields, dict_curves)

        time2 = time.time()
        print("Execution for batch {}/{} for {} entries took: {} s"
                .format(i, reader.num_record_batches, len(all_cropfields), time2 - time1))

    veg_all = pd.DataFrame(dict_curves.items(), columns=['NewID', 'AreaVeg'])
    return veg_all
        
def handle_file(input, output, training_S1):
    lcinput = input.lower()
    if lcinput.endswith('.ipc'):
        print("Handling ipc file {}".format(input))
        veg_all = handle_ipc_file(input, training_S1)
    else :
        print("Invalid file type received as input (unknow extension for {})".format(input))
        sys.exit(1)

    return veg_all

def area_curve(v_m):
    total_area = 0
    for j in range(len(v_m)-1):
        area_rec = v_m[j]*10
        area_j = area_rec + (v_m[j+1]-v_m[j]) * 0.5 * 10
        total_area+=area_j
    return total_area

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input-mdb4", help="Input MDB4 markers file", required=True)
    parser.add_argument("--lpis-csv", required=True, help="The LPIS CSV file")
    parser.add_argument("-o", "--output", help="Output file containing S2 results", required=True)
    
    args = parser.parse_args()

    time1 = time.time()

    lpis_csv = pd.read_csv(args.lpis_csv)
    
    df_results_all = handle_file(args.input_mdb4, lpis_csv, [])

    df_results_all.to_csv(args.output, index=False)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



