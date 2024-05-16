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

# The new ID column name as we find it in the markers file
bs_markers_new_id_col = "NewID"

def extract_markers(decl_csv, new_id_col_name, bs_markers, ids_map):

    parcel_ids_map = dict()
    new_id_idx = decl_csv.columns.get_loc(new_id_col_name)
    cnt = len(decl_csv.index)
    bs_count_arr = [None] * cnt
    count_P1_arr = [None] * cnt
    new_ids_arr = [None] * cnt

    i = 0
    for index, row in decl_csv.iterrows():
        newid = row.iloc[new_id_idx]
        bs_new_id = newid
        # get the BD new id, if a mapping is provided
        # but in the output file we write the new ID from the declarations
        if ids_map and len(ids_map) > 0:
            bs_new_id = ids_map.get(newid)
            if not bs_new_id :
                continue
        tt_bs = bs_markers.loc[bs_markers[bs_markers_new_id_col]==bs_new_id]
        # print(tt_bs)
        if len(tt_bs) == 0:
            bs_count = np.nan
            count_P1 = np.nan
        else :
            bs_count = tt_bs['TTdaysS2'].iloc[:].squeeze()
            count_P1 = tt_bs['NbrTTS2'].iloc[:].squeeze()

        new_ids_arr[i] = newid
        bs_count_arr[i] = bs_count
        count_P1_arr[i] = count_P1

        i = i+1

    ret = pd.DataFrame()
    ret[bs_markers_new_id_col] = new_ids_arr[:i]
    ret["TTdaysS2"] = bs_count_arr[:i]
    ret["NbrTTS2"] = count_P1_arr[:i]

    return ret

def get_mapping(mapping_file, decl_newid, bs_newid):
    ret_dict = dict()
    if mapping_file and decl_newid and bs_newid:
        if decl_newid != bs_newid :
            mapping = pd.read_csv(mapping_file, dtype={decl_newid: 'int', bs_newid: 'int'})
            df = mapping[[decl_newid, bs_newid]]
            # create a mapping from Decl ID to MDB ID
            ret_dict = dict(zip(df[decl_newid], df[bs_newid]))
    return ret_dict

def get_bs_markers_path(bs_path):
    if os.path.isfile(bs_path) and bs_path.endswith('.csv'):
        return bs_path
    elif os.path.isdir(bs_path):
        new_path = os.path.join(bs_path, "VECTOR_DATA", "L4E_BS_MarkersAll.csv")
        if os.path.isfile(new_path):
            return new_path
    
    print("Cannot determine BS markers file from provided input path {}".format(bs_path))
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input-bs-markers", required=True, help="The Bare Soil Markers file")
    parser.add_argument("--lpis-csv", required=True, help="The LPIS CSV file")
    parser.add_argument("-o", "--output", help="Output file containing S2 results", required=True)
    parser.add_argument("-m", "--mapping-file", help="Mapping file", required=False)
    parser.add_argument("--decl-newid", help="The NewID column name in the declarations file", required=False, default="NewID")
    parser.add_argument("--markers-newid", help="The NewID column name in the BS file", required=False)
    
    args = parser.parse_args()

    time1 = time.time()

    lpis_csv = pd.read_csv(args.lpis_csv)
    ids_map = get_mapping(args.mapping_file, args.decl_newid, args.markers_newid)

    input_bs_markers = get_bs_markers_path(args.input_bs_markers)
    bs_markers = pd.read_csv(input_bs_markers, dtype={bs_markers_new_id_col: 'int'})
    ret = extract_markers(lpis_csv, args.decl_newid, bs_markers, ids_map)

    ret = ret.astype({bs_markers_new_id_col: int})
    ret.sort_values(bs_markers_new_id_col, inplace=True)
    ret.to_csv(args.output, index=False)
    
    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



