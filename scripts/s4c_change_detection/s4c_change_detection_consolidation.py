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

class Config(object):
    def __init__(self, args):
        self.input_p1 = args.input_p1
        self.input_p2 = args.input_p2
        
        self.p1_pred_thr = args.p1_pred_thr
        self.p1_pred_incr = args.p1_pred_incr

        self.p2_pred_thr = args.p2_pred_thr
        self.p2_pred_incr = args.p2_pred_incr

        self.mapping_file = args.mapping_file
        self.decl_newid = args.decl_newid 
        self.ref_decl_newid = args.ref_decl_newid 

def change_prediction(veg_all, config):
    veg_all ['pred_changeP1'] = 0
    veg_all.loc[(veg_all['ChangeP1'] >= config.p1_pred_thr),'pred_changeP1'] += config.p1_pred_incr
    
    veg_all ['pred_changeP2'] = 0
    veg_all.loc[(veg_all['ChangeP2'] >= config.p2_pred_thr),'pred_changeP2'] += config.p2_pred_incr
    
    veg_all ['pred_change_P1_P2'] = veg_all['pred_changeP1'] + veg_all['pred_changeP2']
    
    return veg_all

def merge_input_data(config) :
    ids_map = get_mapping(config.mapping_file, config.decl_newid, config.ref_decl_newid)

    # read the ref site (P1) and map the new ids to the main site ids (P2)
    df_p1 = pd.read_csv(config.input_p1)
    cols_p1 = dict()
    for column in df_p1.columns:
        if column != "NewID" and not column.startswith("ChangeP"):
            cols_p1[column] = column + "_P1"
    df_p1.rename(columns = cols_p1, inplace = True) 
    df_p1["NewID"] = df_p1["NewID"].replace(ids_map)

    df_p2 = pd.read_csv(config.input_p2)
    cols_p2 = dict()
    for column in df_p2.columns:
        if column != "NewID" and not column.startswith("ChangeP"):
            cols_p2[column] = column + "_P2"

    df_p2.rename(columns = cols_p2, inplace = True) 

    # Merge by NewID
    veg_all = pd.merge(df_p1, df_p2, on = "NewID", how = 'inner' )
    
    return veg_all

def get_mapping(mapping_file, decl_newid, ref_newid):
    ret_dict = dict()
    if mapping_file and decl_newid and ref_newid:
        if decl_newid != ref_newid :
            mapping = pd.read_csv(mapping_file)
            df = mapping[[decl_newid, ref_newid]]
            # create a mapping from ref Decl ID to Decl ID 
            ret_dict = dict(zip(df[ref_newid], df[decl_newid]))
    return ret_dict

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input-p1", help="Input Period 1", required=True)
    parser.add_argument("-l", "--input-p2", help="Input Period 2", required=True)
    parser.add_argument("-o", "--output", help="Output file containing change detection values", required=True)
    
    parser.add_argument("--p1-pred-thr", help = "Prediction P1 Threshold", required=False, type = float, default=2.5)
    parser.add_argument("--p1-pred-incr", help = "Prediction P1 Increment", required=False, type = float, default=1.)

    parser.add_argument("--p2-pred-thr", help = "Prediction P2 Threshold", required=False, type = float, default=3.0)
    parser.add_argument("--p2-pred-incr", help = "Prediction P2 Increment", required=False, type = float, default=1.)
    
    parser.add_argument("-m", "--mapping-file", help="Sites ids mapping file", required=False)
    parser.add_argument("--decl-newid", help="The NewID column name in the current's year declarations file", required=False, default="NewID")
    parser.add_argument("--ref-decl-newid", help="The NewID column name in the reference's year declarations file", required=False, default="NewID_ref")


    args = parser.parse_args()
    
    config = Config(args)

    time1 = time.time()
    
    veg_all = merge_input_data(config)
    veg_all = change_prediction(veg_all, config)
    
    veg_all.sort_values('NewID', inplace = True)
    veg_all.to_csv(args.output,index=False)
    
    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



