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
        self.input_lpis = args.input_lpis
        self.ref_lpis = args.ref_lpis
        self.mapping_file = args.mapping_file
        self.input_veg_growth_markers = args.input_veg_growth_markers
        self.input_outliers = args.input_outliers
        self.input_bs_markers = args.input_bs_markers

        self.change_period = args.change_period
        self.grassland_ttdayss2_thr = args.grassland_ttdayss2_thr
        self.grassland_ttdayss2_incr = args.grassland_ttdayss2_incr
        self.grassland_ratiostab_min_thr = args.grassland_ratiostab_min_thr
        self.grassland_ratiostab_max_thr = args.grassland_ratiostab_max_thr
        self.grassland_ratiostab_min_incr = args.grassland_ratiostab_min_incr
        self.grassland_ratiostab_max_incr = args.grassland_ratiostab_max_incr
        self.grassland_consecstab_thr = args.grassland_consecstab_thr
        self.grassland_consecstab_incr = args.grassland_consecstab_incr
        
        self.permcrops_ttdayss2_thr = args.permcrops_ttdayss2_thr
        self.permcrops_ttdayss2_incr = args.permcrops_ttdayss2_incr
        self.permcrops_areaveg_thr = args.permcrops_areaveg_thr
        self.permcrops_areaveg_incr = args.permcrops_areaveg_incr
        self.permcrops_ratiostab_thr = args.permcrops_ratiostab_thr
        self.permcrops_ratiostab_incr = args.permcrops_ratiostab_incr
        
        self.arableland_ttdayss2_thr = args.arableland_ttdayss2_thr
        self.arableland_ttdayss2_incr = args.arableland_ttdayss2_incr

        self.decl_newid = args.decl_newid 
        self.ref_decl_newid = args.ref_decl_newid 

def apply_threshold_values(veg_all, config):
    Change_period = 'ChangeP' + str(config.change_period)
    
    #1. Grassland changes 
    veg_all[Change_period] = np.nan
    veg_all.loc[(veg_all['LC_P1']==3), Change_period] = 0
    veg_all.loc[(veg_all['LC_P1']==3) & (veg_all['TTdaysS2'] > config.grassland_ttdayss2_thr),Change_period] += config.grassland_ttdayss2_incr
    veg_all.loc[(veg_all['LC_P1']==3) & (veg_all['Ratio_stability'] > config.grassland_ratiostab_min_thr) &
                                     (veg_all['Ratio_stability'] < config.grassland_ratiostab_max_thr),
                                     Change_period] += config.grassland_ratiostab_min_incr
    veg_all.loc[(veg_all['LC_P1']==3) & (veg_all['Ratio_stability'] > config.grassland_ratiostab_max_thr), Change_period] += config.grassland_ratiostab_max_incr
    veg_all.loc[(veg_all['LC_P1']==3) & (veg_all['ConsecC_stability'] >= config.grassland_consecstab_thr) ,Change_period] += config.grassland_consecstab_incr
     
     
    #2. Permanent Crop changes 
    veg_all.loc[(veg_all['LC_P1']==2),Change_period] = 0
    veg_all.loc[(veg_all['LC_P1']==2) & (veg_all['TTdaysS2'] > config.permcrops_ttdayss2_thr),Change_period] += config.permcrops_ttdayss2_incr
    veg_all.loc[(veg_all['LC_P1']==2) & (veg_all['AreaVeg'] > config.permcrops_areaveg_thr), Change_period] += config.permcrops_areaveg_incr
    veg_all.loc[(veg_all['LC_P1']==2) & (veg_all['Ratio_stability'] > config.permcrops_ratiostab_thr), Change_period] += config.permcrops_ratiostab_incr
     
    #3. Arable land changes 
    veg_all.loc[(veg_all['LC_P1']==1),Change_period] = 0
    veg_all.loc[(veg_all['LC_P1']==1) & (veg_all['TTdaysS2'] == config.arableland_ttdayss2_thr),Change_period] = config.arableland_ttdayss2_incr
    
    return veg_all

def change_prediction(veg_all):
    veg_all ['pred_changeP1'] = 0
    veg_all.loc[(veg_all['ChangeP1'] >= 2.5),'pred_changeP1'] +=1
    
    veg_all ['pred_changeP2'] = 0
    veg_all.loc[(veg_all['ChangeP2'] >= 3),'pred_changeP2'] +=1
    
    veg_all ['pred_change_P1_P2'] = veg_all['pred_changeP1'] + veg_all['pred_changeP2']
    
    return veg_all

def get_mapping(mapping_file, decl_newid, ref_newid):
    ret_dict = dict()
    if mapping_file and decl_newid and ref_newid:
        if decl_newid != ref_newid :
            mapping = pd.read_csv(mapping_file)
            df = mapping[[decl_newid, ref_newid]]
            # create a mapping from MDB ID to Decl ID 
            ret_dict = dict(zip(df[ref_newid], df[decl_newid]))
    return ret_dict

def get_ref_lc(ref_lpis, mapping_file) :
    lpis_csv = pd.read_csv(ref_lpis)
    lpis_filtered = lpis_csv[["NewID", "LC"]]
    lpis_filtered = lpis_filtered.rename(columns = {"LC":"LC_P1"}) 
    return lpis_filtered

def map_ref_newids(df, mapping_file, decl_newid, ref_decl_newid) :
    ids_map = get_mapping(mapping_file, decl_newid, ref_decl_newid)
    df["NewID"] = df["NewID"].replace(ids_map)
    return df

def merge_input_data(config) :
    lpis_csv = pd.read_csv(config.input_lpis)
    
    # if we are in the current year, then we need to use the LC from the reference year (the previous one)
    # but the LC in this ref LPIS are not the same as the ones in the current year, so we have to map them
    if config.change_period > 1:
        if not config.mapping_file or not config.ref_lpis:
            print("For the current year (P2) should be provided the NewIDs mapping file and the LPIS for the reference year (P1)")
            sys.exit(1)
        ref_lc = get_ref_lc(config.ref_lpis, config.mapping_file)

        # map ids from ref LPIS to the current LPIS
        ref_lc = map_ref_newids(ref_lc, config.mapping_file, config.decl_newid, config.ref_decl_newid)

        lpis_csv.rename(columns = {"LC":"LC_P2"}, inplace = True) 
        lpis_csv = pd.merge(lpis_csv, ref_lc, on = "NewID", how = 'inner' )
    else:
        lpis_csv.rename(columns = {"LC":"LC_P1"}, inplace = True) 

    veg_all = pd.read_csv(config.input_veg_growth_markers)
    outliers_csv = pd.read_csv(config.input_outliers)
    stability_markers = pd.read_csv(config.input_bs_markers)

    veg_all = pd.merge(lpis_csv, veg_all, on = "NewID", how = 'inner' )
    veg_all = pd.merge(veg_all, stability_markers, on = "NewID", how = 'inner' )
    veg_all = pd.merge(veg_all, outliers_csv, on = "NewID", how = 'inner' )
    veg_all.rename(columns={"ratio_obs":'Ratio_stability'}, inplace=True)
    veg_all.rename(columns={"consec_count":'ConsecC_stability'}, inplace=True)
    
    return veg_all

def main():
    parser = argparse.ArgumentParser(
        description="Performs the bare soil calibration for S2"
    )
    parser.add_argument("-i", "--input-lpis", help="Input LPIS", required=True)
    parser.add_argument("-l", "--ref-lpis", help="Input LPIS", required=False)
    parser.add_argument("-r", "--input-outliers", help="Input file containing LAI outliers", required=True)
    parser.add_argument("-g", "--input-veg-growth-markers", help="Input file containing the Vegetation growth markers", required=True)
    parser.add_argument("-b", "--input-bs-markers", help="Input file containing the BS markers", required=True)
    parser.add_argument("-p", "--change-period", help="The change period name", required=False, type = int, default = 1)
    parser.add_argument("-o", "--output", help="Output file containing change detection values", required=True)
    
    parser.add_argument("--grassland-ttdayss2-thr", help = "Grassland TTdaysS2 Threshold", required=False, type = float, default=0.)
    parser.add_argument("--grassland-ttdayss2-incr", help = "Grassland TTdaysS2 Increment", required=False, type = float, default=2.)
    parser.add_argument("--grassland-ratiostab-min-thr", help = "Grassland Ratio Stability Threshold Min", required=False, type = float, default=0.)
    parser.add_argument("--grassland-ratiostab-max-thr", help = "Grassland Ratio Stability Threshold Max", required=False, type = float, default=25.)
    parser.add_argument("--grassland-ratiostab-min-incr", help = "Grassland Ratio Stability Min Increment" , required=False, type = float, default=1.)
    parser.add_argument("--grassland-ratiostab-max-incr", help = "Grassland Ratio Stability Max Increment" , required=False, type = float, default=1.5)
    parser.add_argument("--grassland-consecstab-thr", help = "Grassland ConsecC Stability Threshold", required=False, type = float, default=1.)
    parser.add_argument("--grassland-consecstab-incr", help = "Grassland ConsecC Stability Increment", required=False, type = float, default=1.)
    parser.add_argument("--permcrops-ttdayss2-thr", help = "Permanent Crops TTdaysS2 Threshold", required=False, type = float, default=0.)
    parser.add_argument("--permcrops-ttdayss2-incr", help = "Permanent Crops TTdaysS2 Increment", required=False, type = float, default=3.)
    parser.add_argument("--permcrops-areaveg-thr", help = "Permanent Crops AreaVeg Threshold", required=False, type = float, default=25.)
    parser.add_argument("--permcrops-areaveg-incr", help = "Permanent Crops AreaVeg Increment", required=False, type = float, default=1.)
    parser.add_argument("--permcrops-ratiostab-thr", help = "Grassland Ratio Stability Threshold", required=False, type = float, default=20.)
    parser.add_argument("--permcrops-ratiostab-incr", help = "Grassland Ratio Stability Increment", required=False, type = float, default=1.)
    parser.add_argument("--arableland-ttdayss2-thr", help = "Arable land TTdaysS2 Threshold", required=False, type = float, default=0.)
    parser.add_argument("--arableland-ttdayss2-incr", help = "Arable land TTdaysS2 Increment", required=False, type = float, default=1.)
    
    parser.add_argument("-m", "--mapping-file", help="Sites ids mapping file", required=False)
    parser.add_argument("--decl-newid", help="The NewID column name in the current's year declarations file", required=False, default="NewID")
    parser.add_argument("--ref-decl-newid", help="The NewID column name in the reference's year declarations file", required=False)
    
    args = parser.parse_args()
    
    config = Config(args)

    time1 = time.time()
    
    veg_all = merge_input_data(config)

    veg_all = apply_threshold_values(veg_all, config)

    veg_all.sort_values('NewID', inplace = True)
    veg_all.to_csv(args.output,index=False)
    
    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



