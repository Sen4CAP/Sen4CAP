#!/usr/bin/env python

import argparse
import logging
from collections import defaultdict
from datetime import date
import datetime as dt
# from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
import pipes
from osgeo import osr, gdal, ogr
from osgeo import gdalconst
import re
import sys
import csv
import errno
import ntpath
import subprocess
import numpy as np

ID_COL_NAME = "NewID"
CT_COL_NAME = "crop_code"

SAFY_YIELD_COL_NAME = "Yield"
SAFY_D0OUT_COL_NAME = "d0out"
SAFY_SENBOUT_COL_NAME = "SenBout"

OUTPUT_FEATURE_NAMES = ['MeanLaiSGWinter', 'SumLaiSGInt0', 'SumLaiSGInt1', 'SumLaiSGInt2', 'MaxSG', 'DayMaxSG', 'MaxLAI', 'ColdT0', 'ColdT1', 'HotT2', 'SumT1', 'SumT2', 'SumT251', 'SumT252', 'SumP1', 'SumP2', 'SumR1', 'SumR2', 'SumE1', 'SumE2', 'MeanT1', 'MeanT2', 'MeanP1', 'MeanP2', 'MeanR1', 'MeanR2', 'MeanE1', 'MeanE2', 'MeanSW10', 'MeanSW11', 'MeanSW12', 'MeanSW20', 'MeanSW21', 'MeanSW22', 'MeanSW30', 'MeanSW31', 'MeanSW32', 'MeanSW40', 'MeanSW41', 'MeanSW42', SAFY_YIELD_COL_NAME, SAFY_D0OUT_COL_NAME, SAFY_SENBOUT_COL_NAME, CT_COL_NAME]

INDICES_COLUMN_SUFFIXES=["Ind_MaxLai", "Ind_HalfLai", "Ind_Emerg", "Ind_EndLai"]

LAI_FEATURES_COLUMN_SUFFIXES=["_mean_LaiSGWinter", "_sum_LaiSGInt0", "_sum_LaiSGInt1", "_sum_LaiSGInt2", "_max_SG", "_daymax_SG", "_max_LAI"]

class InputColumnsInfo(object) : 
    def __init__(self, header):
        self.header = header
        self.id_pos = header.index(ID_COL_NAME)
        self.ct_pos = header.index(CT_COL_NAME)

        # extract the LAI features indices 
        self.lai_features_indices = self.get_column_indices(header, LAI_FEATURES_COLUMN_SUFFIXES)
        if len(self.lai_features_indices) > 0 and len(self.lai_features_indices) != len (LAI_FEATURES_COLUMN_SUFFIXES) :
            print ("Error: the number of LAI features indices in columns is {} while it was expected {}. Exiting ...".format(len(self.lai_features_indices), len (LAI_FEATURES_COLUMN_SUFFIXES)))
            sys.exit(1)

        # extract the crop partioning indices 
        self.crop_indices = self.get_column_indices(header, INDICES_COLUMN_SUFFIXES)
        if len(self.crop_indices) != len (INDICES_COLUMN_SUFFIXES) :
            print ("Error: the number of crop indices in columns is {} while it was expected {}. Exiting ...".format(len(self.crop_indices), len (INDICES_COLUMN_SUFFIXES)))
            sys.exit(1)
        
        # extract the weather column indices
        self.weather_evap_indices = self.get_column_indices(header, ["_evap"])
        self.weather_prec_indices = self.get_column_indices(header, ["_prec"])
        self.weather_rad_indices = self.get_column_indices(header, ["_rad"])
        self.weather_swvl1_indices = self.get_column_indices(header, ["_swvl1"])
        self.weather_swvl2_indices = self.get_column_indices(header, ["_swvl2"])
        self.weather_swvl3_indices = self.get_column_indices(header, ["_swvl3"])
        self.weather_swvl4_indices = self.get_column_indices(header, ["_swvl4"])
        self.weather_tmax_indices = self.get_column_indices(header, ["_tmax"])
        self.weather_tmean_indices = self.get_column_indices(header, ["_tmean"])
        self.weather_tmin_indices = self.get_column_indices(header, ["_tmin"])
        
        self.safy_d0_indices = self.get_column_indices(header, [SAFY_D0OUT_COL_NAME])
        self.safy_senb_indices = self.get_column_indices(header, [SAFY_SENBOUT_COL_NAME])
        self.safy_yield_indices = self.get_column_indices(header, [SAFY_YIELD_COL_NAME])
        
        # TODO: Here we should remove the duplicated dates (due to intersection of several S2 tiles)
                    
    def get_column_indices(self, header, substr_list) :
        indices_list = []
        for substr in substr_list:
            i = 0
            for col in header:
                if substr in col:
                    indices_list.append(i)
                i += 1
        return indices_list
    
def handle_file(input, writer):
    writer.writerow([ID_COL_NAME] + [x for x in OUTPUT_FEATURE_NAMES] )
    
    with open(input, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.reader(read_obj)
        header = next(csv_reader)
        # print ("Columns: {}".format(header))
        column_infos = InputColumnsInfo(header)
        
        N = 1000
        # Extract the relevant columns
        if header != None:
            batch_rows = []
            for row in csv_reader:
                batch_rows.append(row)
                if len(batch_rows) == N :
                    handle_batch_record(batch_rows,column_infos, writer)
                    # reinitialize the batch
                    batch_rows = []
            
            # last batch or less than max lines in file
            if len(batch_rows) > 0 :
                handle_batch_record(batch_rows, column_infos, writer)

def FloatOrZero(value):
    try:
        return float(value)
    except:
        return 0.0

def filter_row_values(row, indices) :
    return [FloatOrZero(row[i]) for i in indices]
   
def update_result_row_idx_and_increment(result, pos, value) :
    result[pos] = value
    pos = pos + 1 
    return pos

def handle_batch_record(rows, column_infos, writer):
    outputs = []
    batch_results = []
    for row in rows:
        id = row[column_infos.id_pos]
        crop_type = row[column_infos.ct_pos]

        try:
            IndMaxLai = int(row[column_infos.crop_indices[0]])
            IndHalfLai = int(row[column_infos.crop_indices[1]])
            IndEmerg = int(row[column_infos.crop_indices[2]])
            IndEndLai = int(row[column_infos.crop_indices[3]])
        except ValueError:
            print("Cannot read crop growth indices for id = {}. It will be ignored ...".format(id))
            continue

        weather_evap = np.array(filter_row_values(row, column_infos.weather_evap_indices))
        weather_prec = np.array(filter_row_values(row, column_infos.weather_prec_indices))
        weather_rad = np.array(filter_row_values(row, column_infos.weather_rad_indices))
        weather_swvl1 = np.array(filter_row_values(row, column_infos.weather_swvl1_indices))
        weather_swvl2 = np.array(filter_row_values(row, column_infos.weather_swvl2_indices))
        weather_swvl3 = np.array(filter_row_values(row, column_infos.weather_swvl3_indices))
        weather_swvl4 = np.array(filter_row_values(row, column_infos.weather_swvl4_indices))
        weather_tmax = np.array(filter_row_values(row, column_infos.weather_tmax_indices))
        weather_tmean = np.array(filter_row_values(row, column_infos.weather_tmean_indices))
        weather_tmin = np.array(filter_row_values(row, column_infos.weather_tmin_indices))
        
        i = 0
        result = [None] * (len(OUTPUT_FEATURE_NAMES) + 1)     # + 1 for the NewID
        i = update_result_row_idx_and_increment(result, i, int(id))                                                              # ['NewID'] 
        
        has_lai_features = (column_infos.lai_features_indices is not None and len(column_infos.lai_features_indices) > 0)        
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[0]] if has_lai_features else None)                            # MeanLaiSGWinter
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[1]] if has_lai_features else None)                            # SumLaiSGInt0
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[2]] if has_lai_features else None)                            # SumLaiSGInt1
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[3]] if has_lai_features else None)                            # SumLaiSGInt2
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[4]] if has_lai_features else None)                            # MaxSG
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[5]] if has_lai_features else None)                            # DayMaxSG
        i = update_result_row_idx_and_increment(result, i, row[column_infos.lai_features_indices[6]] if has_lai_features else None)                            # MaxLAI
        
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_tmin[IndEmerg:IndHalfLai+1]<=0)))                  # ['ColdT0']   
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_tmin[IndHalfLai:IndMaxLai+1]<=0)))                 # ['ColdT1']   
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_tmax[IndMaxLai:IndEndLai+1]>=35)))                 # ['HotT2']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(np.maximum(weather_tmean[IndHalfLai:IndMaxLai+1],0))))     # ['SumT1']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(np.maximum(weather_tmean[IndMaxLai :IndEndLai+1],0))))     # ['SumT2']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(np.maximum(weather_tmax[IndHalfLai:IndMaxLai+1]-25,0))))   # ['SumT251']  
        i = update_result_row_idx_and_increment(result, i, int(np.sum(np.maximum(weather_tmax[IndMaxLai :IndEndLai+1]-25,0))))   # ['SumT252']  
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_prec[IndHalfLai:IndMaxLai+1])))                    # ['SumP1']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_prec[IndMaxLai :IndEndLai+1])))                    # ['SumP2']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_rad[IndHalfLai:IndMaxLai+1])/1000))                # ['SumR1']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_rad[IndMaxLai :IndEndLai+1])/1000))                # ['SumR2']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_evap[IndHalfLai:IndMaxLai+1])))                    # ['SumE1']    
        i = update_result_row_idx_and_increment(result, i, int(np.sum(weather_evap[IndMaxLai :IndEndLai+1])))                    # ['SumE2']    
        i = update_result_row_idx_and_increment(result, i, np.mean(np.maximum(weather_tmean[IndHalfLai:IndMaxLai+1],0)))         # ['MeanT1']   
        i = update_result_row_idx_and_increment(result, i, np.mean(np.maximum(weather_tmean[IndMaxLai :IndEndLai+1],0)))         # ['MeanT2']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_prec[IndHalfLai:IndMaxLai+1]))                        # ['MeanP1']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_prec[IndMaxLai :IndEndLai+1]))                        # ['MeanP2']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_rad[IndHalfLai:IndMaxLai+1])/1000)                    # ['MeanR1']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_rad[IndMaxLai :IndEndLai+1])/1000)                    # ['MeanR2']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_evap[IndHalfLai:IndMaxLai+1]))                        # ['MeanE1']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_evap[IndMaxLai :IndEndLai+1]))                        # ['MeanE2']   
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl1[IndEmerg  :IndHalfLai+1]))                      # ['MeanSW10'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl1[IndHalfLai:IndMaxLai +1]))                      # ['MeanSW11'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl1[IndMaxLai :IndEndLai +1]))                      # ['MeanSW12'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl2[IndEmerg  :IndHalfLai+1]))                      # ['MeanSW20'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl2[IndHalfLai:IndMaxLai +1]))                      # ['MeanSW21'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl2[IndMaxLai :IndEndLai +1]))                      # ['MeanSW22'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl3[IndEmerg  :IndHalfLai+1]))                      # ['MeanSW30'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl3[IndHalfLai:IndMaxLai +1]))                      # ['MeanSW31'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl3[IndMaxLai :IndEndLai +1]))                      # ['MeanSW32'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl4[IndEmerg  :IndHalfLai+1]))                      # ['MeanSW40'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl4[IndHalfLai:IndMaxLai +1]))                      # ['MeanSW41'] 
        i = update_result_row_idx_and_increment(result, i, np.mean(weather_swvl4[IndMaxLai :IndEndLai +1]))                      # ['MeanSW42'] 
        
        safy_yield = filter_row_values(row, column_infos.safy_yield_indices)
        safy_d0 = filter_row_values(row, column_infos.safy_d0_indices)
        safy_senb = filter_row_values(row, column_infos.safy_senb_indices)
        
        i = update_result_row_idx_and_increment(result, i, safy_yield[0] if safy_yield is not None and len(safy_yield) > 0 else None)   # ['safyyield']
        i = update_result_row_idx_and_increment(result, i, safy_d0[0] if safy_d0 is not None and len(safy_d0) > 0 else None)            # ['safyd0'] 
        i = update_result_row_idx_and_increment(result, i, safy_senb[0] if safy_senb is not None and len(safy_senb) > 0 else None)      # ['safysenb']
       
        i = update_result_row_idx_and_increment(result, i, int(crop_type) if crop_type is not None and len(crop_type) > 0 else None)    # ['crop_type']
        
        batch_results.append(result)

    # write batch result lines
    writer.writerows(batch_results)
        
def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-i", "--input", help="File containing all merged extracted features (Weather, LAI, SG Crop Growth and SAFY)", required=True)
    parser.add_argument("-o", "--output", help="Output file containing the extracted yield features", required=True)
    
    args = parser.parse_args()
    
    # handle the created merge file
    with open(args.output, "w") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        handle_file(args.input, writer)
    
if __name__ == "__main__":
    main()
