#!/usr/bin/env python
from __future__ import print_function

import argparse
import subprocess

import pandas as pd
import numpy as np
import sys
import glob
from itertools import islice
import datetime as dt
import csv
import time
import os

ID_COL_NAME = "NewID"

class Config(object):
    def __init__(self, args):
        self.esu_path = args.esu_path
        self.lai_merged_path = args.lai_merged_path
        self.working_dir = args.working_dir
        self.output = args.output

class MetricCSVConverter(object) :
    class SelectedColumns(object):
        class DateColumnsInfos(object):
            def __init__(self, date):
                self.date = date
                # The order is mean, valid, invalid
                self.indexes = [-1] * 3
            
            def set_mean_idx(self, idx) :
                self.indexes[0] = idx

            def set_valid_pix_idx(self, idx) :
                self.indexes[1] = idx

            def set_invalid_pix_idx(self, idx) :
                self.indexes[2] = idx

            def get_mean(self, values):
                return values[self.indexes[0]]    

            def get_valid_pix_cnt(self, values):
                try :
                    return (int)(values[self.indexes[1]])
                except ValueError:
                    return np.nan

            def get_invalid_pix_cnt(self, values):
                try :
                    return (int)(values[self.indexes[2]])
                except ValueError:
                    return np.nan

            def get_total_pix_cnt(self, values):
                return self.get_valid_pix_cnt(values) + self.get_invalid_pix_cnt(values)

        
        def __init__(self, col_names, global_col_indices, id_col_global_idx):
            
            col_names.sort()
            self.global_col_indices = global_col_indices
            self.id_col_global_idx = id_col_global_idx

            self.dates = dict()
            self.idx_to_date = dict()

            cur_idx = 1 # We start from 1 as on the first position in data will be always the ID
            for col in col_names:
                # get the date until the first _
                idx = col.index('_')
                date_str = ""
                if idx > 0:
                    date_str = str(col[:idx])
                    if not date_str in self.dates:
                        self.dates[date_str] = self.DateColumnsInfos(date_str) 
                        
                    if "_mean_" in col:
                        self.dates[date_str].set_mean_idx(cur_idx)
                        
                    if "_valid_pixels_cnt_" in col:
                        self.dates[date_str].set_valid_pix_idx(cur_idx)
                        
                    if "_invalid_pixels_cnt_" in col:
                        self.dates[date_str].set_invalid_pix_idx(cur_idx)

                    self.idx_to_date[cur_idx] = date_str
                cur_idx = cur_idx+1
                
        def get_all_global_col_indices(self) :
            return [self.id_col_global_idx] + self.global_col_indices

    def get_selected_columns(self, columns) : 
        bi_cols = ['_mean_LAI', '_valid_pixels_cnt_LAI', '_invalid_pixels_cnt_LAI', '_total_pixels_cnt_LAI']
        col_names = []
        cur_idx = 0
        global_col_indices = []
        id_col_global_idx = -1
        for name in columns:
            if name != ID_COL_NAME:
                for bi_col in bi_cols:
                    if bi_col in name:
                        col_names.append(name)
                        global_col_indices.append(cur_idx)
            else :
                id_col_global_idx = cur_idx
            cur_idx = cur_idx+1

        # print("Selected columns: {}".format(col_names))
        return self.SelectedColumns(col_names, global_col_indices, id_col_global_idx)

    
    def handle_csv_file(self, input, output_handler) :
        with open(input, 'r') as read_obj:
            # pass the file object to reader() to get the reader object
            csv_reader = csv.reader(read_obj)
            header = next(csv_reader)
            selCols = self.get_selected_columns(header)
            
            N = 1000
            # Extract the relevant columns
            if header != None:
                while True:
                    gen = islice(read_obj,N)
                    arr = np.genfromtxt(gen, delimiter=',', usecols=selCols.get_all_global_col_indices(), encoding=None)
                    all_cropfields = np.array(arr.tolist())
                    self.handle_batch_record(selCols, all_cropfields, output_handler)
                    if arr.shape[0]<N:
                        break

    def handle_batch_record(self, selCols, all_cropfields, output_handler):
        lines_to_write = []
        for cropfield_descr in all_cropfields:
            cropfield_n = int(cropfield_descr[0])
            for date_info in selCols.dates.values():
                out_line = [int(cropfield_descr[0]), date_info.date]
                # out_line.append(date_info.get_invalid_pix_cnt(cropfield_descr)) 
                out_line.append(date_info.get_mean(cropfield_descr)) 
                out_line.append(date_info.get_total_pix_cnt(cropfield_descr)) 
                out_line.append(date_info.get_valid_pix_cnt(cropfield_descr)) 
                
                lines_to_write.append(out_line)

        output_handler.writerows(lines_to_write)

    def translate_csv(self, in_csv_file, out_cvs_file) :
        with open(out_cvs_file, "w") as out_file:
            output_handler = csv.writer(out_file, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
            output_handler.writerows([["ESUId", "date", "LAImean", "Nbpix", "Nbpixvalid"]])

            start = time.time()
            self.handle_csv_file(in_csv_file, output_handler)
            end = time.time()
            print("Converting LAI CSV file took {}".format(end - start))


def bv_time_series_aggregation(config):
    SUtable = pd.read_csv(config.esu_path) # Import ESU table containg SU weight for each ESU

    # LAIobstable = pd.DataFrame(columns=['cropfield','date','LAImean','NbPix','NbPixValid'])# Create final BV Time Serie table
    # TileList=glob.glob(InputDir+'Output/Parcellaire/'+Yearst+'/ESU_Random*') # list ESU Rasterized file to list tiles
    
    # Import the Table of LAI extracted by ESU for all tiles with the BV processor, for the year y, the table should be presented as the exemple as follow
    # cropfield is in our case the name of the randomly selected pixel of an ESU
    # LAIobs= pd.DataFrame({'cropfield':[1,7,1,7],'date':[1,1,2,3],'LAImean':[2,1,3,2],'Nbpix':[150,50,150,50],'Nbpixvalid':[100,25,100,50]})
    
    lai_merged_converted_path = os.path.join(config.working_dir, "translated_lai.csv")
    MetricCSVConverter().translate_csv(config.lai_merged_path, lai_merged_converted_path)
    # I don't know how you store this intermediate outputs in the yield processor but you get the idea.
    LAIobs = pd.read_csv(lai_merged_converted_path)

    # Merge of the Observation with the Weighted ESU table for the aggregation
    LAIobs = pd.merge(LAIobs, SUtable, left_on='ESUId', right_on='ESUid') 
    LAIobs = LAIobs.loc[np.where(LAIobs['LAImean']>0)[0]]
    
    filename, file_extension = os.path.splitext(config.output)
    LAIobs.to_csv(filename + "_intermediate.csv", index=False)
    # print(LAIobs.to_string())
    
    # Aggregation of the observation from ESU to SU weighting by the relative size of the ESU within the SU
    output_ct_files_dict = dict()
    filename, file_extension = os.path.splitext(config.output)

    for c in np.unique(LAIobs['CropID']):
        cropfield=[]
        date=[]
        LAImean=[]
        Nbpix=[]
        Nbpixvalid=[]
        unique_dates = []

        # print("CropID = {}".format(c)) 
        for su in np.unique(LAIobs['SUid']):
            # print("SUid = {}".format(su))
            for d in np.unique(LAIobs['date']):
                d_str = str(d)
                # print("Date = {}".format(d))
                wherelai = np.intersect1d(np.where(LAIobs['date']==d), np.intersect1d(np.where(LAIobs['SUid']==su), np.where(LAIobs['CropID']==c)))
                # wherelai = np.intersect1d(np.where(LAIobs['date']==d), np.where(LAIobs['SUid']==su))
                # print("WhereLAI = {}".format(wherelai)) 
                if len(wherelai) == 0:
                    continue
                if len(wherelai)> len(np.unique(LAIobs['ESUid'].iloc[wherelai])):
                    w=[]
                    for u in np.unique(LAIobs['ESUid'].iloc[wherelai]):
                        w.append(np.where(LAIobs['ESUid'].iloc[wherelai]==u)[0][np.argmax(LAIobs['Nbpixvalid'].iloc[wherelai][LAIobs['ESUid'].iloc[wherelai]==u])])
                    wherelai = np.array(w)        
                date.append(d_str)
                if not d_str in unique_dates:
                    unique_dates.append(d_str)
                cropfield.append(su)
                LAImean.append(np.sum(LAIobs['LAImean'].iloc[wherelai]*LAIobs['Weight'].iloc[wherelai])/np.sum(LAIobs['Weight'].iloc[wherelai]))
                
                Nbpix.append(np.sum(LAIobs['Nbpix'].iloc[wherelai] * LAIobs['Weight'].iloc[wherelai]))
                Nbpixvalid.append(np.sum(LAIobs['Nbpixvalid'].iloc[wherelai] *LAIobs['Weight'].iloc[wherelai]))
        
        # compute the columns
        output_dict = dict()
        output_dict["NewID"] = []
        output_dict["crop_code"] = []
        unique_dates.sort()
        for dt in unique_dates:
            output_dict[dt + "_mean_LAI"] = []
            output_dict[dt + "_valid_pixels_cnt_LAI"] = []
            output_dict[dt + "_total_pixels_cnt_LAI"] = []
        
        curNewId = None
        for (cf, dt, lai, total_pix, valid_pix) in zip(cropfield, date, LAImean, Nbpix, Nbpixvalid):
            if curNewId is None or cf != curNewId : 
                # add entries for all columns in dictionary (a line)
                for k in output_dict.keys() : 
                    output_dict[k].append(None)
                output_dict["NewID"][-1] = cf
                output_dict["crop_code"][-1] = c
                curNewId = cf

            output_dict[dt + "_mean_LAI"][-1] = lai
            output_dict[dt + "_valid_pixels_cnt_LAI"][-1] = valid_pix
            output_dict[dt + "_total_pixels_cnt_LAI"][-1] = total_pix
        
        # cropfield is not anymore the ESU but now the SU, the rest of the yield feature computation model can take place.
        LAIobs_new=pd.DataFrame(output_dict) 
        out_file_name = filename + "_" + str(c) + ".csv"
        LAIobs_new.to_csv(out_file_name, index=False)
        output_ct_files_dict[str(c)] = out_file_name

        LAIobs_new2=pd.DataFrame({'NewID':cropfield,'date':date,'LAImean':LAImean,'Nbpix':Nbpix,'Nbpixvalid':Nbpixvalid})     
        LAIobs_new2.to_csv(filename + "_" + str(c) + "_orig.csv", index=False)

    # write the list of files created for each individual crop type
    with open(config.output, 'w') as output_csv_file:  
        writer = csv.writer(output_csv_file)
        writer.writerow(["crop_code", "su_lai_aggregated_file"])
        for key, value in output_ct_files_dict.items():
            writer.writerow([key, value])

def main():
    parser = argparse.ArgumentParser(description="Yield SU Parcels Extraction")
    parser.add_argument('-e', '--esu-path', help="ESU csv path")
    parser.add_argument('-l', '--lai-merged-path', help="LAI merged markers path")
    parser.add_argument('-w', '--working-dir', help="Working directory")
    parser.add_argument('-o', '--output', help="LAI grouped file CSV output path")
    
    args = parser.parse_args()

    config = Config(args)
    
    bv_time_series_aggregation(config)

if __name__ == "__main__":
    main()
