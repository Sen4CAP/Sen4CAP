#!/usr/bin/env python
from __future__ import print_function

import argparse
import subprocess

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import sys
import glob
from itertools import islice
import datetime as dt
import csv
import time
import os

from pathlib import Path

class Config(object):
    def __init__(self, args):
        self.input = args.input
        self.year = args.year
        self.output = args.output

def split_input_file(input_file, output_dir_path):
    input_files = dict()

    filename = Path(input_file).stem

    HistoricalRecord = pd.read_csv(input_file)
    columns = HistoricalRecord.columns
    columns = columns.drop('crop_code')
    for i, g in HistoricalRecord.groupby(['crop_code']):
        # print (i)
        g = g.sort_values('SUid')
        # print (g)    
        out_file_name = os.path.join(output_dir_path, filename + "_" + str(i) + ".csv")
        g.to_csv(out_file_name, columns = columns, index=False)
        input_files[i] = out_file_name

    return input_files

def compute_trend_feature(input_file, year_to_process, output_file) :

    HistoricalRecord = pd.read_csv(input_file)
    
    ## ImportHistorical Records given by user (here is an exemple)
    #HistoricalRecord = pd.DataFrame({'SUid':['1','2','3'],'2010':[80,50,65],'2011':[83,48,60],'2012':[78,42,52],'2013':[80,50,65],'2014':[85,54,70],'2015':[83,52,67],
    #                                        '2016':[85,56,69],'2017':[84,55,68],'2018':[88,58,74],'2019':[87,59,71],'2020':[90,58,72]})
    # print(HistoricalRecord)
    
    #Compute Features for year_to_process
    #TODO : here we should not assume that it SUid is the first one
    Years = [int(y) for y in HistoricalRecord.columns[1:]]
    YearLoc = np.where(np.array(Years) == year_to_process)[0][0]
    Trend=[]

    for i,SU in enumerate(HistoricalRecord.SUid):
        Interp = LinearRegression().fit(np.array(Years[:YearLoc]).reshape(YearLoc,1),np.array(HistoricalRecord.loc[i,][1:YearLoc+1]).reshape(YearLoc,1))
        Trend.append(int(Interp.predict(np.array([[year_to_process]]))))

    TrendFeature=pd.DataFrame({'NewID':HistoricalRecord.SUid,'Trend':Trend })
    TrendFeature.to_csv(output_file, index=False)

def main():
    parser = argparse.ArgumentParser(description="Yield SU Parcels Extraction")
    parser.add_argument('-i', '--input', help="Input file containing yearly yields for SU")
    parser.add_argument('-y', '--year', type=int, nargs="+", help="Year where to compute")
    parser.add_argument('-o', '--output', help="Output")
    
    args = parser.parse_args()

    config = Config(args)
    
    out_path = Path(config.output)
    output_dir_path = out_path.parent.absolute()
    output_file, file_extension = os.path.splitext(config.output)

    crop_code_files = split_input_file(config.input, output_dir_path)
    print(crop_code_files)

    output_files_dict = dict()
    for year in config.year:
        output_ct_files_dict = dict()
        for crop_type in crop_code_files.keys() : 
            input_file = crop_code_files[crop_type]
            crop_output_file = output_file + "_" + str(year) + "_" + str(crop_type) + ".csv"
            compute_trend_feature(input_file, year, crop_output_file)
            output_ct_files_dict[str(crop_type)] = crop_output_file
    
        output_files_dict[str(year)] = output_ct_files_dict
        
    with open(args.output, 'w') as out_sg:  
        writer = csv.writer(out_sg)
        writer.writerow(["year", "crop_code", "features_file"])
        for year, dict_ct in output_files_dict.items(): 
            output_ct_files_dict = dict_ct
            for key, value in output_ct_files_dict.items():
                writer.writerow([year, key, value])

if __name__ == "__main__":
    main()
