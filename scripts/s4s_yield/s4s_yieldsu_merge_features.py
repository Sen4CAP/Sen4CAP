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

#NOTE: In other files, the extracted dict contains [year][crop] while here we extract [crop][year]
def read_input_files(input_file):
    input_files = dict()
    if input_file is None or input_file == "":
        return input_files    
    input_file_dir = os.path.dirname(input_file)
    with open(input_file, "r") as file:
        # skip headers
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if len(row) == 3:
                year = row[0]
                crop_type = row[1]
                file_path = row[2]
                abs_path = file_path
                if not os.path.isabs(file_path):
                    abs_path = os.path.join(input_file_dir, file_path)
                if not crop_type in input_files:
                    input_files[crop_type] = dict()
                input_files[crop_type][year] = abs_path

    return input_files

def merge_yearly_ct_files(years_input_files, output_file) : 
    sorted_years = list(years_input_files.keys())
    sorted_years.sort()
    list_files = []
    for year in sorted_years : 
        list_files.append(years_input_files[year])
    out_header = []
    all_lines = []
    for year, input_file in zip(sorted_years, list_files):
        print("Processing file {} for year {}".format(input_file, year))
        with open(input_file, "r") as file:  
            r = csv.reader(file)
            header = next(r)
            if len(out_header) == 0 and len(header) > 1 : 
                out_header = header
                out_header.insert(len(out_header)-1, "year")
                all_lines.append(out_header)

            # ensure that we have the header
            if len(out_header) > 0 :
                for item in r:
                    item.insert(len(item)-1, year)
                    all_lines.append(item)
    
    with open(output_file, 'w') as csvoutput:
        writer = csv.writer(csvoutput)
        writer.writerows(all_lines)
    
def main():
    parser = argparse.ArgumentParser(
        description="Merges the yield features for all years, by crop type"
    )
    parser.add_argument("-i", "--input", help="File containing the yield features", required=True)
    parser.add_argument("-o", "--output", help="Output file containing the merged yield features", required=True)
    
    args = parser.parse_args()
    
    input_files = read_input_files(args.input)

    output_files_dict = dict()
    for crop_type in input_files.keys() : 
        print("Processing ct = {}".format(crop_type))
        years_input_files = input_files[crop_type]   
        
        output_file, file_extension = os.path.splitext(args.output)
        output_file = output_file + "_" + str(crop_type) + ".csv"
        output_files_dict[str(crop_type)] = output_file
        
        merge_yearly_ct_files(years_input_files, output_file)


    with open(args.output, 'w') as out_sg:  
        writer = csv.writer(out_sg)
        writer.writerow(["crop_type", "features_file"])
        for key, value in output_files_dict.items():
            # In this case, write the relative path instead of the full path
            # as usually the full path is from a temporary folder
            # NOTE: Attention in the loading modules to handle the relative path
            writer.writerow([key, os.path.basename(value)])
  
    
if __name__ == "__main__":
    main()
