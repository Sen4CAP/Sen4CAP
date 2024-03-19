#!/usr/bin/env python

import argparse
from collections import defaultdict
from datetime import date
import datetime as dt
# from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
import re
import pipes
import shutil
import subprocess
import sys
import csv
import errno

def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))

    print(cmd_line)
    result = subprocess.call(args, env=env)
    if result != 0:
        raise RuntimeError(
            "Command `{}` failed with exit code {}".format(cmd_line, result)
        )

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
                if not year in input_files:
                    input_files[year] = dict()
                input_files[year][crop_type] = abs_path
                    
    return input_files

def main():
    parser = argparse.ArgumentParser(
        description="Performs the Savitzky Golay interpolation of the BI values in received file"
    )
    parser.add_argument("-i", "--sg-list-file", help="File containing the SG indices files for each crop", required=True)
    parser.add_argument("-t", "--trend-features-list-file", help="File containing the trend features files for each crop", required=True)
    parser.add_argument("-w", "--weather-metrics-file", help="File or folder containing the weather features merged", required=True)
    parser.add_argument("-l", "--sg-metrics-file", help="File or folder containing the SG weather features", required=False, default="")
    parser.add_argument("-o", "--output", help="Output merged file", required=True)
    parser.add_argument("-g", "--ignnodatecol", help="Ignore date column", required=False, default = 0)
    
    args = parser.parse_args()
    
    sg_input_files = read_input_files(args.sg_list_file)
    trend_input_files = read_input_files(args.trend_features_list_file)
    sg_metrics_files = read_input_files(args.sg_metrics_file)

    output_files_dict = dict()
    
    for year in sg_input_files.keys() : 
        sg_ct_input_files = sg_input_files[year]
        trend_ct_input_files = trend_input_files[year]
        sg_ct_metrics_files = sg_metrics_files[year]
        ct_output_files_dict = dict()
        for crop_type in sg_ct_input_files.keys() : 
            if trend_ct_input_files is not None:
                sg_input_file = sg_ct_input_files[crop_type]
                trend_input_file = trend_ct_input_files.get(crop_type)
                sg_metrics_file = sg_ct_metrics_files.get(crop_type, "")
                
                if trend_input_file is not None:
                    output_file, file_extension = os.path.splitext(args.output)
                    output_file = output_file + "_" + str(year) + "_" + str(crop_type) + ".csv"
                    ct_output_files_dict[str(crop_type)] = output_file
                    
                    print("Merging SG with weather for crop type {} and file {}".format(crop_type, sg_input_file))
                    
                    command = []
                    command += ["otbcli", "Markers1CsvMerge"]
                    command += ["-il", sg_input_file, trend_input_file, args.weather_metrics_file]
                    if sg_metrics_file != "" : 
                        command += [sg_metrics_file]
                    command += ["-ignnodatecol", args.ignnodatecol]
                    command += ["-out", output_file]

                    run_command(command)
                
                else :
                    print("WARNING: Ignoring crop type = {} and SG file {} for year {} as the trend features do not exist for this crop type".format(crop_type, sg_input_file, year))
                
            else :
                print("WARNING: Ignoring crop type = {} for year {} as no trend features exist for this year".format(crop_type, year))
        
        output_files_dict[str(year)] = ct_output_files_dict
        
    with open(args.output, 'w') as out_sg:  
        writer = csv.writer(out_sg)
        writer.writerow(["year", "crop_type", "features_file"])
        for year, dict_ct in output_files_dict.items():
            output_ct_files_dict = dict_ct
            for key, value in output_ct_files_dict.items():
                writer.writerow([year, key, value])
    
if __name__ == "__main__":
    main()
