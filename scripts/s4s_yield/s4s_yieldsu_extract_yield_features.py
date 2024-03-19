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
        print("WARNING: Command `{}` failed with exit code {}".format(cmd_line, result))

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

def write_output_files(out_file, dict_files) :
    with open(out_file, 'w') as out_sg:  
        writer = csv.writer(out_sg)
        writer.writerow(["year", "crop_type", "features_file"])
        for year, dict_ct in dict_files.items():
            output_ct_files_dict = dict_ct
            for key, value in output_ct_files_dict.items():
                # In this case, write the relative path instead of the full path
                # as usually the full path is from a temporary folder
                # NOTE: Attention in the loading modules to handle the relative path
                writer.writerow([year, key, os.path.basename(value)])


def main():
    parser = argparse.ArgumentParser(
        description="Performs the Savitzky Golay interpolation of the BI values in received file"
    )
    parser.add_argument("-i", "--input", help="File containing all merged extracted features files for each crop", required=True)
    parser.add_argument("-y", "--year", help="The maximum year for the yield features", type=int, required=True)
    parser.add_argument("-p", "--output-prev-years", help="Output merged file for the previous years", required=True)
    parser.add_argument("-o", "--output", help="Output merged file", required=True)
    
    args = parser.parse_args()
    
    input_files = read_input_files(args.input)

    prev_years_output_files_dict = dict()
    cur_year_output_files_dict = dict()
    for year in input_files.keys() : 
        ct_input_files = input_files[year]    
        output_ct_files_dict = dict()
        for crop_type in ct_input_files.keys() : 
            input_file = ct_input_files[crop_type]
            
            output_file, file_extension = os.path.splitext(args.output)
            output_file = output_file + "_" + str(year) + "_" + str(crop_type) + ".csv"
            output_ct_files_dict[str(crop_type)] = output_file
            
            print("Extracting yield features for crop type {} and file {}".format(crop_type, input_file))
            
            command = []
            command += ["extract_yield_features.py", 
                        "--input", input_file, 
                        "--output", output_file]

            run_command(command)
        if int(year) < args.year:
            prev_years_output_files_dict[str(year)] = output_ct_files_dict
        elif int(year) == args.year:
            cur_year_output_files_dict[str(year)] = output_ct_files_dict
    
    write_output_files(args.output, cur_year_output_files_dict)
    write_output_files(args.output_prev_years, prev_years_output_files_dict)

    
if __name__ == "__main__":
    main()
