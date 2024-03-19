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

def read_prev_years_files(input_file):
    input_files = dict()
    if input_file is None or input_file == "":
        return input_files    
    input_file_dir = os.path.dirname(input_file)
    with open(input_file, "r") as file:
        # skip headers
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if len(row) == 2:
                crop_type = row[0]
                file_path = row[1]
                abs_path = file_path
                if not os.path.isabs(file_path):
                    abs_path = os.path.join(input_file_dir, file_path)
                input_files[crop_type] = abs_path

    return input_files

def main():
    parser = argparse.ArgumentParser(
        description="Yield model computation"
    )
    parser.add_argument(
        "-a", "--algo", required=False, default="rf", help="The algorithm to be used. lm - LinerarRegression, svm - SupportVectortMachine. Default rf = RandomForest", choices=['rf', 'lm', 'svm']
    )
    parser.add_argument(
        "-s", "--selection", required=False, default="none", help="The selection mode. Possible values: automatic or manual or none", choices=['none', 'manual', 'automatic'] 
    )

    parser.add_argument(
        "-m", "--manual-selection-features", required=False, help="The selection features list for the manual mode", nargs='+', type=str
    )

    parser.add_argument(
        "-n", "--max-automatic-features-no", required=False, help="The maximum number of selection features for the automatic mode", type=int, default = 44
    )
    
    parser.add_argument(
        "-i", "--input-features", required=True, help="The input features file"
    )

    parser.add_argument(
        "-r", "--yield-reference", required=True, help="The input yield reference file"
    )

    parser.add_argument(
        "-c", "--crop-codes", required=True, help="List of crop codes"
    )  ### change  ex : -c /mnt/archive/orchestrator_temp/s4s_yield_feat/8067/81836-s4s-merge-lai-with-grid/lai_with_grid.csv -> I saw that the crop type was added in the lai with grid file

    parser.add_argument(
        "-o", "--output", required=True, help="The output estimation file"
    )

    args = parser.parse_args()
    
    input_files = read_input_files(args.input_features)
    prev_years_files = read_prev_years_files(args.yield_reference)
    
    if len(input_files) != 1:
        print("Input files for year does not contains exactly one year but it has = {}. Exiting ...".format(input_files))
        sys.exit(1)

    proc_year = list(input_files.keys())[0]
    ct_input_files = input_files[proc_year]

    output_files_dict = dict()
    for crop_type in ct_input_files.keys() : 
        input_file = ct_input_files[crop_type]
        if not crop_type in prev_years_files:
            print("Crop type {} will be ignored as it was not found in the previous years files".format(crop_type))
            continue
        prev_years_file = prev_years_files[crop_type]
        
        output_file, file_extension = os.path.splitext(args.output)
        output_file = output_file + "_" + str(crop_type) + ".csv"
        output_files_dict[str(crop_type)] = output_file
        
        print("Extracting yield features for crop type {} and file {}".format(crop_type, input_file))
        
        command = []
        command += ["S4S_Yield_Model.py", 
                    "--algo", args.algo, 
                    "--selection", args.selection,
                    "--manual-selection-features", args.manual_selection_features,
                    "--max-automatic-features-no", args.max_automatic_features_no,
                    "--input-features", input_file,
                    "--yield-reference", prev_years_file,
                    "--crop-codes", args.crop_codes,
                    "--output", output_file,
                    "--has-trend"
                    ]

        run_command(command)
    
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
