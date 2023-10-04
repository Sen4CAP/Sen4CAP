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
    with open(input_file, "r") as file:
        # skip headers
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if len(row) == 2:
                input_files[row[0]] = row[1]
    return input_files

def main():
    parser = argparse.ArgumentParser(
        description="Performs the Savitzky Golay interpolation of the BI values in received file"
    )
    parser.add_argument("-i", "--input", help="File containing all merged extracted features files for each crop", required=True)
    parser.add_argument("-o", "--output", help="Output merged file", required=True)
    
    args = parser.parse_args()
    
    input_files = read_input_files(args.input)

    output_ct_files_dict = dict()
    
    for crop_type in input_files.keys() : 
        input_file = input_files[crop_type]
        
        output_file, file_extension = os.path.splitext(args.output)
        output_file = output_file + "_" + str(crop_type) + ".csv"
        output_ct_files_dict[str(crop_type)] = output_file
        
        print("Extracting yield features for crop type {} and file {}".format(crop_type, input_file))
        
        command = []
        command += ["extract_yield_features.py", 
                    "--input", input_file, 
                    "--output", output_file]

        run_command(command)
    
        with open(args.output, 'w') as out_sg:  
            writer = csv.writer(out_sg)
            writer.writerow(["crop_type", "features_file"])
            for key, value in output_ct_files_dict.items():
                writer.writerow([key, value])

    
if __name__ == "__main__":
    main()
