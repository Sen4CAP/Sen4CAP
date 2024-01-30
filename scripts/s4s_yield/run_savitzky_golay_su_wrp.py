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

SG_DEFAULT_WINDOW_LEN = 21
SG_DEFAULT_MIN_INDICES = 50

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
            if len(row) == 2:
                file_path = row[1]
                if os.path.isabs(file_path):
                    input_files[row[0]] = file_path
                else:
                    input_files[row[0]] = os.path.join(input_file_dir, file_path)
    return input_files

def main():
    parser = argparse.ArgumentParser(
        description="Performs the Savitzky Golay interpolation of the BI values in received file"
    )
    parser.add_argument("-i", "--input", help="File containing the input files containig the extracted features (mean, stddev etc.) for the desired BI", required=True)
    parser.add_argument("-s", "--sg-output", help="Output file containing SG LAI interpolated values", required=True)
    parser.add_argument("-x", "--indices-output", help="Output file for crop growth indices extracted values", required=True)
    parser.add_argument("-m", "--metrics-output", help="Output file LAI metrics extracted values", required=True)
    parser.add_argument("-y", "--year", help="The processing year", required=True)
    parser.add_argument("-w", "--window-length", help="Window length", required=False, type=int, default=SG_DEFAULT_WINDOW_LEN)
    parser.add_argument("-n", "--min-indices", help="Minimum number of indices", required=False, type=int, default=SG_DEFAULT_MIN_INDICES)
    parser.add_argument("-b", "--season-start", help="Season start (format YYYY-mm-dd)", required=False)
    parser.add_argument("-e", "--season-end", help="Season end (format YYYY-mm-dd)", required=False)
    
    args = parser.parse_args()
    
    input_files = read_input_files(args.input)

    output_ct_sg_files_dict = dict()
    output_ct_indices_files_dict = dict()
    output_ct_metrics_files_dict = dict()
    
    for crop_type in input_files.keys() : 
        input_file = input_files[crop_type]
        
        sg_output, file_extension = os.path.splitext(args.sg_output)
        sg_output = sg_output + "_" + str(crop_type) + ".csv"
        output_ct_sg_files_dict[str(crop_type)] = sg_output
        
        indices_output, file_extension = os.path.splitext(args.indices_output)
        indices_output = indices_output + "_" + str(crop_type) + ".csv"
        output_ct_indices_files_dict[str(crop_type)] = indices_output

        metrics_output, file_extension = os.path.splitext(args.metrics_output)
        metrics_output = metrics_output + "_" + str(crop_type) + ".csv"
        output_ct_metrics_files_dict[str(crop_type)] = metrics_output
        
        print("Executing SG for file {}".format(input_file))
        
        command = []
        command += ["run_savitzky_golay.py"]
        command += ["--input", input_file]
        command += ["--year", args.year]
        command += ["--sg-output", sg_output]
        command += ["--indices-output", indices_output]
        command += ["--metrics-output", metrics_output]
        command += ["--window-length", args.window_length]
        command += ["--min-indices", args.min_indices]
        
        if args.season_start:
            command += ["--season-start", args.season_start]
        if args.season_end:
            command += ["--season-end", args.season_end]

        run_command(command)

    
        with open(args.sg_output, 'w') as out_sg:  
            writer = csv.writer(out_sg)
            writer.writerow(["crop_type", "sg_file"])
            for key, value in output_ct_sg_files_dict.items():
                writer.writerow([key, value])

        with open(args.indices_output, 'w') as out_indices:  
            writer = csv.writer(out_indices)
            writer.writerow(["crop_type", "sg_indices_file"])
            for key, value in output_ct_indices_files_dict.items():
                writer.writerow([key, value])

        with open(args.metrics_output, 'w') as out_metrics:  
            writer = csv.writer(out_metrics)
            writer.writerow(["crop_type", "sg_metrics_file"])
            for key, value in output_ct_metrics_files_dict.items():
                writer.writerow([key, value])

    
if __name__ == "__main__":
    main()
