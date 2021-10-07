#!/usr/bin/env python3

import argparse
import os
import errno
import csv
import re
import pipes
import subprocess

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


EXPORTED_MARKERS=["M1", "M2", "M3", "M4", "M5"]
CSV_TO_IPC_SCRIPT="csv_to_ipc.py"
IPC_TO_CSV_SCRIPT="ipc_to_csv.py"
 
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
    
def translate_values(val) :
    if val == "TRUE":
        return 1
    if val == "FALSE" :
        return 0
    if val == "NA" : 
        return -1
    if val == "NA1" : 
        return -2
    if val == "NR" : 
        return -3
    
    # unknown value
    print ("Unknown value {} found in practice file!".format(val))
    return -4
    
     
def export_mdb_csv_practice_file(practiceFile, out_file_path, prd_date, add_no_data_rows) :
    print("Exporting practice file {} into mdb csv format : {}".format(practiceFile,out_file_path))
    with open(practiceFile) as f:
        reader = csv.reader(f, delimiter=';')
        with open(out_file_path,"w") as result:
            wtr= csv.writer( result )
            i = 0;
            header_idxs = []
            new_id_col_idx = -1
            for row in reader:
                if i == 0 : 
                    # handle column line
                    # print ("First line is : {}".format(row))
                    new_header = []
                    col_idx = 0
                    for column in row:
                        # print ("Column is {}".format(column))
                        if column == "FIELD_ID" : 
                            new_header.append("NewID")
                            header_idxs.append(col_idx)
                            new_id_col_idx = col_idx
                        idx = -1
                        if column in EXPORTED_MARKERS:
                            idx = EXPORTED_MARKERS.index(column)
                        if idx != -1: 
                            new_header.append(prd_date + "_" + column) 
                            header_idxs.append(col_idx)
                        col_idx = col_idx + 1
                    print ("The new header is {}".format(new_header))
                    wtr.writerow(new_header)
                else :
                    # handle data line
                    new_row = []
                    ignore_row = True   # Ignore rows having only NA, NA1 or NR (no valid marker present on row)
                    for i in header_idxs:
                        if i == new_id_col_idx :
                            translated_val = row[i]
                        else :
                            translated_val = translate_values(row[i])
                            if translated_val == 0 or translated_val == 1 :
                                ignore_row = False
                        new_row.append(translated_val)
                    #print ("The new line to write is {}".format(new_row))
                    if add_no_data_rows == 1 or ignore_row == False :
                        wtr.writerow(new_row)
                i = i + 1
    return out_file_path

def merge_mdb_csv_files(prev_mdb_file, cur_mdb_file, out_file) :
    command = []
    command += ["otbcli", "Markers1CsvMerge", "-out", out_file,  "-il"]
    command += [prev_mdb_file, cur_mdb_file]
    run_command(command)
    return out_file

def csv_to_ipc_export(csv_merged_file, out_file) :
    command = [CSV_TO_IPC_SCRIPT, "-i", csv_merged_file, "-o", out_file, "--int32-columns", "NewID", "--int8-columns", "\d{8}_M[1-5]$" ]
    run_command(command)

def ipc_to_csv_export(ipc_file, csv_file) :
    command = [IPC_TO_CSV_SCRIPT, ipc_file, csv_file ]
    run_command(command)

def main():
    parser = argparse.ArgumentParser(description="Create a new markers DB ipc file from a list of L4C products.")
    parser.add_argument('-i', '--input', required=True, help="Input time series analysis file")
    parser.add_argument('-d', '--input-date', required=True, help="Date corresponding to input time series analysis file")
    parser.add_argument('-o', '--output', required=True, help="The output IPC file")
    parser.add_argument('-w', '--working-dir', required=False, default = ".", help="Working dir")
    parser.add_argument('-p', '--prev-mdb3-file', required=False, help="The previous IPC file, if any")
    parser.add_argument('-g', '--add-no-data-rows', required=False, type=int, default=1, help="Add also rows having all values invalid")

    args = parser.parse_args()
    
    outDir = os.path.dirname(args.output)
    if outDir == '' :
        outDir = "."
    print ("Output dir = {}".format(outDir))
    if not os.path.exists(outDir):
        try:
            os.makedirs(outDir)
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise        

    prev_mdb3_csv_file = None
    if not args.prev_mdb3_file is None: 
        # export the mdb3 file into a csv file, if needed
        if args.prev_mdb3_file.lower().endswith('.ipc') :
            prev_mdb3_file_name = os.path.basename(args.prev_mdb3_file)
            fileNameNoExt = os.path.splitext(prev_mdb3_file_name)[0]
            prev_mdb3_csv_file_name = fileNameNoExt + "_PREV_MDB3_CSV.csv"
            prev_mdb3_csv_file = os.path.join(args.working_dir, prev_mdb3_csv_file_name)
            ipc_to_csv_export(args.prev_mdb3_file, prev_mdb3_csv_file)
        elif args.prev_mdb3_file.lower().endswith('.csv') :
            prev_mdb3_csv_file = args.prev_mdb3_file
        else:
            print("Provided previous mdb3 file extension not supported : {}".format(args.prev_mdb3_file))
            
    input_file_name = os.path.basename(args.input)
    fileNameNoExt = os.path.splitext(input_file_name)[0]
    new_file_name = fileNameNoExt + "_MDB.csv"
    mdb_file_path = os.path.join(args.working_dir, new_file_name)

    export_mdb_csv_practice_file(args.input, mdb_file_path, args.input_date, args.add_no_data_rows)
    
    mdb3_csv_file = mdb_file_path
    if not prev_mdb3_csv_file is None :
        # merge the markers files
        new_file_name = fileNameNoExt + "_MDB3_Merged.csv"
        out_merged_file = os.path.join(args.working_dir, new_file_name)
        mdb3_csv_file = merge_mdb_csv_files(prev_mdb3_csv_file, mdb_file_path, out_merged_file)
        if not os.path.exists(mdb3_csv_file) : 
            print("Merged file could not be created. Stopping ...")
            exit(2)
            
    # finally, export as IPC file
    csv_to_ipc_export(mdb3_csv_file, args.output)
    if not os.path.exists(args.output) : 
        print("Final IPC file could not be created. Stopping ...")
        exit(3)
    else :
        print("Final IPC file was successfuly created into {}".format(args.output))
    
if __name__ == '__main__':
    main()
    
    
    
    
    