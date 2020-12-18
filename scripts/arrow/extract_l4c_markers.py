#!/usr/bin/env python

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
EXPORT_TO_IPC_SCRIPT="run_csv_to_ipc.sh"
 
class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        self.input_products = args.products
        self.output = args.output
        self.add_no_data_rows = args.add_no_data_rows
 
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
    
     
def export_mdb_csv_practice_file(config, vect_data_path, practiceFile, prd_date) :
    print("Exporting practice into mdb csv format : {}".format(practiceFile))
    # TODO: This is not needed for now in the header 
    # Sen4CAP_L4C_CatchCrop_CZE_2019_CSV.csv
    # z = re.match("Sen4CAP_L4C_(CatchCrop|NFC|Fallow|NA)_.*_CSV.csv", practiceFile)
    # practiceType = ()
    # if z :
    #     practiceType = z.groups(1)[0]
    #     print("Practice type is {}".format(practiceType))
    # else :
    #     print("Ignoring practice file {} as it does not match the expected naming".format(practiceFile))
    #     return prd_mdb_csv_files

    fileNameNoExt = os.path.splitext(practiceFile)[0]
    new_file_name = fileNameNoExt + "_MDB.csv"
    new_file_path = os.path.join(vect_data_path, new_file_name)
    
    with open(os.path.join(vect_data_path, practiceFile)) as f:
        reader = csv.reader(f, delimiter=';')
        with open(new_file_path,"wb") as result:
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
                    # print ("The new header is {}".format(new_header))
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
                    if config.add_no_data_rows == 1 or ignore_row == False :
                        wtr.writerow(new_row)
                i = i + 1
    return new_file_path

def export_input_prd_mdb_csv_files(config, path) :
    prd_mdb_csv_files = []
    prdDirName = os.path.basename(path)
    # S2AGRI_S4C_L4C_PRD_S53_20200825T151214_V20190201T000000_20190710T235959
    prdLastDate = prdDirName.split("_")[-1]
    date_str = prdLastDate.split('T')[0]
    if len(date_str) != 8 : 
        print ("Ignoring product path {} as seems to be in an unknown product name".format(path))
        return prd_mdb_csv_files
    
    full_path = os.path.join(path, "VECTOR_DATA")
    if os.path.isdir(full_path):
        print("Using product dir : {}".format(full_path))
    else :
        print("Ignoring non existing dir : {}".format(full_path))
        return prd_mdb_csv_files

    practicesFiles = [f for f in os.listdir(full_path) if (os.path.isfile(os.path.join(full_path, f)) and f.endswith("_CSV.csv"))]        
    for practiceFile in practicesFiles:
        new_path = export_mdb_csv_practice_file(config, full_path, practiceFile, date_str)
        prd_mdb_csv_files.append(new_path)
        
    return prd_mdb_csv_files

def export_all_mdb_csv_files(config) :
    all_mdb_csv_files = []
    for path in config.input_products:
        all_mdb_csv_files += export_input_prd_mdb_csv_files(config, path)
    return all_mdb_csv_files
    
def merge_all_mdb_csv_files(config, all_mdb_csv_files) :
    out_merged_file = config.output + ".csv"
    command = []
    command += ["otbcli", "Markers1CsvMerge", "-out", out_merged_file,  "-il"]
    command += all_mdb_csv_files
    run_command(command)
    return out_merged_file

def export_to_ipc_file(config, csv_merged_file) :
    command = [EXPORT_TO_IPC_SCRIPT, "--in", csv_merged_file, "--out", config.output, "--int8-columns", "\d{8}_M[1-5]$" ]
    run_command(command)

def cleanup_mdb_csv_files(mdb_csv_files, merged_file) :
    for mdbcsv_file in all_mdb_csv_files:
        if os.path.isfile(mdbcsv_file): 
            os.remove(mdbcsv_file)
    if os.path.isfile(merged_file): 
        os.remove(merged_file)
    

def main():
    parser = argparse.ArgumentParser(description="Create a new markers DB ipc file from a list of L4C products.")
    parser.add_argument('products', nargs='+', action='store')
    parser.add_argument('-o', '--output', help="The output IPC file")
    parser.add_argument('-g', '--add-no-data-rows', required=False, type=int, default=1, help="Add also rows having all values invalid")

    args = parser.parse_args()
    
    config = Config(args)

    outDir = os.path.dirname(config.output)
    if outDir == '' :
        outDir = "."
    print ("Output dir = {}".format(outDir))
    if not os.path.exists(outDir):
        try:
            os.makedirs(outDir)
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise        

    # export all products practices files into mdb csv format
    all_mdb_csv_files = export_all_mdb_csv_files(config)
    if len(all_mdb_csv_files) == 0:
        print("No mdb csv files exported. Stopping ...")
        exit(1)
    # merge all mdb csv files
    merged_file = merge_all_mdb_csv_files(config, all_mdb_csv_files)
    if not os.path.exists(merged_file) : 
        print("Merged file could not be created. Stopping ...")
        exit(2)
    # finally, export as IPC file
    export_to_ipc_file(config, merged_file)
    if not os.path.exists(config.output) : 
        print("Final IPC file could not be created. Stopping ...")
        exit(3)
    
    # perform cleanup - delete created mdb and merged csv files
    #cleanup_mdb_csv_files(all_mdb_csv_files, merged_file)
    
if __name__ == '__main__':
    main()
    
    
    
    
    