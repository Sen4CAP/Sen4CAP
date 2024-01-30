#!/usr/bin/env python

import argparse
import numpy as np
import pandas as pd
import sys
import time

class Config(object):
    def __init__(self, args):
        self.input_files = args.input_files
        self.output = args.output

def merge_tiles(config) : 
    newid_idx = -1
    main_marker_idx = -1
    header = []
    dict_parcels = dict()
    for tile_file in config.input_files:
        with open(tile_file) as f:
            line_idx = 0
            time1 = time.time()
            for line in f:
                line_split = line.rstrip().split(',')
                if line_idx == 0:   # skip header line
                    # if the indexes were not initialized, do it once as we assume all files are of same type 
                    # (we cannot combine S1 and S2) and created with the same order of columns
                    if newid_idx == -1:
                        try:
                            # Distinguish between S2 (contains M1) and S1 (contains M5) markers 
                            main_marker_idx = line_split.index("M1")
                        except ValueError:
                            try:
                                main_marker_idx = line_split.index("M5")
                            except ValueError:
                                print("M1 or M5 were not found in file {}. It will be skipped ...".format(tile_file))
                                break   
                        
                        header = line_split 
                        # keep the new_id the last check 
                        try:
                            newid_idx = line_split.index("NewID")
                        except ValueError:
                            print("ERROR: NewID cannot be found in the header of file {}. It will be skipped ...".format(tile_file))
                            break
                else :
                    new_id = int(line_split[newid_idx])
                    if not new_id in dict_parcels:    
                        dict_parcels[new_id] = line_split
                    else :
                        item = dict_parcels[new_id]

                        # In ATBD, if the main marker is 0, the others are also 0, so we check if
                        # are only differences in this marker. If this marker is 1 and there are 
                        # differences in other markers, we don't take them into account
                        main_marker_val1 = int(line_split[main_marker_idx])
                        main_marker_val2 = int(item[main_marker_idx])
                        # overwrite the dictionary entry if the case
                        if main_marker_val1 == 1 and main_marker_val2 == 0 :
                            dict_parcels[new_id] = line_split
                line_idx = line_idx+1
            time2 = time.time()    
            print("Execution for file {} took: {}".format(tile_file, time2-time1))

    dt_out = []          
    for new_id in sorted(dict_parcels.keys()):
        dt_out.append(dict(zip(header, dict_parcels[new_id])))
    dt_out = pd.DataFrame(dt_out)
    dt_out.to_csv(config.output,index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Heterogeneity period analysis"
    )
    parser.add_argument('-f', '--input-files', nargs='+', help="The list of tiles analysis CSV files")   
    parser.add_argument("-o", "--output", help="Output CSV file", required=True)

    args = parser.parse_args()
    
    config = Config(args)

    time1 = time.time()    

    merge_tiles(config)

    time2 = time.time()    
    print("Total execution took: {}".format(time2-time1))

if __name__ == "__main__":
    main()
