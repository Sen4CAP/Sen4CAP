#!/usr/bin/env python

import argparse
import numpy as np
import pandas as pd
import sys

class Config(object):
    def __init__(self, args):
        self.input_files = args.input_files
        self.output = args.output

def merge_tiles(config) : 
    dt_out = []
    file_idx = 0
    for tile_file in config.input_files:
        with open(tile_file) as f:
            line_idx = 0
            main_marker = ""
            main_marker_idx = -1
            header = []
            for line in f:
                line_split = line.rstrip().split(',')
                if line_idx == 0:   # skip header line
                    try:
                        main_marker_idx = line_split.index("M1")
                        main_marker = "M1"
                    except ValueError:
                        try:
                            main_marker_idx = line_split.index("M5")
                            main_marker = "M5"
                        except ValueError:
                            print("M1 or M5 were not found in file {}. It will be ignored ...".format(tile_file))
                            break      
                    header = line_split 
                else :
                    if file_idx == 0 :
                        dt_out.append(dict(zip(header, line_split)))
                    else :
                        item = dt_out.loc[dt_out['NewID'] == line_split[0]]

                        # In ATBD, if the main marker is 0, the others are also 0, so we check if
                        # are only differences in this marker. If this marker is 1 and there are 
                        # differences in other markers, we don't take them into account
                        if line_split[main_marker_idx] == "1.0" :
                            if  len(item[main_marker].values) == 0 or item[main_marker].values[0] == "0.0" or item[main_marker].values[0] == "0" :
                                my_dict = dict(zip(header, line_split))
                                for key in my_dict.keys():
                                    dt_out.loc[item.index, key] = my_dict.get(key)

                line_idx = line_idx+1
            if main_marker_idx >= 0: 
                if file_idx == 0 :
                    dt_out = pd.DataFrame(dt_out)

                file_idx = file_idx+1   

    dt_out.to_csv(config.output,index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Heterogeneity period analysis"
    )
    parser.add_argument('-f', '--input-files', nargs='+', help="The list of tiles analysis CSV files")   
    parser.add_argument("-o", "--output", help="Output CSV file", required=True)

    args = parser.parse_args()
    
    config = Config(args)

    merge_tiles(config)
    
if __name__ == "__main__":
    main()
