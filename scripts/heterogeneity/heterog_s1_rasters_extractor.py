#!/usr/bin/env python
from __future__ import print_function

import argparse
import os
import os.path
import glob

def main():
    parser = argparse.ArgumentParser(description="CropType S1 rasters list extraction")
    parser.add_argument("-i", "--input-dir", default=".", help="Input directory containing S1 rasters")
    parser.add_argument("-t", "--tile", help="S2 tile")
    parser.add_argument("-s", "--filter-str", help="substring to match in raster name")
    parser.add_argument("-o", "--out", help="Output file containing the raster paths")
    args = parser.parse_args()

    file_pattern = os.path.join(args.input_dir, "SEN4CAP_L2A_PRD_S*_W*_T{}_{}.tif".format(args.tile, args.filter_str))
    
    rasters = glob.glob(file_pattern)
    
    rasters.sort()
    
    with open(args.out, 'w') as f:
        for raster in rasters:
            f.write("%s\n" % raster)
        
if __name__ == "__main__":
    main()
        