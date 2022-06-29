#!/usr/bin/env python3
from __future__ import print_function

import argparse
import os
import os.path
import glob
import pipes
import subprocess
from shutil import copyfile
import re
import datetime
import shutil
import docker
import sys

S4X_PRD_PATTERN = "(S2AGRI|SEN4CAP)_(.*)_S\d+_\d{8}T\d{6}_(V|A)(\d{8})((T\d{6})|(_(\d{8})))"

# product regex dictionary to tupple of group indexes for (product type and product date)
S1_PRD_PATTERNS_DICT = {
                    "S1[A-D]_L2_(COH|BCK)_(\d{8})T\d{6}(_(\d{8})T\d{6})?_(VV|VH)_\d{3}_.*\.tif" : (1, 2),
                    "SEN4CAP_L2A_S\d+_V\d{8}T\d{6}_(\d{8})T\d{6}_(VV|VH)_\d{3}_(AMP|COHE)\.tif" : (3, 1)
                  }

PRD_TYPE_RASTERS_DICT = {
                    "L3A" : ("S2AGRI_L3A_SRFL_V\d{8}_\d{8}_T.*_\d{2}M\.TIF", "S2AGRI_L3A_MFLG_V\d{8}_\d{8}_T.*_\d{2}M\.TIF"),
                    "L3B" : ("S2AGRI_L3B_S.*\.TIF", "S2AGRI_L3B_MMONODFLG_.*\.TIF"), 
                    "L4A" : ("S2AGRI_L4A_CM_V\d{8}_\d{8}_T.*\.TIF", "S2AGRI_L4A_MCMFLG_V\d{8}_\d{8}_T.*\.TIF"), 
                    "L4B" : ("S2AGRI_L4A_CT_V\d{8}_\d{8}_T.*\.TIF", "S2AGRI_L4A_MCTFLG_V\d{8}_\d{8}_T.*\.TIF")
                   }

class Config(object):
    def __init__(self, args):
        self.product_path = args.product_path
        self.force_recreate = args.force_recreate
        self.product_name = os.path.basename(os.path.normpath(self.product_path))
        self.product_type = ""
        self.isSen4XPrd = True
        
        if os.path.isfile(self.product_path) :
            for s1_prd_pattern in S1_PRD_PATTERNS_DICT:
                prd_pattern_re = re.compile(s1_prd_pattern)
                m = prd_pattern_re.search(self.product_path)
                if m:
                    self.product_type = m.group(S1_PRD_PATTERNS_DICT[s1_prd_pattern][0])
                    print("Extracted Product Type = {}".format(self.product_type))
                    self.prd_date = m.group(S1_PRD_PATTERNS_DICT[s1_prd_pattern][1])
                    self.prd_date = datetime.datetime.strptime(self.prd_date, "%Y%m%d").strftime("%Y-%m-%dT00:00:00")
                    self.isSen4XPrd = False
            
            if not self.product_type: 
                print("Cannot determine product type from product in file {}".format(self.product_path))
                sys.exit(1)
        else :
            # Get the product type from the product name if not provided
            prd_pattern_re = re.compile(S4X_PRD_PATTERN)
            m = prd_pattern_re.search(self.product_path)
            if m:
                self.product_type = m.group(2)
                if self.product_type.endswith("_PRD"):
                    self.product_type = self.product_type[: -len("_PRD")]
                print("Extracted Product Type = {}".format(self.product_type))
                isInterval = m.group(2) == 'V'
                if isInterval :
                    self.prd_date = m.group(8)
                else:
                    self.prd_date = m.group(4)
                self.prd_date = datetime.datetime.strptime(self.prd_date, "%Y%m%d").strftime("%Y-%m-%dT00:00:00")
                
            if not self.product_type :
                print("Cannot determine product type from product in directory {}".format(self.product_path))
                sys.exit(1)
            
    
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)

def get_files_from_dir(in_dir, regex_patter):
    ret_files = []
    regex = re.compile(regex_patter)
    for root, dirs, files in os.walk(in_dir):
        for file in files:
            if regex.match(file):
                file_path = os.path.join(in_dir, file)
                band_name = file
                if band_name.endswith(".TIF"):
                    band_name = band_name[: -len(".TIF")]
                ret_files += [(file_path, band_name)]
    
    return ret_files

def compute_zarr_path(in_path, zarr_dir) :
    # the zarr file name is the name of the input file or directory, 
    zarr_name = os.path.splitext(os.path.basename(os.path.normpath(in_path)))[0]
    return os.path.join(zarr_dir, "{}.zarr".format(zarr_name))

def create_raster_zarr(tile_dir, input_raster, out_zarr, band_name, prd_date) :
    # print ("Tile_Dir = {}".format(tile_dir))
    # print ("input_raster = {}".format(input_raster))
    # print ("out_zarr = {}".format(out_zarr))
    # print ("band_name = {}".format(band_name))
    # print ("prd_date = {}".format(prd_date))
    client = docker.from_env()
    volumes = {
        input_raster: {"bind": input_raster, "mode": "ro"},
        tile_dir: {"bind": tile_dir, "mode": "rw"},
    }
    command = []
    command += ["gdal_to_xarray.py"]
    command += ["--input", input_raster]
    command += ["--band-names", band_name]
    command += ["--out", out_zarr]
    command += ["--date", prd_date]
    command += ["--scale", "1000"]
    command += ["--fill", "1000"]
    container = client.containers.run(
        image="sen4x/gdal_to_xarray:0.1",
        remove=True,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        command=command,
    )
    client.close()

def create_union_zarr(input_zarrs, out_zarr, zarr_subdir, band_name) :
    client = docker.from_env()
    volumes = {
        zarr_subdir: {"bind": zarr_subdir, "mode": "rw"},
    }
    command = []
    command += ["python", "/usr/bin/union_zarrs.py"]
    command += ["--inputs"]
    command += input_zarrs
    command += ["--out", out_zarr]
    command += ["--band-name", band_name]
    container = client.containers.run(
        image="sen4x/gdal_to_xarray:0.1",
        remove=True,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        command=command,
    )
    client.close()

# def create_raster_zarr(tile_dir, input_raster, out_zarr, band_name, prd_date) :
#     command = []
#     command += ["docker", "run", "--rm"]
#     command += ["-v", "{}:{}".format(tile_dir, tile_dir) ]
#     command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
#     command += ["sen4x/gdal_to_xarray:0.1"]
#     command += ["gdal_to_xarray.py"]
#     command += ["--input", input_raster]
#     command += ["--band-names", band_name]
#     command += ["--out", out_zarr]
#     command += ["--date", prd_date]
#     command += ["--scale", 1000]
#     command += ["--fill", 1000]
#     
#     run_command(command)
# 
# def create_union_zarr(input_zarrs, out_zarr, zarr_subdir, band_name) :
#     
#     command = []
#     command += ["docker", "run", "--rm"]
#     command += ["-v", "{}:{}".format(zarr_subdir, zarr_subdir) ]
#     command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
#     command += ["sen4x/gdal_to_xarray:0.1"]
#     command += ["python", "/usr/bin/union_zarrs.py"]
#     command += ["--inputs"]
#     command += input_zarrs
#     command += ["--out", out_zarr]
#     command += ["--band-name", band_name]
#     
#     run_command(command)

def convert_rasters_to_zarr(config, input_rasters, rasters_dir, zarr_data_dir) :
    if not os.path.exists(zarr_data_dir) :
        print ("Creating directory {} ...".format(zarr_data_dir))
        os.makedirs(zarr_data_dir)

    raster_zarrs = []
    # create individual zarrs for all eligible product rasters and masks
    for input_raster in input_rasters : 
        zarr_path = compute_zarr_path(input_raster[0], zarr_data_dir)
        if os.path.exists(zarr_path): 
            if config.force_recreate :
                print("Removing already zarr for raster {}".format(zarr_path))
                shutil.rmtree(zarr_path)
            else :
                # do nothing if exists and not force recreate
                raster_zarrs += [zarr_path]
                continue
        create_raster_zarr(rasters_dir, input_raster[0], zarr_path, input_raster[1], config.prd_date)
        raster_zarrs += [zarr_path]

    return raster_zarrs

def convert_tile_rasters_to_zarr(config, tile_dir) :
    input_rasters = []
    input_rasters += get_files_from_dir(os.path.join(tile_dir, "IMG_DATA"), PRD_TYPE_RASTERS_DICT.get(config.product_type)[0])
    input_rasters += get_files_from_dir(os.path.join(tile_dir, "QI_DATA"), PRD_TYPE_RASTERS_DICT.get(config.product_type)[1])
    # lists = list(map(list, zip(*input_rasters)))
    
    # create individual zarrs for all eligible product rasters and masks
    zarr_data_dir = os.path.join(tile_dir, "ZARR_DATA")
    raster_zarrs = convert_rasters_to_zarr(config, input_rasters, tile_dir, zarr_data_dir)
    
    # create the final zarr as union of the previour raster zarrs. 
    # The name and band name are built from the tile directory name
    
    # TODO : For now we do not use union anymore as the mode for multi band is not supported yet
    # final_zarr_path = compute_zarr_path(tile_dir, zarr_data_dir)
    # band_name = os.path.splitext(os.path.basename(os.path.normpath(tile_dir)))[0]
    # create_union_zarr(raster_zarrs, final_zarr_path, zarr_data_dir, band_name)
    
def convert_sen4x_product(config) :
    tiles_dir = os.path.join(config.product_path, "TILES")
    for subdir in os.listdir(tiles_dir):
        tile_dir = os.path.join(tiles_dir, subdir)
        if os.path.isdir(tile_dir): 
            print("Tile Dir: {}".format(tile_dir))
            convert_tile_rasters_to_zarr(config, tile_dir)

def convert_simple_raster_product(config) :
    # this applies to S1 products
    prd_dir = os.path.dirname(config.product_path)
    zarr_data_dir = os.path.join(prd_dir, "ZARR_DATA")
    input_rasters = [(config.product_path, os.path.splitext(config.product_name)[0])]
    raster_zarrs = convert_rasters_to_zarr(config, input_rasters, prd_dir, zarr_data_dir)

def main():
    parser = argparse.ArgumentParser(
        description="Handles the upload of the grassland mowing config file"
    )
    parser.add_argument(
        "-p", "--product-path", required=True, help="The product path to be converted to zarr"
    )
    parser.add_argument(
        "-f", "--force-recreate", required=False, default=False, help="Forces the recreation of the zarr if it already exists for an individual raster"
    )
    
    # parser.add_argument(
    #     "-o", "--zarr", required=False, help="The output zarr file"
    # )
    
    args = parser.parse_args()
    config = Config(args)

    if not os.path.exists(config.product_path) :
        print ("The input product {} does not exist".format(config.product_path))
        sys.exit(1)
    
    if config.isSen4XPrd:
        convert_sen4x_product(config)
    else:
        convert_simple_raster_product(config)
            
if __name__ == "__main__":
    main()
