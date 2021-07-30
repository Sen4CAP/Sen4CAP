#!/usr/bin/env python

import argparse
import logging
from collections import defaultdict
from datetime import date
import datetime as dt
# from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
import pipes
from osgeo import osr, gdal, ogr
from gdal import gdalconst
import re
import sys
import csv
import errno
import ntpath
import subprocess
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras

from functools import partial
from multiprocessing import Pool,cpu_count


try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


NETCDF_WEATHER_BANDS = ["evap", "prec", "tmax", "tmin", "tmean", "swvl1", "swvl2", "swvl3", "swvl4", "rad"]

class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        self.site_id = args.site_id
        
class Tile(object):
    def __init__(self, tile_id, epsg_code, tile_extent, xmin, ymin, xmax, ymax):
        self.tile_id = tile_id
        self.epsg_code = epsg_code
        self.tile_extent = tile_extent
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

class CmdArgs(object): 
    def __init__(self, feature, input, output, tile = ""):
        self.feature = feature
        self.input = input
        self.output = output
        self.tile = tile

class FeatExtrArgs() :
    def __init__(self, feature, input, vect, out_dir):
        self.feature = feature
        self.input = input
        self.vect = vect
        self.out_dir = out_dir
        
def get_site_tiles(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
            with Transformed AS (
                select shape_tiles_s2.tile_id as tile_id, shape_tiles_s2.epsg_code as epsg_code,
                        ST_AsBinary(ST_SnapToGrid(ST_Transform(shape_tiles_s2.geom, shape_tiles_s2.epsg_code), 1)) as tile_extent
                    from sp_get_site_tiles(%s :: smallint, 1 :: smallint) site_tiles
                    inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id
                ) 
                select tile_id, epsg_code, tile_extent, st_x(st_pointn(st_exteriorring(tile_extent), 4))::int as xMin, st_y(st_pointn(st_exteriorring(tile_extent), 4))::int as yMin, 
                st_x(st_pointn(st_exteriorring(tile_extent), 2))::int as xMax, st_y(st_pointn(st_exteriorring(tile_extent), 2))::int as yMax 
                from Transformed;"""
        )
        logging.debug(query.as_string(conn))
        cursor.execute(query, (site_id,))

        rows = cursor.fetchall()
        conn.commit()

        result = []
        for (tile_id, epsg_code, tile_extent, xmin, ymin, xmax, ymax) in rows:
            tile_extent = ogr.CreateGeometryFromWkb(bytes(tile_extent))
            result.append(Tile(tile_id, epsg_code, tile_extent, xmin, ymin, xmax, ymax))

        return result

def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)

def get_out_file_name(netcdf_input) :
    filename = ntpath.basename(netcdf_input)
    filename_base = os.path.splitext(filename)[0]
    filename_base = filename_base.replace(".", "_")
    filename_base = filename_base.replace("-", "")
    
    return filename_base
    

# TODO: We should try removing  this step and instead do the bands extraction in the cut_tif_to_s2_tiles
def convert_to_tiffs(inputs, vec, working_dir):

    
    ret_dict = {}
    for feature in NETCDF_WEATHER_BANDS:
        ret_dict[feature] = []
        
    for input in inputs:
        # translate the filename
        filename_base = get_out_file_name(input)
        
        # first extract the netcdf subdatasets as tif 
        for feature in NETCDF_WEATHER_BANDS:
            newfilename = filename_base
            newfilename += "_"
            newfilename += feature
            out_path_tmp = os.path.join(working_dir, newfilename + ".tif")
        
            command = []
            command += ["gdal_translate"]
            command += ["-a_nodata", "-10000"]
            command += ["-a_srs", "+proj=longlat +datum=WGS84 +no_defs"]
            command += ["NETCDF:\"" + input + "\":" + feature]
            command += [out_path_tmp]
            run_command(command)

            print("Added path {} to dict feature {} ...".format(out_path_tmp, feature))
            ret_dict[feature].append(out_path_tmp)
    
    return ret_dict

def convert_netcdf_to_tiff(feature, input, output):
   
    command = []
    command += ["gdal_translate"]
    command += ["-a_nodata", "-10000"]
    command += ["-a_srs", "+proj=longlat +datum=WGS84 +no_defs"]
    command += ["NETCDF:\"" + input + "\":" + feature]
    command += [output]
    # run_command(command)
    
    return (feature, output)

def run_convert_netcdf_to_tiff(args):
    return convert_netcdf_to_tiff(args.feature, args.input, args.output)
    
def convert_to_tiffs_async(inputs, working_dir):
    exec_inputs = []
    for input in inputs:
        # translate the filename
        filename_base = get_out_file_name(input)
        
        # first extract the netcdf subdatasets as tif 
        for feature in NETCDF_WEATHER_BANDS:
            newfilename = filename_base
            newfilename += "_"
            newfilename += feature
            out_path_tmp = os.path.join(working_dir, newfilename + ".tif")
            
            exec_inputs.append(CmdArgs(feature, input, out_path_tmp))

    p = Pool(cpu_count())
    exec_results = p.map(partial(run_convert_netcdf_to_tiff), exec_inputs)
    p.close() 

    ret_dict = {}
    for feature in NETCDF_WEATHER_BANDS:
        ret_dict[feature] = []
        
    for feature, out_path in exec_results:
        print("Added path {} to dict feature {} ...".format(out_path, feature))
        ret_dict[feature].append(out_path)
    
    return ret_dict

def run_cut_tif_to_s2_tiles(args) :
    return cut_tif_to_s2_tiles(args.feature, args.input, args.output, args.tile)

def cut_tif_to_s2_tiles(feature, input, output, tile) :
    command = []
    command += ["/usr/local/bin/gdalwarp"]
    command += ["-s_srs", "EPSG:4326"]
    command += ["-t_srs", "EPSG:{}".format(tile.epsg_code)]
    command += ["-te", tile.xmin, tile.ymin, tile.xmax, tile.ymax]
    command += ["-te_srs", "EPSG:{}".format(tile.epsg_code)]
    command += ["-tr", 10, 10]

    # command += ["-srcnodata"]
    # command += ["-32767"]
    # command += ["-dstnodata"]
    # command += ["-10000"]

    command += ["-crop_to_cutline"]
    command += ["-overwrite"]
    command += ["-multi"]
    command += ["-r", "near"]
    command += ["-co"]
    command += ["COMPRESS=DEFLATE"]
    # command += ["-co"]
    # command += ["PREDICTOR=3"]
    
    command += [input]
    command += [output]
    
    # run_command(command)
    
    return (feature, output)
    
def cut_to_s2_tiles(dict, tiles) :
    ret_dict = {}
    for feature in NETCDF_WEATHER_BANDS:
        ret_dict[feature] = []

    for feature in dict:
        for file in dict[feature] :
            parent_dir = os.path.dirname(file)
            filename = ntpath.basename(file)
            filename_base = os.path.splitext(filename)[0]        
            for tile in tiles:
                out_file_name = filename_base
                out_file_name += "_"
                out_file_name += tile.tile_id
                out_path_tmp = os.path.join(parent_dir, out_file_name + ".tif")
                
                ret_dict[feature].append(out_path_tmp)
                
                cut_tif_to_s2_tiles(feature, file, out_path_tmp, tile)
    
    return ret_dict            

def cut_to_s2_tiles_async(dict, tiles) :

    exec_inputs = []
    for feature in dict:
        for file in dict[feature] :
            parent_dir = os.path.dirname(file)
            filename = ntpath.basename(file)
            filename_base = os.path.splitext(filename)[0]        
            for tile in tiles:
                out_file_name = filename_base
                out_file_name += "_"
                out_file_name += tile.tile_id
                out_path_tmp = os.path.join(parent_dir, out_file_name + ".tif")
                
                exec_inputs.append(CmdArgs(feature, file, out_path_tmp, tile))

    p = Pool(cpu_count())
    exec_results = p.map(partial(run_cut_tif_to_s2_tiles), exec_inputs)
    p.close() 

    ret_dict = {}
    for feature in NETCDF_WEATHER_BANDS:
        ret_dict[feature] = []
        
    for (feature, out_path) in exec_results:
        print("Added path {} to dict feature {} ...".format(out_path, feature))
        ret_dict[feature].append(out_path)
        
    return ret_dict            
           
def extract_feature_values(feature, file, shapefile, out_dir):
    
    command = []
    command += ["otbcli", "Markers1Extractor"]
    command += ["-field", "NewID"]
    command += ["-prdtype", feature]
    command += ["-outdir", out_dir]
    command += ["-il", file]
    command += ["-vec", shapefile]
    command += ["-stdev", 0]

    run_command(command)
    
def extract_feature_values_async(args):
    extract_feature_values(args.feature, args.input, args.vect, args.out_dir)
    
def run_features_extraction(dict, vect, out_dir):
    exec_inputs = []
    for feature in dict:
        for file in dict[feature] : 
            exec_inputs.append(FeatExtrArgs(feature, file, vect, out_dir))

    p = Pool(cpu_count())
    p.map(partial(extract_feature_values_async), exec_inputs)
    p.close() 
    
    
def merge_feature_values(inputs, out_file) :
    command = []
    command += ["otbcli", "Markers1CsvMerge"]
    command += ["-out", out_file]
    command += ["-il"]
    command.append(inputs)

    run_command(command)

def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-c", "--config-file", default="/etc/sen2agri/sen2agri.conf", help="configuration file location")

    parser.add_argument("-s", "--site-id", type=int, required=True, help="site ID to filter by")
    parser.add_argument("-i", "--input-list", nargs="+", help="List of netcdf files containing weather data", required=True)
    parser.add_argument("-v", "--vec", help="Shapefile containing the parcels", required=True)
    parser.add_argument("-w", "--working-dir", help="Working directory", required=True)
    parser.add_argument("-o", "--output", help="Output file containing interpolated values", required=True)
    
    args = parser.parse_args()
    
    config = Config(args)
    
    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        print("Retrieving site tiles")
        tiles = get_site_tiles(conn, config.site_id)
        
        # extract the netcdf subdatasets as tif 
        ret_dict = convert_to_tiffs_async(args.input_list, args.working_dir)

        # cut the extracted subdatasets to S2 tiles
        ret_dict = cut_to_s2_tiles_async(ret_dict, tiles)
        
        feature_files_dir = os.path.join(args.working_dir, "feature_files")
        if not os.path.exists(feature_files_dir):
            os.makedirs(feature_files_dir)

        run_features_extraction(ret_dict, args.vec, feature_files_dir)        
        
        merge_feature_values(feature_files_dir, args.output)
    
if __name__ == "__main__":
    main()
