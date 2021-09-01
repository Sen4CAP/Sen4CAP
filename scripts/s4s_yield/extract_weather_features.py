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
import osgeo
from osgeo import osr, gdal, ogr
from gdal import gdalconst
import re
import sys
import csv
import errno
import ntpath
import subprocess

from functools import partial
from multiprocessing import Pool,cpu_count


NETCDF_WEATHER_BANDS = ["evap", "prec", "tmax", "tmin", "tmean", "swvl1", "swvl2", "swvl3", "swvl4", "rad"]
WRITE_BUF_SIZE = 1000
ID_COL_NAME = "NewID"
VEC_ID_COL_NAME = "parcel_id"

GRID_NO_COL_NAME = "GridNo"
CT_COL_NAME = "crop_code"

class GridDescr(object):
    def __init__(self, grid_no, geom, value):
        self.grid_no = grid_no
        self.geom = geom
        # self.value = value

class CmdArgs(object): 
    def __init__(self, feature, input, output, parcels_to_grid_map):
        self.feature = feature
        self.input = input
        self.output = output
        self.parcels_to_grid_map = parcels_to_grid_map

def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)


def create_polygon(geotransform, xOffset,yOffset) :
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    coordX = originX+pixelWidth*xOffset
    coordY = originY+pixelHeight*yOffset
    
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint_2D(coordX, coordY)
    ring.AddPoint_2D(coordX+pixelWidth, coordY)
    ring.AddPoint_2D(coordX+pixelWidth, coordY+pixelHeight)
    ring.AddPoint_2D(coordX, coordY+pixelHeight)
    ring.AddPoint_2D(coordX, coordY)

    # Create polygon
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    
    return poly.ExportToWkt()

def extract_parcel_to_grid_mapping(input, feature, vec) :
    # open the weather file and get information from it
    src_ds = gdal.Open("NETCDF:" + input + ":" + feature)
    geotransform = src_ds.GetGeoTransform()
    array = src_ds.ReadAsArray()    

    # open the vector file and get infos from it
    # driver_vec = ogr.GetDriverByName("ESRI Shapefile")
    driver_vec = ogr.GetDriverByName("GPKG")
    dataSource = driver_vec.Open(vec, 0)
    layer_vec = dataSource.GetLayer()
    inSpatialRef = layer_vec.GetSpatialRef()
    sr = osr.SpatialReference(str(inSpatialRef))
    res = sr.AutoIdentifyEPSG()
    srid = sr.GetAuthorityCode(None)
    # print(srid)
    
    # Getting spatial reference of input vector
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(srid))
    # srs.ImportFromWkt(inSpatialRef.ExportToWkt())

    # WGS84 projection reference
    OSR_WGS84_REF = osr.SpatialReference()
    OSR_WGS84_REF.ImportFromEPSG(4326)
    if int(osgeo.__version__[0]) >= 3:
        # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
        OSR_WGS84_REF.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)    

    # OSR transformation
    wgs84_to_image_trasformation = osr.CoordinateTransformation(srs, OSR_WGS84_REF)

    grid_no = 0
    grid_descrs = []
    parcels_to_grid = []
    for ridx, row in enumerate(array):
        for cidx, value in enumerate(row):
            # Xcoord, Ycoord = pixelOffset2coord(geotransform,cidx,ridx)
            # print ("grid_no = {}:  XCoord = {}, Ycoord = {}".format(grid_no, Xcoord, Ycoord))
            poly = create_polygon(geotransform, cidx,ridx)
            # print("grid_no = {}:  poly = {}".format(grid_no, poly))
            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkt(poly)
            grid_descrs.append(GridDescr(grid_no, geom, value))
            grid_no += 1

    for feature in layer_vec:
        geom_vec = feature.GetGeometryRef()
        if geom_vec is None :
            continue
        geom_vec.Transform(wgs84_to_image_trasformation)
        
        parcel_id = feature.GetField(VEC_ID_COL_NAME)
        parcel_ct = feature.GetField(CT_COL_NAME)
        for grid_descr in grid_descrs:
            if geom_vec.Intersects(grid_descr.geom):
                print("Parcel found with NewID = {} intersecting grid no {} => Extracting value {} ...".format(parcel_id, grid_descr.grid_no, parcel_ct))
                parcels_to_grid.append([parcel_id, grid_descr.grid_no, parcel_ct])
    # sort by parcel id
    parcels_to_grid.sort()
    
    return parcels_to_grid  
        
def get_out_file_name(netcdf_input) :
    filename = ntpath.basename(netcdf_input)
    filename_base = os.path.splitext(filename)[0]
    filename_base = filename_base.replace(".", "_")
    filename_base = filename_base.replace("-", "")
    
    return filename_base
        
def extract_parcels_weather_data(exec_info):
    feature = exec_info.feature
    input = exec_info.input
    parcels_to_grid_map = exec_info.parcels_to_grid_map
    output = exec_info.output
    
    print("Processing file {} for feature {} into {}".format(input, feature, output))
    src_ds = gdal.Open("NETCDF:" + input + ":" + feature)
    array = src_ds.ReadAsArray()
    
   # extract the grid values. We could have use also np.array(src_ds.ReadAsArray()).flatten() but is better to be consistent
    grid_values = []
    for ridx, row in enumerate(array):
        for cidx, value in enumerate(row):
            grid_values.append(value)
    
    grid_vals_len = len(grid_values)
    write_buffer = []
    header = [ID_COL_NAME]
    
    # get the date from the file name 
    filename = ntpath.basename(output)
    filename_base = os.path.splitext(filename)[0]
    result = re.search('weather_(.*)_' + feature, filename_base)
    header += [result.group(1) + "_" + feature]
        
    with open(output, "w") as out_file:
        writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        
        # now iterate all parcels
        for parcel_info in parcels_to_grid_map:
            grid_no = parcel_info[1]
            if len(parcel_info) == 3 and grid_no < grid_vals_len :
                write_buffer.append([parcel_info[0], grid_values[grid_no]])
                
            if len(write_buffer) > WRITE_BUF_SIZE:
                writer.writerows(write_buffer)
                write_buffer = []
                
        # write the last remaining unwritten rows        
        if len(write_buffer) > 0:
            writer.writerows(write_buffer)
            
def extract_parcels_weather_data_async(inputs, working_dir, parcels_to_grid_map):
    exec_inputs = []
    for input in inputs:
        # translate the filename
        filename_base = get_out_file_name(input)

        # first extract the netcdf subdatasets as tif 
        for feature in NETCDF_WEATHER_BANDS:
            newfilename = filename_base
            newfilename += "_"
            newfilename += feature
            out_path_tmp = os.path.join(working_dir, newfilename + ".csv")

            exec_inputs.append(CmdArgs(feature, input, out_path_tmp, parcels_to_grid_map))

    p = Pool(cpu_count())
    exec_results = p.map(partial(extract_parcels_weather_data), exec_inputs)
    p.close() 
    
def write_parcels_to_grid_infos(out_file, parcels_to_grid) :
    if out_file is not None:
        print("Writing parcels to grid file {} ...".format(out_file))
        parcels_file = open(out_file, "w")
        parcels_writer = csv.writer(parcels_file, quoting=csv.QUOTE_MINIMAL)
        parcels_header = [ID_COL_NAME, GRID_NO_COL_NAME, CT_COL_NAME]
        parcels_writer.writerow(parcels_header)
        parcels_writer.writerows(parcels_to_grid)

def write_grid_to_parcels_infos(out_file, parcels_to_grid) :
    if out_file is not None:
        parcels_file = open(out_file, "w")
        parcels_writer = csv.writer(parcels_file, quoting=csv.QUOTE_MINIMAL)
        parcels_header = [GRID_NO_COL_NAME, ID_COL_NAME, CT_COL_NAME]
        grid_to_parcels = []
        print("Extracting grid to parcels ...")
        for item in parcels_to_grid:
            grid_to_parcels.append([item[1], item[0], item[2]])
        
        print("Sorting grid to parcels ...")
        grid_to_parcels.sort()
        
        print("Writing grid to parcels file {} ...".format(out_file))
        parcels_writer.writerow(parcels_header)
        parcels_writer.writerows(grid_to_parcels)

def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-i", "--input-list", nargs="+", help="List of netcdf files containing weather data", required=True)
    parser.add_argument("-v", "--vec", help="Shapefile containing the parcels", required=True)
    parser.add_argument("-o", "--out-dir", help="Out directory where feature files are stored", required=True)
    parser.add_argument("-p", "--out-parcels-to-grid-file", help="File containing the mapping between parcel id and the grid number")
    parser.add_argument("-g", "--out-grid-to-parcels-file", help="File containing the mapping between grid number and the parcel id")
    
    args = parser.parse_args()
    
    # extract the mapping parcel_id - grid_no - crop type
    parcels_to_grid = extract_parcel_to_grid_mapping(args.input_list[0], NETCDF_WEATHER_BANDS[0], args.vec)
    
    # extract the weather data for all inputs
    extract_parcels_weather_data_async(args.input_list, args.out_dir, parcels_to_grid)
    
    # write the parcels-grid mapping
    write_parcels_to_grid_infos(args.out_parcels_to_grid_file, parcels_to_grid)
    
    # write the grid-parcels mapping
    write_grid_to_parcels_infos(args.out_grid_to_parcels_file, parcels_to_grid)
    
if __name__ == "__main__":
    main()
