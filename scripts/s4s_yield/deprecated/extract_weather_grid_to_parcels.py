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
import osgeo

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
    
ID_COL_NAME = "NewID"
GRID_NO_COL_NAME = "GridNo"
    
class GridDescr(object):
    def __init__(self, grid_no, geom, value):
        self.grid_no = grid_no
        self.geom = geom
        self.value = value
        
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
    
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
    
def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-c", "--config-file", default="/etc/sen2agri/sen2agri.conf", help="configuration file location")
    parser.add_argument("-e", "--weather-features-file", help="File containing the weather extracted features", required=True)
    parser.add_argument("-v", "--vec", help="Shapefile containing the parcels", required=True)
    parser.add_argument("-g", "--out-grid-parcels-file", help="File containing the mapping between grid no and the parcel ids")
    parser.add_argument("-o", "--out-parcels-file", help="File containing the mapping between parcel id and the grid number")
    
    args = parser.parse_args()
    
    if args.out_parcels_file is None and args.out_grid_parcels_file is None:
        print ("Please provide at least one of the output files!")
        sys.exit(1)
     
    src_ds = gdal.Open(args.weather_features_file)
    # print(src_ds.GetSpatialRef())
    
    geotransform = src_ds.GetGeoTransform()
    # pixelWidth = geotransform[1]
    # print(geotransform)
    # print("RasterX = {}, RasterY = {}".format(src_ds.RasterXSize, src_ds.RasterYSize))
    
    array = src_ds.ReadAsArray()
    # print("Arr is:  {}".format(array))
    
    # /////////////////////////////////////////////////
    driver_vec = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver_vec.Open(args.vec, 0)
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

    parcels_writer = grid_parcels_writer = None
    if args.out_parcels_file is not None:
        parcels_file = open(args.out_parcels_file, "w")
        parcels_writer = csv.writer(parcels_file, quoting=csv.QUOTE_MINIMAL)
        parcels_header = [ID_COL_NAME, GRID_NO_COL_NAME]
        parcels_writer.writerow(parcels_header)
    
    if args.out_grid_parcels_file is not None:
        grid_parcels_file = open(args.out_grid_parcels_file, "w")
        grid_parcels_writer = csv.writer(grid_parcels_file, quoting=csv.QUOTE_MINIMAL)
        grid_header = [GRID_NO_COL_NAME, ID_COL_NAME]
        grid_parcels_writer.writerow(grid_header)
    
    # Get first the grid information
    grid_no = 0
    # grid_descrs = []
    
    parcels_in_grid = []
    for ridx, row in enumerate(array):
        for cidx, value in enumerate(row):
            # Xcoord, Ycoord = pixelOffset2coord(geotransform,cidx,ridx)
            # print ("grid_no = {}:  XCoord = {}, Ycoord = {}".format(grid_no, Xcoord, Ycoord))
            poly = create_polygon(geotransform, cidx,ridx)
            # print("grid_no = {}:  poly = {}".format(grid_no, poly))
            
            # Make a geometry
            geom = ogr.CreateGeometryFromWkt(poly)
            # grid_descrs.append(GridDescr(grid_no, geom, value))

            grid_no += 1
            
            grid_parcels = []
            layer_vec.ResetReading()
            for feature in layer_vec:
                parcel_id = feature.GetField("NewID")
                # print(feature.GetField("NewID"))
                geom_vec = feature.GetGeometryRef()
                if geom_vec is None :
                    continue
                geom_vec.Transform(wgs84_to_image_trasformation)
            
                # print("Geom vect : {}".format(geom_vec.ExportToWkt()))
                if geom_vec.Intersects(geom):
                    # print("Parcel found with NewID = {} intersecting grid no {} => Extracting value {} ...".format(feature.GetField("NewID"), grid_no, value))
                    grid_parcels.append([grid_no, parcel_id])
                    parcels_in_grid.append([parcel_id, grid_no])

            if grid_parcels_writer is not None:
                grid_parcels_writer.writerows(grid_parcels)

    # sort the parcels by parcel id
    print ("Sorting parcels ...")
    parcels_in_grid.sort() 
    print ("Writing parcels ...")
    # and write the parcels
    if parcels_writer is not None:
        parcels_writer.writerows(parcels_in_grid)
    
    
if __name__ == "__main__":
    main()
    