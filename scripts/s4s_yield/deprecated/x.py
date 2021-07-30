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
    
class GridDescr(object):
    def __init__(self, grid_no, geom, value, transf):
        self.grid_no = grid_no
        self.geom = geom
        self.value = value
        
        #self.geom.Transform(transf)
        
def pixelOffset2coord(geotransform, xOffset,yOffset):
    #geotransform = raster.GetGeoTransform()
    originX = geotransform[0]
    originY = geotransform[3]
    pixelWidth = geotransform[1]
    pixelHeight = geotransform[5]
    coordX = originX+pixelWidth*xOffset
    coordY = originY+pixelHeight*yOffset
    return coordX, coordY
    
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
    
    
def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-c", "--config-file", default="/etc/sen2agri/sen2agri.conf", help="configuration file location")
    parser.add_argument("-e", "--weather-features-file", help="File containing the weather extracted features", required=True)
    parser.add_argument("-o", "--out-shp", help="File containing the weather extracted features", required=True)
    parser.add_argument("-v", "--vec", help="Shapefile containing the parcels", required=True)
    
    args = parser.parse_args()
     
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
    print(srid)
    
    driver = ogr.GetDriverByName('Esri Shapefile')
    if os.path.exists(args.out_shp):
        driver.DeleteDataSource(args.out_shp)
        
    ds = driver.CreateDataSource(args.out_shp)
    layer = ds.CreateLayer(args.out_shp, None, ogr.wkbPolygon)
    # Add one attribute
    layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn('value', ogr.OFTReal))
    defn = layer.GetLayerDefn()


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
    for ridx, row in enumerate(array):
        for cidx, value in enumerate(row):
            # Xcoord, Ycoord = pixelOffset2coord(geotransform,cidx,ridx)
            # print ("grid_no = {}:  XCoord = {}, Ycoord = {}".format(grid_no, Xcoord, Ycoord))
            poly = create_polygon(geotransform, cidx,ridx)
            # print("grid_no = {}:  poly = {}".format(grid_no, poly))
            
            # Create a new feature (attribute and geometry)
            feat = ogr.Feature(defn)
            feat.SetField('id', grid_no)
            feat.SetField('value', value)
            
            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkt(poly)
            feat.SetGeometry(geom)
            layer.CreateFeature(feat)
            
            grid_descrs.append(GridDescr(grid_no, geom, value, wgs84_to_image_trasformation))
            feat = geom = None  # destroy these
            
            grid_no += 1
    
    for feature in layer_vec:
        # print(feature.GetField("NewID"))
        geom_vec = feature.GetGeometryRef()
        if geom_vec is None :
            continue
        geom_vec.Transform(wgs84_to_image_trasformation)
        
        for grid_descr in grid_descrs:
            # print("Geom vect : {}".format(geom_vec.ExportToWkt()))
            # print("grid_descr.geom : {}".format(grid_descr.geom.ExportToWkt()))
            if geom_vec is not None and geom_vec.Intersects(grid_descr.geom):
                print("Parcel found with NewID = {} intersecting grid no {} => Extracting value {} ...".format(feature.GetField("NewID"), grid_descr.grid_no, grid_descr.value))

    
if __name__ == "__main__":
    main()
    