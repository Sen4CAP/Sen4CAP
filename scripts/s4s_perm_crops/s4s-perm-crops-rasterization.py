#!/usr/bin/env python

import argparse
from collections import defaultdict
from datetime import date
from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
from osgeo import osr, gdal, ogr
from gdal import gdalconst
import re
import sys
import csv
import errno

# rasterizes the input
def do_rasterize (image, vect, field, output, in_val, correction_val):

    driver = ogr.GetDriverByName('ESRI Shapefile')
    fn = vect
    dataSource = driver.Open(fn, 1)

    layer = dataSource.GetLayer()
    feature = layer.GetNextFeature()

    # Normalize the vect
    i = 0
    print("Correcting features with {} = {} (setting field to {})".format(field, in_val, correction_val))
    for feature in layer :
        if feature.GetFieldAsInteger(field) == 0:
            feature.SetField(field, correction_val)
            layer.SetFeature(feature)

    dataSource.Destroy()

    print("Done normalization \n\n Starting rasterization")
    print("Performing rasterization ...")
    
    gdalformat = 'Gtiff'
    datatype = gdal.GDT_Byte
    ref =gdal.Open(image, gdal.GA_ReadOnly)

    shp = ogr.Open(vect)
    shp_layer = shp.GetLayer()
    output_conversion = gdal.GetDriverByName(gdalformat).Create(output, ref.RasterXSize, ref.RasterYSize, 1, datatype)
    output_conversion.SetProjection(ref.GetProjectionRef())
    output_conversion.SetGeoTransform(ref.GetGeoTransform())

    Band = output_conversion.GetRasterBand(1)
    Band.SetNoDataValue(0)
    gdal.RasterizeLayer(output_conversion, [1], shp_layer, options = ["ATTRIBUTE={}".format(field)])

    print("Done rasterization")

def main():
    parser = argparse.ArgumentParser(
        description="Rasterizes the given input shapefile"
    )
    parser.add_argument("-i", "--image", help="Input image", required=True)
    parser.add_argument("-s", "--vec", help="Input samples", required=True)
    parser.add_argument("-o", "--output", help="Rasterized output", required=True)
    parser.add_argument("-f", "--field", help="Field to be used", required=True)
    parser.add_argument("-v", "--val-to-replace", type=int, help="Value to replace",  default=0)
    parser.add_argument("-r", "--replacing-val", type=int, help="Working directory",  default=3)
    
    args = parser.parse_args()

    do_rasterize(args.image, args.vec, args.field, args.output, args.val_to_replace, args.replacing_val)
    
if __name__ == "__main__":
    main()
