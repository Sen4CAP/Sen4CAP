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
import pipes
import subprocess
import re
import sys
import csv
import errno

def run_command2(args, env=None):
    # args = list(map(str, args))
    # cmd_line = " ".join(map(pipes.quote, args))
    print(args)
    # subprocess.call(args, env=env)
    
    p6=subprocess.Popen(args, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p6.communicate()[1]
    
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
    
# executes broceliande
def broceliande (image, output, sample, mounts, docker_image):

    ref = gdal.Open(image, gdal.GA_ReadOnly)
    nbr_band_per_image = int(ref.RasterCount)
    nombre_ndvi =int(nbr_band_per_image / 2)

    # cmd = "docker run --rm --name SEN4STAT_Broceliande --cpuset-cpus=\"0-39\" --memory=\"100G\"" + " "
    command = []
    command += ["docker", "run", "--rm"]
    # command += ["--name", "SEN4STAT_Broceliande"]
    for mount in mounts:
        command += ["-v", mount]

    command += [docker_image]
    command += ["-i", image]
    command += ["-o", output]
    command += ["-g", sample]
    command += ["--timeFlag"]

    for i,j in zip(range(0,nbr_band_per_image,2), range(1,nbr_band_per_image,2)):
        command += ["--ndviBands", "%s,%s"%(i,j)]

    nbr_band_total = (nbr_band_per_image + int(nombre_ndvi))
    for k in range(nbr_band_per_image,nbr_band_total,1):
        command += ["-b", k, "-f", "AP", "-t", "Max", "-a", "area", "--thresholds", "400,1000,10000,30000"]
    
    command += ["--autoThreadFlag"]
    command += ["-c", "{}-{}".format(str(nbr_band_per_image), str(nbr_band_total-1))]
    command += ["--bgRate", "100%"]
    command += ["--bgTagRate", "100%"]
    command += ["--showChannel"]
    command += ["--tagValue", "1,2,3"]
    command += ["--bgValue",  "0"]

    # print ("Executing command: {}".format(command))
    run_command(command)

def main():
    parser = argparse.ArgumentParser(
        description="Rasterizes the given input shapefile"
    )
    parser.add_argument("-i", "--image", help="Input reflectance stack image", required=True)
    parser.add_argument("-s", "--vec", help="Input samples", required=True)
    parser.add_argument("-o", "--output", help="Broceliande output", required=True)
    parser.add_argument("--docker-mounts", help="Broceliande docker mounts", required=True, nargs='+')
    parser.add_argument("--docker-image", help="Broceliande docker image", required=True)
    
    
    args = parser.parse_args()

    broceliande(args.image, args.output, args.vec, args.docker_mounts, args.docker_image)
    
if __name__ == "__main__":
    main()
