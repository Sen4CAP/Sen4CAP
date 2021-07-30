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

def run_command(args, env=None):
    # args = list(map(str, args))
    # cmd_line = " ".join(map(pipes.quote, args))
    print(args)
    # subprocess.call(args, env=env)
    
    p6=subprocess.Popen(args, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p6.communicate()[1]
    
# executes broceliande
def broceliande (image, output, sample, mounts, docker_image):

    ref = gdal.Open(image, gdal.GA_ReadOnly)
    nombre_bande_par_image = int(ref.RasterCount)
    nombre_ndvi =int(nombre_bande_par_image / 2)

    # cmd = "docker run --rm --name SEN4STAT_Broceliande --cpuset-cpus=\"0-39\" --memory=\"100G\"" + " "
    cmd = "docker run --rm --name SEN4STAT_Broceliande "
    for mount in mounts:
        cmd += "-v {} ".format(mount)
    cmd += docker_image + " "
    cmd += "-i" + image + " "
    cmd += "-o"+ output + " "
    cmd += "-g" + sample + " "
    cmd += "--timeFlag"

    for i,j in zip(range(0,nombre_bande_par_image,2), range(1,nombre_bande_par_image,2)):
        cmd += " --ndviBands %s,%s"%(i,j)

    nombre_total_bande = (nombre_bande_par_image + int(nombre_ndvi))

    for k in range(nombre_bande_par_image,nombre_total_bande,1):
        cmd += " -b %s -f AP -t Max -a area --thresholds 400,1000,10000,30000"%(k)

    cmd += " --autoThreadFlag -c {}-{} --bgRate \"100%\" --bgTagRate \"100%\" --showChannel --tagValue 1,2,3 --bgValue 100".format(str(nombre_bande_par_image), str(nombre_total_bande-1))

    # print ("Executing command: {}".format(cmd))
    run_command(cmd)

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
