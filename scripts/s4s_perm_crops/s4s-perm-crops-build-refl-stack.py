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
from osgeo import osr
from osgeo import gdal
from gdal import gdalconst
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
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
    

# Creating intermediate virtual images (1 per band in each images)
def intermediate_vrt (liste, working_dir):

    try:
        os.makedirs(working_dir)
    except OSError as exc:  # Python >= 2.5
        if exc.errno == errno.EEXIST and os.path.isdir(working_dir):
            pass
        # possibly handle other errno cases here, otherwise finally:
        else:
            raise

    # Getting number of images and number of band
    nombre_image = len(liste)
    image_index_0 = gdal.Open(liste[0])
    nombre_bande_par_image = (image_index_0.RasterCount)
    
    print ("Raster bands cnt: {}".format(nombre_bande_par_image))

    # Creation of vrt image / band of each image
    for i in range(1,nombre_bande_par_image+1):
        for j in liste :
            commande="gdalbuildvrt" + " "
            commande+=os.path.join(working_dir,"output_b%s_image%05d.vrt"%(i,liste.index(j)+1)) + " "
            commande+="-b " + str(i) + " "
            commande+= j + " "
            run_command(commande)
            # p6=subprocess.Popen(commande, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # retour=p6.communicate()[1]
            print ("done image %s bande %s"%(liste.index(j)+1, i))

# Creating the final stack of input images in TIFF format
def final_vrt_to_tiff (output_path, working_dir):

    # os.makedirs(output_path, exist_ok=True)
    
    # Listing all vrt in temp directory
    liste_intermediate = sorted(glob(os.path.join(working_dir,"output_b*.vrt")))

    # Removing previous list of vrt images in a txt file and creating a new one
    if os.path.exists(os.path.join(working_dir,"intermediate_vrt.txt")):
        os.remove(os.path.join(working_dir,"intermediate_vrt.txt"))
    else:
        f=open(os.path.join(working_dir,"intermediate_vrt.txt"), 'w')
        for element in liste_intermediate:
            f.write(element +'\n')
        f.close()

    # Creation of the full vrt stack
    commande="gdalbuildvrt" + " "
    commande+="-separate" + " "
    commande+=os.path.join(working_dir,"full_stack.vrt") + " "
    commande+="-input_file_list " + os.path.join(working_dir,"intermediate_vrt.txt") + " "
    run_command(commande)
    
    # p6=subprocess.Popen(commande, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # retour=p6.communicate()[1]

    # Converting vrt to TIFF
    commande="gdal_translate" + " "
    commande+=os.path.join(working_dir,"full_stack.vrt") + " "
    commande+=output_path + " "
    commande+="-a_nodata -10000" + " "
    commande+="--config GDAL_MAX_DATASET_POOL_SIZE 1000"
    run_command(commande)
    
    # p6=subprocess.Popen(commande, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # retour=p6.communicate()[1]

    print("done full stack tiff \nExiting program")

    # End of processing

def main():
    parser = argparse.ArgumentParser(
        description="Creates a raster containing the stack reflectance bands"
    )
    parser.add_argument("-i", "--inputs", help="File containing the input products")
    parser.add_argument("-o", "--output", help="Output raster containing the stack reflectance bands")
    parser.add_argument("-w", "--working-dir", help="Working directory")
    
    args = parser.parse_args()

    fileInputs = open(args.inputs, 'r')
    prdsList = fileInputs.readlines()
    prdsList = [x.strip() for x in prdsList] 
    
    print("prds list = {}".format(prdsList))
    intermediate_vrt(prdsList, args.working_dir)
    final_vrt_to_tiff(args.output, args.working_dir)
    
if __name__ == "__main__":
    main()
