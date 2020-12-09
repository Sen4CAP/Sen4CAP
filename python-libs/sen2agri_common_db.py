#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen2Agri-Processors
   Language:     Python
   Copyright:    2015-2016, CS Romania, office@c-s.ro
   See COPYRIGHT file for details.

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
_____________________________________________________________________________

"""

from __future__ import print_function
from __future__ import with_statement
import argparse
import re
import glob
import gdal
import osr
import subprocess
import lxml.etree
from lxml.builder import E
import math
import os
from os.path import isfile, isdir, join
import sys
import time, datetime
from time import gmtime, strftime
from subprocess import check_output
import pipes
import shutil
import psycopg2
import psycopg2.errorcodes
import optparse
import signal
from psycopg2.errorcodes import (SERIALIZATION_FAILURE, DEADLOCK_DETECTED)
from datetime import date
from dateutil.relativedelta import relativedelta
from threading import Thread
import threading
from datetime import timedelta

#FAKE_COMMAND = 1
DEBUG = True

DOWNLOADER_NUMBER_OF_CONFIG_PARAMS_FROM_DB = int(4)
SENTINEL2_SATELLITE_ID = int(1)
LANDSAT8_SATELLITE_ID = int(2)
L1C_UNKNOWN_PROCESSOR_OUTPUT_FORMAT = int(0)
L1C_MACCS_PROCESSOR_OUTPUT_FORMAT = int(1)
L1C_MAJA_PROCESSOR_OUTPUT_FORMAT = int(2)
L1C_SEN2COR_PROCESSOR_OUTPUT_FORMAT = int(3)
FILES_IN_LANDSAT_L1_PRODUCT = int(13)
UNKNOWN_SATELLITE_ID = int(-1)
#should not exceed 11 !!!!
MONTHS_FOR_REQUESTING_AFTER_SEASON_FINSIHED = int(2)

DATABASE_DEMMACCS_GIPS_PATH = "demmaccs.gips-path"
DATABASE_DEMMACCS_OUTPUT_PATH = "demmaccs.output-path"
DATABASE_DEMMACCS_SRTM_PATH = "demmaccs.srtm-path"
DATABASE_DEMMACCS_SWBD_PATH = "demmaccs.swbd-path"
DATABASE_DEMMACCS_MACCS_IP_ADDRESS = "demmaccs.maccs-ip-address"
DATABASE_DEMMACCS_MACCS_LAUNCHER = "demmaccs.maccs-launcher"
DATABASE_DEMMACCS_WORKING_DIR = "demmaccs.working-dir"

DATABASE_DEMMACCS_COMPRESS_TIFFS = "demmaccs.compress-tiffs"
DATABASE_DEMMACCS_COG_TIFFS = "demmaccs.cog-tiffs"
DATABASE_DEMMACCS_REMOVE_SRE = "demmaccs.remove-sre"
DATABASE_DEMMACCS_REMOVE_FRE = "demmaccs.remove-fre"

DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = int(1)
DATABASE_DOWNLOADER_STATUS_DOWNLOADED_VALUE = int(2)
DATABASE_DOWNLOADER_STATUS_FAILED_VALUE = int(3)
DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE = int(4)
DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE = int(5)
DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE = int(6)

TIME_INTERVAL_RETRY = int(8) #hours
MAX_LOG_FILE_SIZE = int(419430400) #bytes -> 400 MB
MAX_NUMBER_OF_KEPT_LOG_FILES = int(4) #number of maximum logfiles to be kept

g_exit_flag = False

def delete_file_if_match(fullFilePath, fileName, regex, fileType) :
    isMatch = re.match(regex, fileName)
    if isMatch is not None:
        print("Deleting {} file {}".format(fileType, fullFilePath))
        os.remove(fullFilePath)

def log(location, info, log_filename = ""):    
    try:
        if DEBUG:
            print("{}:[{}]:{}".format(str(datetime.datetime.now()), os.getpid(), str(info)))
            sys.stdout.flush()
        if len(location) > 0 and len(log_filename) > 0: 
            log_path = os.path.join(location, log_filename)   
            log = open(log_path, 'a')
            log.write("{}:[{}]:{}\n".format(str(datetime.datetime.now()), os.getpid(), str(info)))
            log.close()
    except:
        print("Could NOT write inside the log file {}".format(log_filename))

def manage_log_file(location, log_filename):
    try:
        log_file = os.path.join(location, log_filename)
        if not os.path.isfile(log_file):
            print("The logfile {} does not exist yet".format(log_file))
            return
        if os.stat(log_file).st_size >= MAX_LOG_FILE_SIZE:
            print("Log file is bigger than {}".format(MAX_LOG_FILE_SIZE))
            #take the  current datetime
            new_log_file = "{}_{}".format(log_file, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            #move the log file with the new name, with datetime at the end
            print("Log file {} moved to {}".format(log_file, new_log_file))
            shutil.move(log_file, new_log_file)
            #check if there are other previous saved log files and delete the oldest one
            previous_log_files = glob.glob("{}*.log_20*".format(location if location.endswith("/") else location + "/"))
            while len(previous_log_files) > MAX_NUMBER_OF_KEPT_LOG_FILES:
                oldest_idx = -1
                idx = 0
                oldest_datetime = datetime.datetime.strptime("40000101000001", "%Y%m%d%H%M%S")
                for log_file in previous_log_files:
                    underscore_idx = log_file.rfind('_')
                    if underscore_idx > 0 and underscore_idx + 1 < len(log_file):
                        str_log_datetime = log_file[underscore_idx + 1:len(log_file)]
                        if len(str_log_datetime) != 14: #number of digits in the timestamp
                            idx += 1
                            continue
                        log_datetime = datetime.datetime.strptime(str_log_datetime, "%Y%m%d%H%M%S")
                        if log_datetime <= oldest_datetime:
                            oldest_datetime = log_datetime
                            oldest_idx = idx
                    idx += 1
                # remove the oldest file if found
                print("oldest_datetime: {} | oldest_idx: {}" .format(oldest_datetime, oldest_idx))
                if oldest_idx > -1:
                    os.remove(previous_log_files[oldest_idx])
                    print("Log file {} removed".format(previous_log_files[oldest_idx]))
                else:
                    break
                #the main 'if'  can be replaced by 'while', and the following line should
                #be uncommented. be aware though...it can lead to infinite loop (probably not, but never say never again
                previous_log_files = glob.glob("{}*.log_20*".format(location if location.endswith("/") else location + "/"))
    except Exception as e:
        print("Error in manage_log_file: exception {} !".format(e))




def run_command(cmd_array, log_path = "", log_filename = "", fake_command = False):
    start = time.time()
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    log(log_path, "Running command: {}".format(cmd_str), log_filename)
    res = 0
    if not fake_command:
        try:
            res = subprocess.call(cmd_array, shell=False)
        except:
            res = 1 
    ok = "OK"
    nok = "NOK"
    log(log_path, "Command finished {} (res = {}) in {} : {}".format((ok if res == 0 else nok), res, datetime.timedelta(seconds=(time.time() - start)), cmd_str), log_filename)
    return res


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def signal_handler(signal, frame):
    global g_exit_flag
    g_exit_flag = True
    print("SIGINT caught! {}".format(g_exit_flag))
    #sys.exit(0)


def GetExtent(gt, cols, rows):
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + px * gt[1] + py * gt[2]
            y = gt[3] + px * gt[4] + py * gt[5]
            ext.append([x, y])
        yarr.reverse()
    return ext


def ReprojectCoords(coords, src_srs, tgt_srs):
    trans_coords = []
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        trans_coords.append([x, y])
    return trans_coords

def get_footprint(image_filename):
    dataset = gdal.Open(image_filename, gdal.gdalconst.GA_ReadOnly)

    size_x = dataset.RasterXSize
    size_y = dataset.RasterYSize

    geo_transform = dataset.GetGeoTransform()

    spacing_x = geo_transform[1]
    spacing_y = geo_transform[5]

    extent = GetExtent(geo_transform, size_x, size_y)

    source_srs = osr.SpatialReference()
    source_srs.ImportFromWkt(dataset.GetProjection())
    epsg_code = source_srs.GetAttrValue("AUTHORITY", 1)
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(4326)

    wgs84_extent = ReprojectCoords(extent, source_srs, target_srs)
    return (wgs84_extent, extent)


def create_recursive_dirs(dir_name):
    try:
        #create recursive dir
        os.makedirs(dir_name)
    except:
        pass
    #check if it already exists.... otherwise the makedirs function will raise an exception
    if os.path.exists(dir_name):
        if not os.path.isdir(dir_name):
            print("The directory couldn't be created. Reason: file with the same name exists: {}".format(dir_name))
            print("Remove: {}".format(dir_name))
            sys.stdout.flush()
            return False
    else:
        #for sure, the problem is with access rights
        print("The directory couldn't be created due to the access rights {}".format(dir_name))
        return False
    return True


def remove_dir(directory):
    try:
        shutil.rmtree(directory)
    except:
        return False
    return True


def remove_dir_content(directory):
    for content in os.listdir(directory):
        content_path = os.path.join(directory, content)
        try:
            if os.path.isfile(content_path):
                os.unlink(file_path)
            elif os.path.isdir(content_path): 
                shutil.rmtree(content_path)
        except Exception as e:
            print(e)
            return False
    return True


def copy_directory(src, dest):
    try:
        #print("Fake copy {} to {}".format(src, dest))
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print("Directory not copied. Error: {}".format(e))
        return False
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print("Directory not copied. Error: {}".format(e))
        return False
    return True

def get_product_info(product_name):
    acquisition_date = None
    sat_id = UNKNOWN_SATELLITE_ID
    if product_name.startswith("S2"):
        m = re.match("\w+_V(\d{8}T\d{6})_\w+.SAFE", product_name)
        # check if the new convention naming aplies
        if m == None:
            m = re.match(r"\w+_(\d{8}T\d{6})_\w+.SAFE", product_name)
        if m != None:
            sat_id = SENTINEL2_SATELLITE_ID
            acquisition_date = m.group(1)
    elif product_name.startswith("LC8") or product_name.startswith("LC08"):
        m = re.match(r"LC8\d{6}(\d{7})[A-Z]{3}\d{2}", product_name)
        if m != None:
            acquisition_date = datetime.datetime.strptime("{} {}".format(m.group(1)[0:4],m.group(1)[4:]), '%Y %j').strftime("%Y%m%dT%H%M%S")
        else :
            m = re.match(r"LC08_[A-Z0-9]+_\d{6}_(\d{8})_\d{8}_\d{2}_[A-Z0-9]{2}", product_name)
            if m != None:
                acquisition_date = datetime.datetime.strptime("{} {} {}".format(m.group(1)[0:4],m.group(1)[4:6], m.group(1)[6:]), '%Y %m %d').strftime("%Y%m%dT%H%M%S")
        if m != None:
            sat_id = LANDSAT8_SATELLITE_ID

    return sat_id and (sat_id, acquisition_date)

def check_maja_valid_output(maja_out, tile_id):
    if not os.path.isdir(maja_out):
        return False
    dir_content = glob.glob("{}/*".format(maja_out))
    atb_files_count = 0
    fre_files_count = 0
    sre_files_count = 0
    qkl_file = False
    mtd_file = False
    data_dir = False
    masks_dir = False
    for filename in dir_content:
        if os.path.isfile(filename) and re.search(r"_L2A_.*ATB.*\.tif$", filename, re.IGNORECASE) is not None:
            atb_files_count += 1
        if os.path.isfile(filename) and re.search(r"_L2A_.*FRE.*\.tif$", filename, re.IGNORECASE) is not None:
            fre_files_count += 1
        if os.path.isfile(filename) and re.search(r"_L2A_.*SRE.*\.tif$", filename, re.IGNORECASE) is not None:
            sre_files_count += 1
        if os.path.isfile(filename) and re.search(r"_L2A_.*MTD.*\.xml$", filename, re.IGNORECASE) is not None:
            mtd_file = True
        if os.path.isfile(filename) and re.search(r"_L2A_.*QKL.*\.jpg$", filename, re.IGNORECASE) is not None:
            qkl_file = True
        if os.path.isdir(filename) and re.search(r".*\DATA$", filename) is not None:
            data_dir = True
        if os.path.isdir(filename) and re.search(r".*\MASKS$", filename) is not None:
            masks_dir = True

    res = (atb_files_count > 0 and fre_files_count > 0 and sre_files_count > 0 and qkl_file and mtd_file and data_dir and masks_dir)
    return res

def get_l1c_processor_output_format(working_directory, tile_id):
    if not os.path.isdir(working_directory):
        return L1C_UNKNOWN_PROCESSOR_OUTPUT_FORMAT, None
    #check for MACCS output
    maccs_dbl_dir = glob.glob("{}/*_L2VALD_{}*.DBL.DIR".format(working_directory, tile_id))
    maccs_hdr_file = glob.glob("{}/*_L2VALD_{}*.HDR".format(working_directory, tile_id))
    if len(maccs_dbl_dir) >= 1 and len(maccs_hdr_file) >= 1:
        return L1C_MACCS_PROCESSOR_OUTPUT_FORMAT, None
    #check for THEIA/MUSCATE format
    working_dir_content = glob.glob("{}/*".format(working_directory))
    for maja_out in working_dir_content:
        if os.path.isdir(maja_out) and re.search(r".*_L2A_.*", maja_out, re.IGNORECASE) and check_maja_valid_output(maja_out, tile_id):
            return L1C_MAJA_PROCESSOR_OUTPUT_FORMAT, maja_out
	#check for Sen2Cor format
	pvi_pattern = "S2[A|B|C|D]_MSIL2A_*/GRANULE/L2A*_T{}_*/QI_DATA/*PVI.[jp2|tif|jpeg]".format(tile_id)
	pvi_path = os.path.join(working_directory,pvi_pattern)
	pvi_files = glob.glob(pvi_path)
	if len(pvi_files) == 1:
            return L1C_SEN2COR_PROCESSOR_OUTPUT_FORMAT, None
		
    return L1C_UNKNOWN_PROCESSOR_OUTPUT_FORMAT, None

#def check_if_season(startSeason, endSeason, numberOfMonthsAfterEndSeason, yearArray):
##, logDir, logFileName):
#    currentYear = datetime.date.today().year
#    currentMonth = datetime.date.today().month
#    #log(logDir, "{} | {}".format(startSeason, endSeason), logFileName)
#    del yearArray[:]
#    yearArray.append(currentYear)
#    yearArray.append(currentYear)
#    startSeasonMonth = int(startSeason[0:2])
#    startSeasonDay = int(startSeason[2:4])
#    print("StartSeason {}".format(startSeason))
#    print("endSeason {}".format(endSeason))
#    # check if we have also the year
#    startSeasonYear = -1
#    endSeasonYear = -1
#    endSeasonMonth = int(endSeason[0:2])
#    endSeasonDay = int(endSeason[2:4])
#    if len(endSeason) == 8 :
#        endSeasonYear = int(endSeason[4:8])
#    elif len(endSeason) == 6 :
#        endSeasonYear = int(endSeason[4:6]) + 2000
#    startSeasonYear = endSeasonYear
#    print("Extracted years: start {} end {}".format(startSeasonYear, endSeasonYear))
#
#    if startSeasonMonth < 1 or startSeasonMonth > 12 or startSeasonDay < 1 or startSeasonDay > 31 or endSeasonMonth < 1 or endSeasonMonth > 12 or endSeasonDay < 1 or endSeasonDay > 31:
#        return False
#    #check if the season comprises 2 consecutive years (e.q. from october to march next year)
#    if startSeasonMonth > endSeasonMonth:
#        if currentMonth >= startSeasonMonth and currentMonth <= 12:
#            yearArray[1] = currentYear + 1
#        else:
#            if currentMonth >= 1:
#                yearArray[0] = currentYear - 1
#        # if we have the year specified and the start and end seasons are greater
#        # then subtract 1 from start year
#        if startSeasonYear != -1 :
#            startSeasonYear = startSeasonYear - 1
#    else:
#        if currentMonth < startSeasonMonth:
#            yearArray[0] = currentYear - 1
#            yearArray[1] = currentYear - 1
#
#    # Disregard the computed years and replace the years in the array if we have valid years in the dates
#    print("Extracted years2: start {} end {}".format(startSeasonYear, endSeasonYear))
#    if ((startSeasonYear != -1)):
#        yearArray[0] = startSeasonYear
#        yearArray[1] = endSeasonYear
#        print("set years: start {} end {}".format(yearArray[0], yearArray[1]))
#        return True   # To check if is not OK, if no, comment the line
#
#    currentDate = datetime.date.today()
#    endSeasonYearToCheck = int(yearArray[1])
#    endSeasonMonthToCheck = int(endSeasonMonth) + numberOfMonthsAfterEndSeason
#    if endSeasonMonthToCheck > 12:
#        endSeasonMonthToCheck -= 12
#        endSeasonYearToCheck += 1
#    endSeasonDayToCheck = int(endSeasonDay)
#    if endSeasonDayToCheck > 30:
#        endSeasonDayToCheck = 28
#    if currentDate < datetime.date(int(yearArray[0]), int(startSeasonMonth), int(startSeasonDay)) or currentDate > datetime.date(endSeasonYearToCheck, endSeasonMonthToCheck, endSeasonDayToCheck):
#        return False
#    print("start year {}".format(yearArray[0]))
#    print("end year {}".format(yearArray[1]))
#    return True


def landsat_crop_to_cutline(landsat_product_path, working_dir):
    product_name = os.path.basename(landsat_product_path[:len(landsat_product_path) - 1]) if landsat_product_path.endswith("/") else os.path.basename(landsat_product_path)
    tile = re.match("LC8(\w{6})\w+|[A-Z][A-Z]\d\d_[A-Z0-9]+_(\d{6})_\d{8}_\d{8}_\d{2}_[A-Z0-9]{2}", product_name)
    if tile is None:
        return "", "Couldn't get the tile id for the LANDSAT product {} found here {}. Imposible to process the alignment, exit".format(product_name, landsat_product_path)

    tile_id = (tile.group(1) or tile.group(2))
    alignment_directory = "{}/{}_alignment".format(working_dir, tile_id)
    aligned_landsat_directory_path = "{}/{}".format(alignment_directory, product_name)
    if not create_recursive_dirs(alignment_directory):
        return "", "Could not create the alignment directory {} for LANDSAT product {}".format(alignment_directory, landsat_product_path)
    if not create_recursive_dirs(aligned_landsat_directory_path):
        return "", "Could not create the aligned landsat {} directory for LANDSAT product {}".format(aligned_landsat_directory_path, landsat_product_path)

    landsat_files = glob.glob("{}/*".format(landsat_product_path))
    if(len(landsat_files) < FILES_IN_LANDSAT_L1_PRODUCT):
        return "", "Found {} files in LANDSAT product {}. Should have been {}".format(len(landsat_files), landsat_product_path, FILES_IN_LANDSAT_L1_PRODUCT)

    first_tile_file_path = None
    tmp_shape_file = ""
    for landsat_file in landsat_files:
        landsat_file_basename = os.path.basename(landsat_file[:len(landsat_file) - 1]) if landsat_file.endswith("/") else os.path.basename(landsat_file)
        band = re.match("LC8\w{13}[A-Z]{3}\w{2}_B1.TIF|[A-Z][A-Z]\d\d_[A-Z0-9]+_\d{6}_\d{8}_\d{8}_\d{2}_[A-Z0-9]{2}_B1\.TIF", landsat_file_basename)
        if band is not None:
            # the filename for the first output tile is created only (not accessed)
            first_tile_file_path = "{}/{}".format(aligned_landsat_directory_path, landsat_file_basename)
            out = check_output(["gdalsrsinfo", "-o", "wkt", landsat_file])
            print("out = {}".format(out))
            tmp_shape_file = "{}/tmp.shp".format(alignment_directory)
            print("tmp = {}".format(tmp_shape_file))
            #TODO: handle errors !!!!
            run_command(["ogr2ogr", "-t_srs", out, "-where", "PR={}".format(tile_id), "-overwrite", tmp_shape_file, "/usr/share/sen2agri/wrs2_descending/wrs2_descending.shp"])
            break
    if not first_tile_file_path :
	    return "", "Unable to find B1 in output product"

    processed_files_counter = 0
    for landsat_file in landsat_files:
        landsat_file_basename = os.path.basename(landsat_file[:len(landsat_file) - 1]) if landsat_file.endswith("/") else os.path.basename(landsat_file)
        print("landsat_file_basename = {}".format(landsat_file_basename))
        band = re.match("LC8\w{13}[A-Z]{3}\w{2}_B\w+.TIF|[A-Z][A-Z]\d\d_[A-Z0-9]+_\d{6}_\d{8}_\d{8}_\d{2}_[A-Z0-9]{2}_B\w+\.TIF", landsat_file_basename)
        if band is not None:
            output_file = "{}/{}".format(aligned_landsat_directory_path, landsat_file_basename)
            run_command(["gdalwarp", "-overwrite", "-crop_to_cutline", "-cutline", tmp_shape_file, landsat_file, output_file])
            processed_files_counter += 1

    print("Using footprint reference: {}".format(first_tile_file_path))

    for landsat_file in landsat_files:
        landsat_file_basename = os.path.basename(landsat_file[:len(landsat_file) - 1]) if landsat_file.endswith("/") else os.path.basename(landsat_file)
	metadata = re.match("LC8\w{13}[A-Z]{3}\w{2}_MTL.txt|[A-Z][A-Z]\d\d_[A-Z0-9]+_\d{6}_\d{8}_\d{8}_\d{2}_[A-Z0-9]{2}_MTL\.txt", landsat_file_basename)
	if metadata is not None:
            output_metadata_file = "{}/{}".format(aligned_landsat_directory_path, landsat_file_basename)
            print(output_metadata_file)
            shape_env_points_wgs84, shape_env_points = get_footprint(first_tile_file_path)
        try:
	        with open(landsat_file, 'r') as genuine_metadata, open(output_metadata_file, 'w') as modified_metadata:
                    for line in genuine_metadata:
                        # UTM coordinates
                        if "CORNER_UL_PROJECTION_X_PRODUCT" in line:
                            line = "    CORNER_UL_PROJECTION_X_PRODUCT = " + str(shape_env_points[0][0]) + "\n"
                        if "CORNER_UL_PROJECTION_Y_PRODUCT" in line:
                            line = "    CORNER_UL_PROJECTION_Y_PRODUCT = " + str(shape_env_points[0][1]) + "\n"
                        if "CORNER_UR_PROJECTION_X_PRODUCT" in line:
                            line = "    CORNER_UR_PROJECTION_X_PRODUCT = " + str(shape_env_points[3][0]) + "\n"
                        if "CORNER_UR_PROJECTION_Y_PRODUCT" in line:
                            line = "    CORNER_UR_PROJECTION_Y_PRODUCT = " + str(shape_env_points[3][1]) + "\n"
                        if "CORNER_LL_PROJECTION_X_PRODUCT" in line:
                            line = "    CORNER_LL_PROJECTION_X_PRODUCT = " + str(shape_env_points[1][0]) + "\n"
                        if "CORNER_LL_PROJECTION_Y_PRODUCT" in line:
                            line = "    CORNER_LL_PROJECTION_Y_PRODUCT = " + str(shape_env_points[1][1]) + "\n"
                        if "CORNER_LR_PROJECTION_X_PRODUCT" in line:
                            line = "    CORNER_LR_PROJECTION_X_PRODUCT = " + str(shape_env_points[2][0]) + "\n"
                        if "CORNER_LR_PROJECTION_Y_PRODUCT" in line:
                            line = "    CORNER_LR_PROJECTION_Y_PRODUCT = " + str(shape_env_points[2][1]) + "\n"
                        # latlong coordinates

                        if "CORNER_UL_LAT_PRODUCT" in line:
                            line = "    CORNER_UL_LAT_PRODUCT = " + str(shape_env_points_wgs84[0][0]) + "\n"
                        if "CORNER_UL_LON_PRODUCT" in line:
                            line = "    CORNER_UL_LON_PRODUCT = " + str(shape_env_points_wgs84[0][1]) + "\n"
                        if "CORNER_UR_LAT_PRODUCT" in line:
                            line = "    CORNER_UR_LAT_PRODUCT = " + str(shape_env_points_wgs84[3][0]) + "\n"
                        if "CORNER_UR_LON_PRODUCT" in line:
                            line = "    CORNER_UR_LON_PRODUCT = " + str(shape_env_points_wgs84[3][1]) + "\n"
                        if "CORNER_LL_LAT_PRODUCT" in line:
                            line = "    CORNER_LL_LAT_PRODUCT = " + str(shape_env_points_wgs84[1][0]) + "\n"
                        if "CORNER_LL_LON_PRODUCT" in line:
                            line = "    CORNER_LL_LON_PRODUCT = " + str(shape_env_points_wgs84[1][1]) + "\n"
                        if "CORNER_LR_LAT_PRODUCT" in line:
                            line = "    CORNER_LR_LAT_PRODUCT = " + str(shape_env_points_wgs84[2][0]) + "\n"
                        if "CORNER_LR_LON_PRODUCT" in line:
                            line = "    CORNER_LR_LON_PRODUCT = " + str(shape_env_points_wgs84[2][1]) + "\n"

		        modified_metadata.write(line)
        except EnvironmentError:
	        return "", "Could not open the landsat metadata file for alignment or could not create the output file: Input = {} | Output = {}".format(landsat_file, output_metadata_file)
        processed_files_counter += 1
    if(processed_files_counter != FILES_IN_LANDSAT_L1_PRODUCT):
        return "", "The number of processed files in LANDSAT alignment is {} which is different than how many the should have been: {}".format(processed_files_counter, FILES_IN_LANDSAT_L1_PRODUCT)
    #all went ok, return the path for the aligned product
    return aligned_landsat_directory_path, ""


###########################################################################
class OptionParser (optparse.OptionParser):

    def check_required (self, opt):
      option = self.get_option(opt)
      # Assumes the option's 'default' is set to None!
      if getattr(self.values, option.dest) is None:
          self.error("{} option not supplied".format(option))

class Args(object):
    def __init__(self):
        self.general_log_path = "/tmp/"
        self.general_log_filename = "downloader.log"

###########################################################################
class Config(object):
    def __init__(self):
        self.host = ""
        self.database = ""
        self.user = ""
        self.password = ""
    def loadConfig(self, configFile):
        try:
            with open(configFile, 'r') as config:
                found_section = False
                for line in config:
                    line = line.strip(" \n\t\r")
                    if found_section and line.startswith('['):
                        break
                    elif found_section:
                        elements = line.split('=')
                        if len(elements) == 2:
                            if elements[0].lower() == "hostname":
                                self.host = elements[1]
                            elif elements[0].lower() == "databasename":
                                self.database = elements[1]
                            elif elements[0].lower() == "username":
                                self.user = elements[1]
                            elif elements[0].lower() == "password":
                                self.password = elements[1]
                            else:
                                print("Unkown key for [Database] section")
                        else:
                            print("Error in config file, found more than on keys, line: {}".format(line))
                    elif line == "[Database]":
                        found_section = True
        except:
            print("Error in opening the config file ".format(str(configFile)))
            return False
        if len(self.host) <= 0 or len(self.database) <= 0:
            return False
        return True


###########################################################################
class SeasonInfo(object):
    def __init__(self):

        self.seasonName = ""
        self.seasonId = int(0)

        self.startSeasonMonth = int(0)
        self.startSeasonDay = int(0)
        self.endSeasonMonth = int(0)
        self.endSeasonDay = int(0)
        self.startSeasonYear = int(0)
        self.endSeasonYear = int(0)

        self.startSeasonDate = datetime.datetime(2016, 01, 01, 00, 00, 00)
        self.endSeasonDate = datetime.datetime(2016, 01, 01, 00, 00, 00)

        self.startSeasonDateStr = ""
        self.endSeasonDateStr = ""

        self.startOfSeasonOffset = int(0)

    def setStartSeasonDate(self, startSeasonDate):
        self.startSeasonDate = startSeasonDate + relativedelta(months=-self.startOfSeasonOffset)
        self.startSeasonDateStr = self.startSeasonDate.strftime("%Y-%m-%d")
        #parsedDate = datetime.datetime.strptime(startSeasonDate, "%Y-%m-%d" )
        self.startSeasonDay = self.startSeasonDate.day
        self.startSeasonMonth = self.startSeasonDate.month
        self.startSeasonYear = self.startSeasonDate.year

    def setEndSeasonDate(self, endSeasonDate):
        self.endSeasonDate = endSeasonDate
        self.endSeasonDateStr = self.endSeasonDate.strftime("%Y-%m-%d")
        #parsedDate = datetime.datetime.strptime(endSeasonDate, "%Y-%m-%d" )
        self.endSeasonDay = self.endSeasonDate.day
        self.endSeasonMonth = self.endSeasonDate.month
        self.endSeasonYear = self.endSeasonDate.year

    def setStartSeasonOffset(self, startOfSeasonOffset):
        self.startOfSeasonOffset = int(startOfSeasonOffset)
        self.setStartSeasonDate(self.startSeasonDate)

class AOIContext(object):
    def __init__(self):
        # the following info will be fed up from database
        self.siteId = int(0)
        self.siteName = ""
        self.polygon = []

        #self.startSeasonMonth = int(0)
        #self.startSeasonDay = int(0)
        #self.endSeasonMonth = int(0)
        #self.endSeasonDay = int(0)
        #self.startSeasonYear = int(0)
        #self.endSeasonYear = int(0)

        self.aoiSeasonInfos = []

        self.maxCloudCoverage = int(100)
        self.maxRetries = int(3)
        self.writeDir = ""
        self.aoiHistoryFiles = []
        self.aoiTiles = []
        #the following info will be fed up from the downloader arguments
        self.configObj = None
        self.remoteSiteCredentials = ""
        self.proxy = None
        #sentinel satellite only
        self.sentinelLocation = ""
        self.localInDir = ""
        #ed of sentinel satellite only
        #landsat only
        self.landsatDirNumbers = None
        self.landsatStation = None
        #end of landsat only

    def addHistoryFiles(self, historyFiles):
        self.aoiHistoryFiles = historyFiles

    def appendHistoryFile(self, historyFile):
        self.aoiHistoryFiles.append(historyFile)

    def appendTile(self, tile):
        self.aoiTiles.append(tile)

    def setConfigParams(self, configParams, forced_season = False):
        if len(configParams) != DOWNLOADER_NUMBER_OF_CONFIG_PARAMS_FROM_DB:
            return False

#        startSummerSeason = configParams[0]
#        endSummerSeason = configParams[1]
#        startWinterSeason = configParams[2]
#        endWinterSeason = configParams[3]

        # Apply the season offset if defined for each season in the site
        for seasonInfo in self.aoiSeasonInfos:
            seasonInfo.setStartSeasonOffset(configParams[0])

        self.maxCloudCoverage = int(configParams[1])
        self.maxRetries = int(configParams[2])
        self.writeDir = configParams[3]
        #print("Seasons: summer:{}-{} / winter:{}-{}".format(startSummerSeason, endSummerSeason, startWinterSeason, endWinterSeason))
        # first position is the startSeasonYear, the second is the endPositionYear
#        currentYearArray = []
#        if forced_season:
#            check_if_season(startSummerSeason, endSummerSeason, MONTHS_FOR_REQUESTING_AFTER_SEASON_FINSIHED, currentYearArray)
#            self.startSeasonMonth = int(startSummerSeason[0:2])
#            self.startSeasonDay = int(startSummerSeason[2:4])
#            self.endSeasonMonth = int(endSummerSeason[0:2])
#            self.endSeasonDay = int(endSummerSeason[2:4])
#        else:
#            if startSummerSeason != "null" and endSummerSeason != "null" and check_if_season(startSummerSeason, endSummerSeason, MONTHS_FOR_REQUESTING_AFTER_SEASON_FINSIHED, currentYearArray):
#                self.startSeasonMonth = int(startSummerSeason[0:2])
#                self.startSeasonDay = int(startSummerSeason[2:4])
#                self.endSeasonMonth = int(endSummerSeason[0:2])
#                self.endSeasonDay = int(endSummerSeason[2:4])
#            elif startWinterSeason != "null" and endWinterSeason != "null" and check_if_season(startWinterSeason, endWinterSeason, MONTHS_FOR_REQUESTING_AFTER_SEASON_FINSIHED, currentYearArray):
#                self.startSeasonMonth = int(startWinterSeason[0:2])
#                self.startSeasonDay = int(startWinterSeason[2:4])
#                self.endSeasonMonth = int(endWinterSeason[0:2])
#                self.endSeasonDay = int(endWinterSeason[2:4])
#            else:
#                print("Out of season ! No request will be made for {}".format(self.siteName))
#                sys.stdout.flush()
#                return False
#        if len(currentYearArray) == 0:
#            print("Something went wrong in check_if_season function")
#            sys.stdout.flush()
#            return False
#        self.startSeasonYear = currentYearArray[0]
#        self.endSeasonYear = currentYearArray[1]

        return True

    def fillHistory(self, dbInfo):
        self.aoiHistoryFiles = dbInfo

    def fileExists(self, filename):
        for historyFilename in self.aoiHistoryFiles:
            if filename == historyFilename:
                return True
        return False

    def setConfigObj(self, configObj):
        self.configObj = configObj

    def setRemoteSiteCredentials(self, filename):
        self.remoteSiteCredentials = filename

    def setProxy(self, filename):
        self.proxy = filename

    def setSentinelLocation(self, location):
        self.sentinelLocation = location

    def setLocalInDir(self, localindir):
        self.localInDir = localindir

    def setLandsatDirNumbers(self, dir_numbers):
        self.landsatDirNumbers = dir_numbers

    def setLandsatStation(self, station):
        self.landsatStation = station

    def printInfo(self):
        print("SiteID  : {}".format(self.siteId))
        print("SiteName: {}".format(self.siteName))
        print("Polygon : {}".format(self.polygon))

        print("Number of Seasons : {}".format(len(self.aoiSeasonInfos)))
        i = 0
        for seasonInfo in self.aoiSeasonInfos:
            print("Season Idx  : {}".format(i))
            print("Season Name : {}".format(seasonInfo.seasonName))
            print("Season Id   : {}".format(seasonInfo.seasonId))
            print("startS      : {}-{}-{}".format(seasonInfo.startSeasonYear, seasonInfo.startSeasonMonth, seasonInfo.startSeasonDay))
            print("endS        : {}-{}-{}".format(seasonInfo.endSeasonYear, seasonInfo.endSeasonMonth, seasonInfo.endSeasonDay))
            i += 1

        print("CloudCov: {}".format(self.maxCloudCoverage))
        print("general configuration: ")
        if self.configObj != None:
            print("configObj : {}|{}|{}|{}".format(self.configObj.host, self.configObj.database, self.configObj.user, self.configObj.password))
        else:
            print("configObj : None")
        print("remSiteCred: {}".format(self.remoteSiteCredentials))
        if self.proxy != None:
            print("proxy      : {}".format(self.proxy))
        else:
            print("proxy      : None")
        print("sentinelLocation: {}".format(self.sentinelLocation))
        print("localInDir: {}".format(self.localInDir))
        if self.landsatDirNumbers != None:
            print("landsatDirNumbers: {}".format(self.landsatDirNumbers))
        else:
            print("landsatDirNumbers: None")
        if self.landsatStation != None:
            print("landsatStation: {}".format(self.landsatStation))
        else:
            print("landsatStation: None")

        if len(self.aoiTiles) <= 0:
            print("tiles: NONE")
        else:
            print("tiles:")
            print(" ".join(self.aoiTiles))

        sys.stdout.flush()


###########################################################################
class AOIInfo(object):
    def __init__(self, serverIP, databaseName, user, password, logFile):
        self.serverIP = serverIP
        self.databaseName = databaseName
        self.user = user
        self.password = password
        self.isConnected = False
        self.logFile = logFile

    def databaseConnect(self):
        if self.isConnected:
            return True
        try:
            connectString = "dbname='{}' user='{}' host='{}' password='{}'".format(self.databaseName, self.user, self.serverIP, self.password)
            self.conn = psycopg2.connect(connectString)
            self.cursor = self.conn.cursor()
            self.isConnected = True
        except:
            print("Unable to connect to the database")
            self.isConnected = False
            return False
        return True

    def databaseDisconnect(self):
        if self.conn:
            self.conn.close()
            self.isConnected = False

#    def getAOI(self, satelliteId, site_id = -1, start_date = "", end_date = ""):
#        writeDirSatelliteName = ""
#        if satelliteId == SENTINEL2_SATELLITE_ID:
#            writeDirSatelliteName = "s2."
#        elif satelliteId == LANDSAT8_SATELLITE_ID:
#            writeDirSatelliteName = "l8."
#        else:
#            return False
#        if not self.databaseConnect():
#            return False
#        try:
#            #self.cursor.execute("select *,st_astext(geog) from site")
#            self.cursor.execute("select * from sp_downloader_get_sites()")
#            rows = self.cursor.fetchall()
#        except:
#            self.databaseDisconnect()
#            return []
#        # retArray will be a list of AOIContext
#        retArray = []
#        for row in rows:
#            if len(row) == 4 and row[3] != None:
#                # retry in case of disconnection
#                if not self.databaseConnect():
#                    return False
#                if site_id > 0 and site_id != int(row[0]):
#                    continue
#                currentAOI = AOIContext()
#                currentAOI.siteId = int(row[0])
#                currentAOI.siteName = row[2]
#                currentAOI.polygon = row[3]
#
#                baseQuery = "select * from sp_get_parameters(\'downloader."
#                whereQuery = "where \"site_id\"="
#                suffixArray = ["summer-season.start\')", "summer-season.end\')", "winter-season.start\')", "winter-season.end\')", "max-cloud-coverage\')", "{}max-retries')".format(writeDirSatelliteName), "{}write-dir\')".format(writeDirSatelliteName)]
#                dbHandler = True
#                idx = 0
#                configArray = []
#                for suffix in suffixArray:
#                    baseQuerySite = "{}{}".format(baseQuery, suffix)
#                    query = "{} {} {}".format(baseQuerySite, whereQuery, currentAOI.siteId)
#                    #print("query with where={}".format(query))
#                    baseQuerySite += " where \"site_id\" is null"
#                    try:
#                        self.cursor.execute(query)
#                        if self.cursor.rowcount <= 0:
#                            if idx <= 3:
#                                configArray.append("null")
#                            else:
#                                self.cursor.execute(baseQuerySite)
#                                if self.cursor.rowcount <= 0:
#                                    print("Could not get even the default value for downloader.{}".format(suffix))
#                                    dbHandler = False
#                                    break
#                                if self.cursor.rowcount != 1:
#                                    print("More than 1 result from the db for downloader.{}".format(suffix))
#                                    dbHandler = False
#                                    break
#                                result = self.cursor.fetchall()
#                                #print("result={}".format(result))
#                                configArray.append(result[0][2])
#                        else:
#                            result = self.cursor.fetchall()
#                            #print("result={}".format(result))
#                            configArray.append(result[0][2])
#                        idx += 1
#                    except Exception, e:
#                        print("exception in query for downloader.{}:".format(suffix))
#                        print("{}".format(e))
#                        self.databaseDisconnect()
#                        dbHandler = False
#                        break
#                if dbHandler:
#                    if not configArray[-1].endswith("/"):
#                        configArray[-1] += "/"
#                    configArray[-1] += currentAOI.siteName
#                    forced_season = False
#                    if len(start_date) > 0 and len(end_date) > 0:
#                        configArray[0] = start_date
#                        configArray[1] = end_date
#                        configArray[2] = "null"
#                        configArray[3] = "null"
#                        if site_id > 0:
#                            # the offline_l1_handler app is calling this function with site_id set to -1, so don't print this message for it
#                            print("Forcing manuall download for time interval: {} - {}".format(start_date, end_date))
#                            sys.stdout.flush()
#                        forced_season = True
#                    if not currentAOI.setConfigParams(configArray, forced_season):
#                        print("OUT OF THE SEASON")
#                        sys.stdout.flush()
#                        continue
#                    try:
#                        self.cursor.execute("""select \"product_name\" from downloader_history where satellite_id = %(sat_id)s :: smallint and
#                                                                       site_id = %(site_id)s :: smallint and
#                                                                       (status_id != %(status_failed)s ::smallint and status_id != %(status_downloading)s ::smallint) """, {
#                                                                           "sat_id" : satelliteId,
#                                                                           "site_id" : currentAOI.siteId,
#                                                                           "status_failed" : DATABASE_DOWNLOADER_STATUS_FAILED_VALUE,
#                                                                           "status_downloading" : DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE
#                                                                       })
#                        if self.cursor.rowcount > 0:
#                            result = self.cursor.fetchall()
#                            for res in result:
#                                if len(res) == 1:
#                                    currentAOI.appendHistoryFile(res[0])
#                        self.cursor.execute("select * from sp_get_site_tiles({} :: smallint, {})".format(currentAOI.siteId, satelliteId))
#                        if self.cursor.rowcount > 0:
#                            result = self.cursor.fetchall()
#                            for res in result:
#                                if len(res) == 1:
#                                    currentAOI.appendTile(res[0])
#                    except Exception, e:
#                        print("exception = {}".format(e))
#                        print("Error in getting the downloaded files")
#                        dbHandler = False
#                        self.databaseDisconnect()
#                if dbHandler:
#                    retArray.append(currentAOI)
#
#        self.databaseDisconnect()
#        return retArray

    def getAOINew(self, satelliteId, site_id, start_date, end_date):
        writeDirSatelliteName = ""
        if satelliteId == SENTINEL2_SATELLITE_ID:
            writeDirSatelliteName = "s2."
        elif satelliteId == LANDSAT8_SATELLITE_ID:
            writeDirSatelliteName = "l8."
        else:
            return False
        if not self.databaseConnect():
            return False
        try:
            #self.cursor.execute("select *,st_astext(geog) from site")
            self.cursor.execute("select * from sp_downloader_get_sites()")
            rows = self.cursor.fetchall()
        except:
            self.databaseDisconnect()
            return []
        # retArray will be a list of AOIContext
        retArray = []
        for row in rows:
            if len(row) == 4 and row[3] != None:
                # retry in case of disconnection
                if not self.databaseConnect():
                    return False
                if site_id > 0 and site_id != int(row[0]):
                    continue
                currentAOI = AOIContext()
                currentAOI.siteId = int(row[0])
                currentAOI.siteName = row[2]
                currentAOI.polygon = row[3]

                dbHandler = True
                forced_season = False
                # First check if we neeed to override the current seasons for this site and use instead the specified start and end dates
                if start_date is not None and end_date is not None:
                    # Force only one season with the specified dates
                    currentSeasonInfo = SeasonInfo()
                    currentSeasonInfo.seasonName = "forced_season"
                    currentSeasonInfo.setStartSeasonDate(start_date)
                    currentSeasonInfo.setEndSeasonDate(end_date)
                    currentAOI.aoiSeasonInfos.append(currentSeasonInfo)
                    if site_id > 0:
                        # the offline_l1_handler app is calling this function with site_id set to -1, so don't print this message for it
                        print("Forcing manual download for time interval: {} - {}".format(start_date, end_date))
                        sys.stdout.flush()
                    forced_season = True
                else :
                    # Otherwise, read the seasons for the site from the DB
                    siteSeasonsQuery = "select * from sp_get_site_seasons({} :: smallint) where enabled = true".format(currentAOI.siteId)
                    try:
                        self.cursor.execute(siteSeasonsQuery)
                        print("Executing seasons query for site name {} returned a number of = {} rows".format(currentAOI.siteName, self.cursor.rowcount))
                        if self.cursor.rowcount > 0:
                            siteSeasonsRows = self.cursor.fetchall()
                            print("SiteSeasons={}".format(siteSeasonsRows))
                            for siteSeasonRow in siteSeasonsRows:
                                if len(siteSeasonRow) != 7:
                                    continue
                                currentSeasonInfo = SeasonInfo()
                                currentSeasonInfo.seasonId = int(siteSeasonRow[0])
                                currentSeasonInfo.seasonName = siteSeasonRow[2]
                                # set the start season date
                                currentSeasonInfo.setStartSeasonDate(siteSeasonRow[3])
                                # set the end season date
                                currentSeasonInfo.setEndSeasonDate(siteSeasonRow[4])
                                currentAOI.aoiSeasonInfos.append(currentSeasonInfo)
                    except Exception as e:
                        print("exception in query for seasons for query {}:".format(siteSeasonsQuery))
                        print("{}".format(e))
                        self.databaseDisconnect()
                        dbHandler = False
                        break

                # If no seasons defined, there is no need to continue
                if (len(currentAOI.aoiSeasonInfos) == 0 ) :
                    print("No active seasons defined for site name {}. It will be ignored!".format(currentAOI.siteName))
                    continue

                # Now get the downloader parameters from the config table (the max cloud coverage, the max retries and write directories for each satellite)
                baseQuery = "select * from sp_get_parameters(\'downloader."
                whereQuery = "where \"site_id\"="
                suffixArray = ["start.offset\')", "max-cloud-coverage\')", "{}max-retries')".format(writeDirSatelliteName), "{}write-dir\')".format(writeDirSatelliteName)]
                configArray = []
                for suffix in suffixArray:
                    baseQuerySite = "{}{}".format(baseQuery, suffix)
                    query = "{} {} {}".format(baseQuerySite, whereQuery, currentAOI.siteId)
                    #print("query with where={}".format(query))
                    baseQuerySite += " where \"site_id\" is null"
                    try:
                        self.cursor.execute(query)
                        if self.cursor.rowcount <= 0:
                            self.cursor.execute(baseQuerySite)
                            if self.cursor.rowcount <= 0:
                                print("Could not get even the default value for downloader.{}".format(suffix))
                                dbHandler = False
                                break
                            if self.cursor.rowcount != 1:
                                print("More than 1 result from the db for downloader.{}".format(suffix))
                                dbHandler = False
                                break
                            result = self.cursor.fetchall()
                            #print("result={}".format(result))
                            configArray.append(result[0][2])
                        else:
                            result = self.cursor.fetchall()
                            #print("result={}".format(result))
                            configArray.append(result[0][2])
                    except Exception as e:
                        print("exception in query for downloader.{}:".format(suffix))
                        print("{}".format(e))
                        self.databaseDisconnect()
                        dbHandler = False
                        break
                if dbHandler:
                    if not configArray[-1].endswith("/"):
                        configArray[-1] += "/"
                    configArray[-1] += currentAOI.siteName
                    if not currentAOI.setConfigParams(configArray):
                        print("OUT OF THE SEASON")
                        sys.stdout.flush()
                        continue
                    try:
                        self.cursor.execute("""select \"product_name\" from downloader_history where satellite_id = %(sat_id)s :: smallint and
                                                                       site_id = %(site_id)s :: smallint and
                                                                       (status_id != %(status_failed)s ::smallint and status_id != %(status_downloading)s ::smallint) """, {
                                                                           "sat_id" : satelliteId,
                                                                           "site_id" : currentAOI.siteId,
                                                                           "status_failed" : DATABASE_DOWNLOADER_STATUS_FAILED_VALUE,
                                                                           "status_downloading" : DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE
                                                                       })
                        if self.cursor.rowcount > 0:
                            result = self.cursor.fetchall()
                            for res in result:
                                if len(res) == 1:
                                    currentAOI.appendHistoryFile(res[0])
                        self.cursor.execute("select * from sp_get_site_tiles({} :: smallint, {})".format(currentAOI.siteId, satelliteId))
                        if self.cursor.rowcount > 0:
                            result = self.cursor.fetchall()
                            for res in result:
                                if len(res) == 1:
                                    currentAOI.appendTile(res[0])
                    except Exception as e:
                        print("exception = {}".format(e))
                        print("Error in getting the downloaded files")
                        dbHandler = False
                        self.databaseDisconnect()
                if dbHandler:
                    retArray.append(currentAOI)

        self.databaseDisconnect()
        return retArray

    def upsertProductHistory(self, siteId, satelliteId, productName, status, productDate, fullPath, orbit_id, maxRetries):
        if not self.databaseConnect():
            print("upsertProductHistory could not connect to DB")
            return False
        try:
            #see if the record does already exist in db
            self.cursor.execute("""SELECT id, status_id, no_of_retries, created_timestamp FROM downloader_history
                                WHERE site_id = %(site_id)s and
                                satellite_id = %(satellite_id)s and
                                product_name = %(product_name)s""",
                                {
                                    "site_id" : siteId,
                                    "satellite_id" : satelliteId,
                                    "product_name" : productName
                                })
            rows = self.cursor.fetchall()
            if len(rows) > 1:
                print("upsertProductHistory error: the select for product {} returned more than 1 entry. Illegal, should be only 1 entry in downloader_history table".format(productName))
                self.databaseDisconnect()
                return False
            if len(rows) == 0:
                #if it doesn't exist, simply insert it with the provided info
                self.cursor.execute("""INSERT INTO downloader_history (site_id, satellite_id, product_name, full_path, status_id, no_of_retries, product_date, orbit_id) VALUES (
                                    %(site_id)s :: smallint,
                                    %(satellite_id)s :: smallint,
                                    %(product_name)s,
                                    %(full_path)s,
                                    %(status_id)s :: smallint,
                                    %(no_of_retries)s :: smallint,
                                    %(product_date)s :: timestamp,
                                    %(orbit_id)s :: integer)""",
                                    {
                                        "site_id" : siteId,
                                        "satellite_id" : satelliteId,
                                        "product_name" : productName,
                                        "full_path" : fullPath,
                                        "status_id" : status,
                                        "no_of_retries" : 1,
                                        "product_date" : productDate,
                                        "orbit_id" : orbit_id
                                    })
            else:
                #if the record for this product name does exist, act accordingyl the provided status
                if len(rows[0]) != 4:
                    print("DB result from 'SELECT id, status_id, no_of_retries, created_timestamp FROM downloader_history....' query has more than 4 fields !")
                    self.databaseDisconnect()
                    return False
                db_l1c_id = rows[0][0]
                db_status_id = rows[0][1]
                db_no_of_retries = rows[0][2]
                db_created_timestamp = rows[0][3]
                #for the following values, only the status will be updated
                if status == DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE or \
                status == DATABASE_DOWNLOADER_STATUS_DOWNLOADED_VALUE or \
                status == DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE or \
                status == DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE or \
                status == DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE:
                    self.cursor.execute("""UPDATE downloader_history SET status_id = %(status_id)s :: smallint
                                        WHERE id = %(l1c_id)s :: integer """,
                                        {
                                            "status_id" : status,
                                            "l1c_id" : db_l1c_id
                                        })
                #if the failed status is provided , update it in the table if the no_of_retries
                #does not exceed maxRetries, otherwise set the status as aborted and forget about it
                elif status == DATABASE_DOWNLOADER_STATUS_FAILED_VALUE:
                    if db_no_of_retries >= maxRetries:
                        status = DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE
                    else:
                        #no of retries means a certain amount of time (TIME_INTERVAL_RETRY hours for example) used for trying to download the product
                        #after this amount of time passed, the number of retries will be incremented
                        #note: db_created_timestamp is an 'python offset-naive datetime' (this is how PG returns)
                        #so it should be converted to 'python offset-aware datetime'
                        #only in this way the datetime can be added with TIME_INTERVAL_RETRY * no_of_retries hours. this is done through '.replace(tzinfo=None)'
                        db_product_timestamp_to_check = (db_created_timestamp + datetime.timedelta(hours=(db_no_of_retries * TIME_INTERVAL_RETRY))).replace(tzinfo=None)
                        now = datetime.datetime.now()
                        if db_product_timestamp_to_check <= now:
                            db_no_of_retries += 1
                    self.cursor.execute("""UPDATE downloader_history SET status_id = %(status_id)s :: smallint , no_of_retries = %(no_of_retries)s :: smallint
                                        WHERE id = %(l1c_id)s :: integer """,
                                        {
                                            "status_id" : status,
                                            "no_of_retries" : db_no_of_retries,
                                            "l1c_id" : db_l1c_id
                                        })
                else:
                    self.databaseDisconnect()
                    print("The provided status {} is not one of the known status from DB. Check downloader_status table !".format(status))
                    return False
            self.conn.commit()
        except Exception as e:
            print("The query for product {} raised an exception {} !".format(productName, e))
            self.databaseDisconnect()
            return False
        self.databaseDisconnect()
        return True


    def updateHistory(self, siteId, satelliteId, productName, productDate, fullPath):
        if not self.databaseConnect():
            return False
        try:
            print("UPDATING: insert into downloader_history (\"site_id\", \"satellite_id\", \"product_name\", \"product_date\", \"full_path\") VALUES ({}, {}, '{}', '{}', '{}')".format(siteId, satelliteId, productName, productDate, fullPath))
            self.cursor.execute("""insert into downloader_history (site_id, satellite_id, product_name, product_date, full_path) VALUES (
                                    %(site_id)s :: smallint,
                                    %(satellite_id)s :: smallint,
                                    %(product_name)s,
                                    %(product_date)s :: timestamp,
                                    %(full_path)s)""", {
                                        "site_id" : siteId,
                                        "satellite_id" : satelliteId,
                                        "product_name" : productName,
                                        "product_date": productDate,
                                        "full_path" : fullPath
                                    })
            self.conn.commit()
        except:
            print("DATABASE INSERT query FAILED!!!!!")
            self.databaseDisconnect()
            return False
        self.databaseDisconnect()
        return True


###########################################################################
class SentinelAOIInfo(AOIInfo):
    def __init__(self, serverIP, databaseName, user, password, logFile=None):
        AOIInfo.__init__(self, serverIP, databaseName, user, password, logFile)

    def getSentinelAOI(self, site_id, start_date, end_date):
        return self.getAOINew(SENTINEL2_SATELLITE_ID, site_id, start_date, end_date)

    #def updateSentinelHistory(self, siteId, productName, productDate, fullPath):
    #    return self.updateHistory(siteId, SENTINEL2_SATELLITE_ID, productName, productDate, fullPath)

    def upsertSentinelProductHistory(self, siteId, productName, status, productDate, fullPath, orbit_id, maxRetries = 0):
        return self.upsertProductHistory(siteId, SENTINEL2_SATELLITE_ID, productName, status, productDate, fullPath, orbit_id, maxRetries)


###########################################################################
class LandsatAOIInfo(AOIInfo):
    def __init__(self, serverIP, databaseName, user, password, logFile=None):
        AOIInfo.__init__(self, serverIP, databaseName, user, password, logFile)

    def getLandsatAOI(self, site_id, start_date, end_date):
        return self.getAOINew(LANDSAT8_SATELLITE_ID, site_id, start_date, end_date)

    #def updateLandsatHistory(self, siteId, productName, productDate, fullPath):
    #    return self.updateHistory(siteId, LANDSAT8_SATELLITE_ID, productName, productDate, fullPat)h

    def upsertLandsatProductHistory(self, siteId, productName, status, productDate, fullPath, maxRetries = 0):
        return self.upsertProductHistory(siteId, LANDSAT8_SATELLITE_ID, productName, status, productDate, fullPath, -1, maxRetries)


###########################################################################
class L1CInfo(object):
    def __init__(self, server_ip, database_name, user, password, log_file=None):
        self.server_ip = server_ip
        self.database_name = database_name
        self.user = user
        self.password = password
        self.is_connected = False;
        self.log_file = log_file

    def database_connect(self):
        if self.is_connected:
            print("{}: Database is already connected...".format(threading.currentThread().getName()))
            return True
        connectString = "dbname='{}' user='{}' host='{}' password='{}'".format(self.database_name, self.user, self.server_ip, self.password)
        try:
            self.conn = psycopg2.connect(connectString)
            self.cursor = self.conn.cursor()
            self.is_connected = True
        except:
            print("Unable to connect to the database")
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # Exit the script and print an error telling what happened.
            print("Database connection failed!\n ->{}".format(exceptionValue))
            self.is_connected = False
            return False
        #print("{}: Database connected...".format(threading.currentThread().getName()))
        return True

    def database_disconnect(self):
        if self.conn:
            self.conn.close()
            self.is_connected = False
            #print("{}: Database disconnected...".format(threading.currentThread().getName()))

    def to_bool(self, value):
        valid = {'true': True, 't': True, '1': True,
                 'false': False, 'f': False, '0': False,
                }   

        if isinstance(value, bool):
            return value

        if not isinstance(value, basestring):
            raise ValueError('invalid literal for boolean. Not a string.')

        lower_value = value.lower()
        if lower_value in valid:
            return valid[lower_value]
        else:
            return False
