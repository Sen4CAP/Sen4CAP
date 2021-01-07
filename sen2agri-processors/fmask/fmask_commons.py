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
from __future__ import absolute_import
import subprocess
import os
import sys
import time, datetime
import pipes
import shutil
import osr
import re
import glob
import gdal

DEBUG = False
SENTINEL2_SATELLITE_ID = 1
LANDSAT8_SATELLITE_ID = 2
UNKNOWN_PROCESSOR_OUTPUT_FORMAT = 0
MACCS_PROCESSOR_OUTPUT_FORMAT = 1
THEIA_MUSCATE_OUTPUT_FORMAT = 2
SEN2COR_PROCESSOR_OUTPUT_FORMAT = 3
FILES_IN_LANDSAT_L1_PRODUCT = 13
UNKNOWN_SATELLITE_ID = -1
FMASK_LOG_DIR = "/tmp/"
FMASK_LOG_FILE_NAME = "fmask.log"
SEN2COR_LOG_DIR = "/tmp/"
SEN2COR_LOG_FILE_NAME = "sen2cor.log"
DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = 1
DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = 2
DATABASE_DOWNLOADER_STATUS_FAILED_VALUE = 3
DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE = 4
DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE = 5
DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE = 6
MAX_LOG_FILE_SIZE = 419430400 #bytes -> 400 MB
MAX_NUMBER_OF_KEPT_LOG_FILES = 4 #number of maximum logfiles to be kept


default_gdal_image_name = "osgeo/gdal:ubuntu-small-3.1.2"

### OS related operations

def remove_dir_content(directory):
    for content in os.listdir(directory):
        content_path = os.path.join(directory, content)
        try:
            if os.path.isfile(content_path):
                os.unlink(content_path)
            elif os.path.isdir(content_path): 
                shutil.rmtree(content_path)
        except Exception as e:
            print(" Can NOT remove directory content {} due to: {}.".format(directory, e))
            return False
    return True

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
    except Exception as e:
        print("Could NOT write inside the log file {} due to: {}".format(log_filename, e))


def run_command(cmd_array, log_path = "", log_filename = "", fake_command = False):
    start = time.time()
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    res = 0
    if not fake_command:
        log(log_path, "Running command: {}".format(cmd_str), log_filename)
        try:
            res = subprocess.call(cmd_array, shell=False)
        except Exception as e:
            log(log_path, "Exception encountered: {} when running command: {}".format(e, cmd_str), log_filename)
            res = 1
    else:
        log(log_path, "Fake command: {}".format(cmd_str), log_filename)
    ok = "OK"
    nok = "NOK"
    log(log_path, "Command finished {} (res = {}) in {} : {}".format((ok if res == 0 else nok), res, datetime.timedelta(seconds=(time.time() - start)), cmd_str), log_filename)
    return res

def create_recursive_dirs(dir_name):
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
        except Exception as e:
            print("The directory {} couldn't be created. Reason: {}".format(dir_name, e))
            return False

    return True

def remove_dir(directory):
    try:
        shutil.rmtree(directory)
    except Exception as e:
        print("Can not remove directory {} due to: {}.".format(directory, e))
        return False
    return True

def copy_directory(src, dest):
    try:
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

def delete_file_if_match(fullFilePath, fileName, regex, fileType) :
    isMatch = re.match(regex, fileName)
    if isMatch is not None:
        print("Deleting {} file {}".format(fileType, fullFilePath))
        os.remove(fullFilePath)

### IMG related operations

def ReprojectCoords(coords, src_srs, tgt_srs):
    #trans_coords = []
    #transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    #for x, y in coords:
    #    x, y, z = transform.TransformPoint(x, y)
    #    trans_coords.append([x, y])
    #return trans_coords

    trans_coords = []
    transform = osr.CoordinateTransformation(src_srs, tgt_srs)
    for x, y in coords:
        x, y, z = transform.TransformPoint(x, y)
        trans_coords.append([x, y])
    return trans_coords

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


















