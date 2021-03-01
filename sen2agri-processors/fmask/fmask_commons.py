# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen2Agri-Processors
   Language:     Python
   Copyright:    2015-2021, CS Romania, office@c-s.ro
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
FMASK_LOG_DIR = "/tmp/"
FMASK_LOG_FILE_NAME = "fmask.log"
DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = 1
DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = 2
DATABASE_DOWNLOADER_STATUS_FAILED_VALUE = 3
DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE = 4
DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE = 5
DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE = 6
MAX_LOG_FILE_SIZE = 419430400 #bytes -> 400 MB
MAX_NUMBER_OF_KEPT_LOG_FILES = 4 #number of maximum logfiles to be kept

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
    if os.path.isdir(directory):
        try:
            shutil.rmtree(directory)
        except Exception as e:
            print("Can not remove directory {} due to: {}.".format(directory, e))
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


















