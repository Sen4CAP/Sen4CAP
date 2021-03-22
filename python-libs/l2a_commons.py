#!/usr/bin/env python
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
import gdal
import ntpath
import zipfile
import tarfile
import tempfile

DEBUG = False
SENTINEL2_SATELLITE_ID = 1
LANDSAT8_SATELLITE_ID = 2
UNKNOWN_PROCESSOR_OUTPUT_FORMAT = 0
MACCS_PROCESSOR_OUTPUT_FORMAT = 1
THEIA_MUSCATE_OUTPUT_FORMAT = 2
SEN2COR_PROCESSOR_OUTPUT_FORMAT = 3
FILES_IN_LANDSAT_L1_PRODUCT = 13
UNKNOWN_SATELLITE_ID = -1

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


### IMG related operations

def ReprojectCoords(coords, src_srs, tgt_srs):
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

### Archiving operations

class ArchiveHandler(object):
    def __init__(self, archives_dir, log_dir, log_file_name):
        self.archives_dir= archives_dir
        self.log_dir = log_dir
        self.log_file_name = log_file_name

    def archive_log(self, info):
        log(self.log_dir, info, self.log_file_name)

    def path_filename(self, path):
        head, filename = ntpath.split(path)
        return filename or ntpath.basename(head)

    def check_if_flat_archive(self, output_dir, archive_filename):
        dir_content = os.listdir(output_dir)
        if len(dir_content) == 1 and os.path.isdir(
            os.path.join(output_dir, dir_content[0])
        ):
            return os.path.join(output_dir, dir_content[0])
        if len(dir_content) > 1:
            # use the archive filename, strip it from extension
            product_name, file_ext = os.path.splitext(
                self.path_filename(archive_filename)
            )
            # handle .tar.gz case
            if product_name.endswith(".tar"):
                product_name, file_ext = os.path.splitext(product_name)
            product_path = os.path.join(output_dir, product_name)
            if create_recursive_dirs(product_path):
                # move the list to this directory:
                for name in dir_content:
                    shutil.move(
                        os.path.join(output_dir, name), os.path.join(product_path, name)
                    )
                return product_path

        return None

    def unzip(self, output_dir, input_file):
        self.archive_log("Unzip archive = {} to {}".format(input_file, output_dir))
        try:
            with zipfile.ZipFile(input_file) as zip_archive:
                zip_archive.extractall(output_dir)
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.archive_log(
                "Exception when trying to unzip file {}:  {} ".format(input_file, e)
            )

        return None

    def untar(self, output_dir, input_file):
        self.archive_log("Untar archive = {} to {}".format(input_file, output_dir))
        try:
            with tarfile.open(input_file) as tar_archive:
                tar_archive.extractall(output_dir)
                tar_archive.close()
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.launcher_log(
                "Exception when trying to untar file {}:  {} ".format(input_file, e)
            )

        return None

    def extract_from_archive_if_needed(self, archive_file):
        if os.path.isdir(archive_file):
            self.archive_log(
                "This {} wasn't an archive, so continue as is.".format(archive_file)
            )
            return False, archive_file
        else:
            if zipfile.is_zipfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.unzip(extracted_archive_dir, archive_file)
                        self.archive_log("Archive extracted to: {}".format(extracted_file_path))
                        return True, extracted_file_path
                    except Exception as e:
                        self.archive_log("Can NOT extract zip archive {} due to: {}".format(archive_file, e))
                        return False, None
                else:
                    self.archive_log("Can NOT create arhive dir.")
                    return False, None
            elif tarfile.is_tarfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.untar(extracted_archive_dir, archive_file)
                        self.archive_log("Archive extracted to: {}".format(extracted_file_path))
                        return True, extracted_file_path
                    except Exception as e:
                        self.archive_log("Can NOT extract tar archive {} due to: {}".format(archive_file, e))
                        return False, None
                else:
                    self.archive_log("Can NOT create arhive dir.")
                    return False, None
            else:
                self.archive_log(
                    "This wasn't an zip or tar archive, can NOT use input product."
                )
                return False, None

