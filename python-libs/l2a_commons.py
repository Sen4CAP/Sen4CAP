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
import errno
import random
import string
import logging

DEBUG = False
SENTINEL2_SATELLITE_ID = 1
LANDSAT8_SATELLITE_ID = 2
UNKNOWN_PROCESSOR_OUTPUT_FORMAT = 0
MACCS_PROCESSOR_OUTPUT_FORMAT = 1
THEIA_MUSCATE_OUTPUT_FORMAT = 2
SEN2COR_PROCESSOR_OUTPUT_FORMAT = 3
FILES_IN_LANDSAT_L1_PRODUCT = 13
UNKNOWN_SATELLITE_ID = -1
DEFAULT_GDAL_IMAGE = "osgeo/gdal:ubuntu-full-3.2.0"
DL = "DEBUG_LOG_LEVEL"
IL = "INFO_LOG_LEVEL"
WL = "WARNING_LOG_LEVEL"
EL = "ERROR_LOG_LEVEL"
CL = "CRITICAL_LOG_LEVEL"
MASTER_ID = -1
NO_ID = -2

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

    
class Log():
    def __init__(self, log_path, name, level):
        self.log_path = log_path
        self.level = level
        self.name = name
        self.formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt = '%Y.%m.%d.%H.%M.%S,§%z')
        self.handler = logging.FileHandler(self.log_path, "a")        
        self.handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(self.name)
        if self.level == DL:
            log_level = logging.DEBUG
        elif self.level == IL:
            log_level = logging.INFO
        elif self.level == WL:
            log_level = logging.WARNING
        elif self.level == EL:
            log_level = logging.ERROR
        elif self.level == CL:
            log_level = logging.CRITICAL
        else:
            log_level = logging.DEBUG
        self.logger.setLevel(self.log_level)
        self.logger.addHandler(self.handler)

    def write(self, level, msg, writer_id, print_msg = False, trace = False):
        
        if writer_id == NO_ID:
            pass
        elif writer_id == MASTER_ID:
            msg = "<master> " + msg
        else:
            msg = "<worker {}> ".format(self.writer_id) + msg
        if level == DL:
            self.logger.debug(msg)
            if print_msg: print(msg)
        elif level == IL:
            self.logger.info(msg)
            if print_msg: print(msg)
        elif level == WL:
            self.logger.warning(msg)
            if print_msg: print(msg)
        elif level == EL:
            self.logger.error(msg, exc_info = trace)
            if print_msg: print(msg)
        elif level == CL:
            self.logger.critical(msg, exc_info = trace)
            if print_msg: print(msg)
        else:
            pass 


def run_command(cmd_array, log, fake_command = False):
    start = time.time()
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    res = 0
    if not fake_command:
        log.write(IL, "Running command: {}".format(cmd_str), print_msg = True)
        try:
            with open(log.log_path, "a") as log_file:
                res = subprocess.call(cmd_array, stdout = log_file, stderr = log_file, shell=False)
        except Exception as e:
            log.write(
                EL,
                "Exception encountered: {} when running command: {}".format(e, cmd_str),
                print_msg = True,
                trace = True
            )
            res = 1
    else:
        log.write(DL, "Fake command: {}".format(cmd_str), print_msg = True)
    ok = "OK"
    nok = "NOK"
    log.write(
        IL,
        "Command finished {} (res = {}) in {} : {}".format((ok if res == 0 else nok), res, datetime.timedelta(seconds=(time.time() - start)), cmd_str),
        print_msg = True
    )
    return res


def create_recursive_dirs(dir_path):
    try:
        os.makedirs(dir_path)
    except Exception as e:
        if e.errno != errno.EEXIST:
            print(e)
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

#reads the first line of a file
def read_1st(file):
    with open(file) as f:
        return f.readline().strip("\n")

def get_node_id(): 
    host = read_1st("/etc/hostname")
    machine_id = read_1st("/etc/machine-id")
    return host + "-" + machine_id

def get_guid(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def stop_containers(container_list, log):
    if container_list:
        #stopping running containers
        cmd = []
        cmd.append("docker")
        cmd.append("stop")
        cmd.extend(container_list)
        print("\nStoping running containers")
        run_command(cmd, log)

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

def translate(input_img,
              output_dir,
              output_img_name,
              output_img_format,
              log,
              gdal_image = DEFAULT_GDAL_IMAGE,
              name = None,
              resample_res = 0,
              compress = False,
              outsize = 0
        ):
            cmd = []
            #docker run
            cmd.append("docker")
            cmd.append("run")
            cmd.append("--rm")
            cmd.append("-u")
            cmd.append("{}:{}".format(os.getuid(), os.getgid()))
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(input_img), os.path.abspath(input_img)))
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(output_dir), os.path.abspath(output_dir)))
            log_file = os.path.join(log_dir, log_file_name)
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(log_file), os.path.abspath(log_file)))
            if name:
                cmd.append("--name")
                cmd.append(name)
            cmd.append(gdal_image)

            # gdal command
            cmd.append("gdal_translate")
            cmd.append("-of")
            cmd.append(output_img_format)
            if compress:
                cmd.append("-co")
                cmd.append("COMPRESS=DEFLATE")
            if output_img_format != "JPEG":
                cmd.append("-co")
                cmd.append("NUM_THREADS=ALL_CPUS")
                cmd.append("-co")
                cmd.append("PREDICTOR=2")
            if resample_res > 0:
                cmd.append("-tr")
                cmd.append(str(resample_res))
                cmd.append(str(resample_res))
            if outsize > 0:
                cmd.append("-outsize")
                cmd.append(str(outsize))
                cmd.append("0") #presevers the ratio of file
            cmd.append(input_img)
            output_img= os.path.join(output_dir, output_img_name)
            cmd.append(output_img)

            cmd_ret = run_command(cmd, log)

            if (cmd_ret == 0) and os.path.isfile(output_img):
                return True
            else:
                return False

            return True

### Archiving operations

class ArchiveHandler(object):
    def __init__(self, archives_dir, log):
        self.archives_dir= archives_dir
        self.log = log

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
        self.log.write(IL, "Unzip archive = {} to {}".format(input_file, output_dir))
        try:
            with zipfile.ZipFile(input_file) as zip_archive:
                zip_archive.extractall(output_dir)
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.log.write(
                EL,
                "Exception when trying to unzip file {}:  {} ".format(input_file, e),
                trace = True
            )

        return None

    def untar(self, output_dir, input_file):
        self.log.write(IL, "Untar archive = {} to {}".format(input_file, output_dir))
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
            self.log.write(
                IL,
                "This {} wasn't an archive, so continue as is.".format(archive_file)
            )
            return False, archive_file
        else:
            if zipfile.is_zipfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.unzip(extracted_archive_dir, archive_file)
                        self.log.write(
                            IL,
                            "Archive extracted to: {}".format(extracted_file_path)
                        )
                        return True, extracted_file_path
                    except Exception as e:
                        self.log.write(
                            EL,
                            "Can NOT extract zip archive {} due to: {}".format(archive_file, e),
                            trace = True
                        )
                        return False, None
                else:
                    self.log.write(
                        EL,
                        "Can NOT create arhive dir."
                    )
                    return False, None
            elif tarfile.is_tarfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.untar(extracted_archive_dir, archive_file)
                        self.log.write(
                            IL,
                            "Archive extracted to: {}".format(extracted_file_path)
                        )
                        return True, extracted_file_path
                    except Exception as e:
                        self.log.write(
                            EL, 
                            "Can NOT extract tar archive {} due to: {}".format(archive_file, e),
                            trace = True
                        )
                        return False, None
                else:
                    self.log.write(
                        EL, 
                        "Can NOT create arhive dir."
                    )
                    return False, None
            else:
                self.log.write(
                    EL,
                    "This wasn't an zip or tar archive, can NOT use input product."
                )
                return False, None

