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
import shutil
from osgeo import osr, gdal
import ntpath
import zipfile
import tarfile
import tempfile
import errno
import random
import string
import logging
import time
import datetime
import pipes

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

    
class LogHandler(object):
    def __init__(self, log_path, name, level, emitter_id):
        self.path = log_path
        self.level = level
        self.name = name
        self.emitter_id = emitter_id
        self.formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt = '%Y.%m.%d-%H:%M:%S')
        self.handler = logging.FileHandler(self.path, mode = "a")        
        self.handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(self.name)
        if self.level == 'debug':
            log_level = logging.DEBUG
        elif self.level == 'info':
            log_level = logging.INFO
        elif self.level == 'warning':
            log_level = logging.WARNING
        elif self.level == 'error':
            log_level = logging.ERROR
        elif self.level == 'critical':
            log_level = logging.CRITICAL
        else:
            log_level = logging.DEBUG
        self.logger.setLevel(log_level)
        while self.logger.handlers:
            self.logger.handlers.pop()
        self.logger.addHandler(self.handler)
        self.logger.propagate = False

    def __del__(self):
        self.close()
    
    def close(self):
        self.handler.close()
        self.logger.removeHandler(self.handler)

    def format_msg(self, msg):
        if self.emitter_id == NO_ID:
            return msg
        elif self.emitter_id == MASTER_ID:
            return "<master> " + msg
        else:
            return "<worker {}> ".format(self.emitter_id) + msg

    def debug(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.debug(log_msg, exc_info = trace)

    def info(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.info(log_msg, exc_info = trace)

    def warning(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.warning(log_msg, exc_info = trace)

    def error(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.error(log_msg, exc_info = trace)

    def critical(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.critical(log_msg, exc_info = trace)


def run_command(cmd_array, log):
    try:
        with open(log.path, "a") as log_file:
            res = subprocess.call(cmd_array, stdout = log_file, stderr = log_file, shell=False)
    except Exception as e:
        log.error(
            "Exception encountered: {}".format(e),
            print_msg = True,
            trace = True
        )
        res = 1

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
        if e.errno == errno.ENOENT:
            return True
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

### Docker related operations

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
        cmd_str = " ".join(map(pipes.quote, cmd))
        log.info("Runnning command: " + cmd_str)
        start_time = time.time()
        ret = run_command(cmd, log)
        end_time = time.time()
        log.info(
            "Command {} finished with return code {} in {}".format(cmd_str, ret, datetime.timedelta(seconds=(end_time - start_time))),
            print_msg = True
        )

def get_docker_gid():
    docker_status = os.stat("/var/run/docker.sock")
    if docker_status:
        docker_gid = docker_status.st_gid
        return docker_gid
    else:
        return None


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
            cmd.append("/etc/localtime:/etc/localtime")
            cmd.append("-v")
            cmd.append("/usr/share/zoneinfo:/usr/share/zoneinfo")
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(input_img), os.path.abspath(input_img)))
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(output_dir), os.path.abspath(output_dir)))
            cmd.append("-v")
            cmd.append("{}:{}".format(os.path.abspath(log.path), os.path.abspath(log.path)))
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
            
            cmd_str = " ".join(map(pipes.quote, cmd))
            log.info("Running command: " + cmd_str, print_msg = True)
            start_time = time.time()
            cmd_ret = run_command(cmd, log)
            end_time = time.time()
            log.info(
                "Command {} finished with return code {} in {}".format(cmd_str, cmd_ret, datetime.timedelta(seconds=(end_time - start_time))),
                print_msg = True
            )
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
        self.log.info("Unzip archive = {} to {}".format(input_file, output_dir))
        try:
            with zipfile.ZipFile(input_file) as zip_archive:
                zip_archive.extractall(output_dir)
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.log.error(
                "Exception when trying to unzip file {}:  {} ".format(input_file, e),
                trace = True
            )

        return None

    def untar(self, output_dir, input_file):
        self.log.info("Untar archive = {} to {}".format(input_file, output_dir))
        try:
            with tarfile.open(input_file) as tar_archive:
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar_archive, output_dir)
                tar_archive.close()
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.log.error(
                "Exception when trying to untar file {}:  {} ".format(input_file, e),
                trace = True
            )

        return None

    def extract_from_archive_if_needed(self, archive_file):
        if os.path.isdir(archive_file):
            self.log.info(
                "This {} wasn't an archive, so continue as is.".format(archive_file)
            )
            return False, archive_file
        else:
            if zipfile.is_zipfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.unzip(extracted_archive_dir, archive_file)
                        self.log.info(
                            "Archive extracted to: {}".format(extracted_file_path)
                        )
                        return True, extracted_file_path
                    except Exception as e:
                        self.log.error(
                            "Can NOT extract zip archive {} due to: {}".format(archive_file, e),
                            trace = True
                        )
                        return False, None
                else:
                    self.log.error("Can NOT create arhive dir.")
                    return False, None
            elif tarfile.is_tarfile(archive_file):
                if create_recursive_dirs(self.archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=self.archives_dir)
                        extracted_file_path = self.untar(extracted_archive_dir, archive_file)
                        self.log.info(
                            "Archive extracted to: {}".format(extracted_file_path)
                        )
                        return True, extracted_file_path
                    except Exception as e:
                        self.log.error(
                            "Can NOT extract tar archive {} due to: {}".format(archive_file, e),
                            trace = True
                        )
                        return False, None
                else:
                    self.log.error("Can NOT create arhive dir.")
                    return False, None
            else:
                self.log.error("This wasn't an zip or tar archive, can NOT use input product.")
                return False, None

