#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen4Cap-Processors
   Language:     Python
   Copyright:    2015-2020, CS Romania, office@c-s.ro
   See COPYRIGHT file for details.

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
_____________________________________________________________________________

"""
from __future__ import print_function
import psycopg2
import sys
import argparse
import threading
import time
import random
import ogr
import os
import Queue
import ntpath
import shutil
import zipfile
import tarfile
import tempfile
import re
import grp
import glob
import subprocess
import signal
import traceback
from psycopg2.errorcodes import SERIALIZATION_FAILURE, DEADLOCK_DETECTED
from psycopg2.sql import NULL
from fmask_commons import log, create_recursive_dirs, remove_dir_content, remove_dir, delete_file_if_match, get_footprint, run_command
from fmask_commons import DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE, DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
from fmask_commons import DEBUG, FMASK_LOG_DIR, FMASK_LOG_FILE_NAME

ARCHIVES_DIR_NAME = "archives"
LAUNCHER_LOG_DIR = "/tmp/"
LAUNCHER_LOG_FILE_NAME = "fmask_launcher.log"
FMASK_EXTRACTOR = "fmask_extractor.py"
DATABASE_FMASK_OUTPUT_PATH = "fmask.output-path"
DATABASE_FMASK_WORKING_DIR = "fmask.working-dir"
DATABASE_FMASK_THRESHOLD = "fmask.threshold"
MAX_CLOUD_COVERAGE = 90.0


class L1CProduct(object):
    def __init__(self, tile):
        self.site_id = tile.site_id
        self.satellite_id = tile.satellite_id
        self.product_id = tile.downloader_history_id
        self.db_path = tile.path
        self.was_archived = False
        self.path = None
        self.rejection_reason = None
        self.should_retry = True
        self.processing_status = None

class FMaskProduct(object):
    def __init__(self):
        self.name = None
        self.output_path = ""
        self.destination_path = ""
        self.product_path = None
        self.satellite_id = None
        self.acquisition_date = None
        self.processed_tiles = []
        self.site_id = None
        self.product_id = None
        self.footprint = None
        self.basename = None

class FmaskProcessor(object):
    def __init__(self, processor_context, unprocessed_tile):
        self.context = processor_context
        self.lin = L1CProduct(unprocessed_tile)
        self.fmask = FMaskProduct()
        self.fmask_log_file = None
        self.name = "Fmask"

    def __del__(self):
        if self.lin.was_archived and os.path.exists(self.lin.path):
            remove_dir(self.lin.path)

    def fmask_log(self, log_msg):
        log_msg = "<worker {}>: ".format(self.context.worker_id) + log_msg
        log(self.fmask.output_path, log_msg, self.fmask_log_file)

    def launcher_log(self, log_msg):
        log_msg = "<worker {}>: ".format(self.context.worker_id) + log_msg
        log(LAUNCHER_LOG_DIR, log_msg, LAUNCHER_LOG_FILE_NAME)

    def get_envelope(self, footprints):
        geomCol = ogr.Geometry(ogr.wkbGeometryCollection)

        for footprint in footprints:
            for pt in footprint:
                point = ogr.Geometry(ogr.wkbPoint)
                point.AddPoint_2D(pt[0], pt[1])
                geomCol.AddGeometry(point)

        hull = geomCol.ConvexHull()
        return hull.ExportToWkt()

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
        self.launcher_log("Unzip archive = {} to {}".format(input_file, output_dir))
        try:
            with zipfile.ZipFile(input_file) as zip_archive:
                zip_archive.extractall(output_dir)
                return self.check_if_flat_archive(
                    output_dir, self.path_filename(input_file)
                )
        except Exception as e:
            self.launcher_log(
                "Exception when trying to unzip file {}:  {} ".format(input_file, e)
            )
            self.update_rejection_reason(
                "Exception when trying to unzip file {}:  {} ".format(input_file, e)
            )

        return None

    def untar(self, output_dir, input_file):
        self.launcher_log("Untar archive = {} to {}".format(input_file, output_dir))
        try:
            tar_archive = tarfile.open(input_file)
            tar_archive.extractall(output_dir)
            tar_archive.close()
            return self.check_if_flat_archive(
                output_dir, self.path_filename(input_file)
            )
        except Exception as e:
            self.launcher_log(
                "Exception when trying to untar file {}:  {} ".format(input_file, e)
            )
            self.update_rejection_reason(
                "Exception when trying to untar file {}:  {} ".format(input_file, e)
            )

        return None

    def extract_from_archive_if_needed(self, archive_file):
        if os.path.isdir(archive_file):
            self.launcher_log(
                "This wasn't an archive, so continue as is."
            )
            return False, archive_file
        else:
            archives_dir = os.path.join(self.context.working_dir, ARCHIVES_DIR_NAME)
            if zipfile.is_zipfile(archive_file):
                if create_recursive_dirs(archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=archives_dir)
                        extracted_file_path = self.unzip(extracted_archive_dir, archive_file)
                        self.launcher_log("Archive extracted to: {}".format(extracted_file_path))
                        return True, extracted_file_path
                    except Exception as e:
                        self.launcher_log("Can NOT extract zip archive {} due to: {}".format(archive_file, e))
                        return False, None
                else:
                    self.launcher_log("Can NOT create arhive dir.")
                    return False, None
            elif tarfile.is_tarfile(archive_file):
                if create_recursive_dirs(archives_dir):
                    try:
                        extracted_archive_dir = tempfile.mkdtemp(dir=archives_dir)
                        extracted_file_path = self.untar(extracted_archive_dir, archive_file)
                        self.launcher_log("Archive extracted to: {}".format(extracted_file_path))
                        return True, extracted_file_path
                    except Exception as e:
                        self.launcher_log("Can NOT extract tar archive {} due to: {}".format(archive_file, e))
                        return False, None
                else:
                    self.launcher_log("Can NOT create arhive dir.")
                    return False, None
            else:
                self.launcher_log(
                    "This wasn't an zip or tar archive, can NOT use input product."
                )
                return False, None

    def validate_input_product_dir(self):
        # First check if the product path actually exists
        try:
            os.stat(self.lin.path)
        except OSError as e:
            self.update_rejection_reason(
                "Can NOT check if product root dir path {} exists or it is a valid symlink. Error was: {}".format(
                    self.lin.path, e.errno
                )
            )
            return False

        for root, subdirs, files in os.walk(self.lin.path):
            for subdir in subdirs:
                subdir_path = os.path.join(root, subdir)
                try:
                    os.stat(subdir_path)
                except OSError as e:
                    self.update_rejection_reason(
                        "Cannot check if dir path {} exists or it is a valid symlink. Error was: {}".format(
                            subdir_path, e.errno
                        )
                    )
                    return False

            for filename in files:
                file_path = os.path.join(root, filename)
                try:
                    os.stat(file_path)
                except OSError as e:
                    self.update_rejection_reason(
                        "Cannot check if file path {} exists or is a valid symlink. Error was: {}".format(
                            file_path, e.errno
                        )
                    )
                    return False

        return True

    def check_lin(self):

        self.launcher_log(
            "{}: Starting extract_from_archive_if_needed for product {}".format(
                self.context.worker_id, self.lin.product_id
            )
        )
        self.lin.was_archived, self.lin.path = self.extract_from_archive_if_needed(
            self.lin.db_path
        )
        self.launcher_log(
            "{}: Ended extract_from_archive_if_needed for product {}".format(
                self.context.worker_id, self.lin.product_id
            )
        )
        if self.lin.path is not None:
            return self.validate_input_product_dir()
        else:
            return False

    def fmask_setup(self):
        # determine the name of the fmask output dir
        name_determined = True
        if self.lin.path.endswith("/"):
            lin_basename = os.path.basename(self.lin.path[:-1])
        else:
            lin_basename = os.path.basename(self.lin.path)
        if lin_basename.startswith("S2"):
            fmask_basename = lin_basename.replace("L1C", "FMASK")
        elif lin_basename.startswith("LC8"):
            fmask_basename = lin_basename + "_FMASK"
        elif lin_basename.startswith("LC08"):
            if lin_basename.find("_L1TP_") > 0:
                fmask_basename = lin_basename.replace("_L1TP_", "_FMASK_")
            elif lin_basename.find("_L1GS_") > 0:
                fmask_basename = lin_basename.replace("_L1GS_", "_FMASK_")
            elif lin_basename.find("_L1GT_") > 0:
                fmask_basename = lin_basename.replace("_L1GT_", "_FMASK_")
            else:
                self.update_rejection_reason(
                    "The input product name is wrong - Fmask cannot be filled: {}".format(
                        lin_basename
                    )
                )
                self.launcher_log(
                    "The input product name is wrong - Fmask cannot be filled: {}".format(
                        lin_basename
                    )
                )
                return False
        else:
            self.update_rejection_reason(
                "The input product name is wrong: {}".format(lin_basename)
            )
            self.launcher_log(
                "The input product name is wrong: {}".format(lin_basename)
            )
            return False
        self.fmask.basename = fmask_basename

        # determine the acq date
        if lin_basename.startswith("S2"):
            result = re.findall(r"_\d{8}T\d{6}_", lin_basename)
            if result:
                acq_date = result[0].strip("_").split("T")[0]
                acq_year = acq_date[:4]
                acq_month = acq_date[4:6]
                acq_day = acq_date[6:]
            else:
                self.update_rejection_reason(
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                self.launcher_log(
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                return False
        elif lin_basename.startswith("LC"):
            result = re.findall(r"_\d{8}_", lin_basename)
            if result:
                acq_date = result[0].strip("_")
                acq_year = acq_date[:4]
                acq_month = acq_date[4:6]
                acq_day = acq_date[6:]
            else:
                self.update_rejection_reason(
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                self.launcher_log(
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                return False
        else:
            self.update_rejection_reason(
                "Can NOT obtain the aquisition date on input product: {}".format(
                    lin_basename
                )
            )
            self.launcher_log(
                "Can NOT obtain the aquisition date on input product: {}".format(
                    lin_basename
                )
            )
            return False

        # determine the path of the fmask product
        fmask_output_path = os.path.join(
            self.context.output_path, "output", fmask_basename
        )
        fmask_destination_path = os.path.join(
            self.context.output_path, acq_year, acq_month, acq_day, fmask_basename
        )
        if not create_recursive_dirs(fmask_output_path):
            self.update_rejection_reason(
                "Can NOT create the output directory: {}".format(fmask_output_path)
            )
            self.launcher_log(
                "Can NOT create the output directory: {}".format(fmask_output_path)
            )
            return False

        self.fmask.output_path = fmask_output_path
        self.fmask.destination_path = fmask_destination_path
        self.fmask.satellite_id = self.lin.satellite_id
        self.fmask.site_id = self.lin.site_id
        self.fmask.product_id = self.lin.product_id
        self.fmask_log_file = "fmask.log"
        return True

    def update_rejection_reason(self, message):
        messages_separator = "\n "
        if self.lin.rejection_reason is None:
            self.lin.rejection_reason = message
        else:
            self.lin.rejection_reason = (
                self.lin.rejection_reason + messages_separator + message
            )

    def move_to_destination(self):
        #Copies a valid product from the output path to the destination product path
        try:
            if self.fmask.destination_path.endswith("/"):
                dst = os.path.dirname(self.fmask.destination_path[:-1])
            else:
                dst = os.path.dirname(self.fmask.destination_path)
            if not os.path.isdir(dst):
                create_recursive_dirs(dst)
            os.rename(self.fmask.output_path, self.fmask.destination_path)
        except Exception as e:
            self.update_rejection_reason(" Can NOT copy from output path {} to destination product path {} due to: {}".format(self.fmask.output_path, self.fmask.destination_path, e))

    def get_fmask_footprint(self, output_path) :
        footprint_file_pattern = "*_Fmask4.tif"
        footprint_file_path = os.path.join(output_path,footprint_file_pattern)
        footprint_files = glob.glob(footprint_file_path)
        if len(footprint_files) > 0:
            wgs84_extent_list = []
            wgs84_extent_list.append(get_footprint(footprint_files[0])[0])
            self.fmask.footprint = self.get_envelope(wgs84_extent_list)
            return True
        else:
            self.update_rejection_reason(
                "Can NOT create the footprint, no Fmask4 tif file exists."
            )
            self.fmask_log("Can NOT create the footprint, no Fmask4 tif file exists.")
            return False

    def run_script(self):

        script_command = []
        #docker run
        script_command.append("docker")
        script_command.append("run")
        script_command.append("-v")
        script_command.append("/var/run/docker.sock:/var/run/docker.sock")
        script_command.append("--rm")
        script_command.append("-u")
        script_command.append("{}:{}".format(os.getuid(), os.getgid()))
        script_command.append("--group-add")
        script_command.append("{}".format(grp.getgrnam("dockerroot").gr_gid))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.working_dir, self.context.working_dir))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.lin.path, self.lin.path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.fmask.output_path, self.fmask.output_path))
        script_command.append("--name")
        script_command.append("fmask_extractor_{}".format(self.lin.product_id))
        script_command.append("fmask_extractor")

        #actual fmask_extractor command
        script_path = "/usr/share/fmask/fmask_extractor.py"
        script_command.append(script_path)
        script_command.append("--working-dir")
        script_command.append(self.context.working_dir)
        script_command.append("--delete-temp")
        script_command.append("False")
        script_command.append("--product-id")
        script_command.append(str(self.lin.product_id))
        fmask_threshold = db_get_fmask_threshold(self.lin.site_id)
        if fmask_threshold is not None:
            script_command.append("-t")
            script_command.append(self.lin.fmask_threshold)

        script_command.append(self.lin.path)
        script_command.append(self.fmask.output_path)
  
        fmask_log_path = os.path.join(self.fmask.output_path,"fmask.log")
        fmask_log_file = open(fmask_log_path, "a")
        command_string =""
        for argument in script_command:
            command_string = command_string + " " + str(argument)
        self.fmask_log("Running command: {}".format(command_string))
        print("Running Fmask, console output can be found at {}".format(fmask_log_path))
        command_return = subprocess.call(script_command, stdout = fmask_log_file, stderr = fmask_log_file)

        if (command_return == 0) and os.path.isdir(self.fmask.output_path):
            return True
        else:
            self.lin.should_retry = False #TBD
            self.update_rejection_reason(
                "Can NOT run Fmask script, error code: {}.".format(command_return)
            )
            self.fmask_log(
                "Can NOT run Fmask script, error code: {}.".format(command_return)
            )
            return False

    def postprocess(self):
        #remove and compress sre/fre and transform to cog/compressed tiffs
        for root, dirnames, filenames in os.walk(self.fmask.output_path):
            for filename in filenames:
                if filename.endswith((".TIF", ".tif")):
                    tifFilePath = os.path.join(root, filename)
                    print("Post-processing {}".format(filename))
                    if self.context.removeSreFiles:
                        delete_file_if_match(
                            tifFilePath, filename, ".*SRE.*\.DBL\.TIF", "SRE"
                        )
                        delete_file_if_match(
                            tifFilePath, filename, ".*_SRE_B.*\.tif", "SRE"
                        )
                    elif self.context.removeFreFiles:
                        delete_file_if_match(
                            tifFilePath, filename, ".*FRE.*\.DBL\.TIF", "FRE"
                        )
                        delete_file_if_match(
                            tifFilePath, filename, ".*_FRE_B.*\.tif", "FRE"
                        )
                    if self.context.compressTiffs or self.context.cogTiffs:
                        optgtiffArgs = ""
                        if self.context.compressTiffs:
                            optgtiffArgs += " --compress"
                            optgtiffArgs += " DEFLATE"
                        else:
                            optgtiffArgs += " --no-compress"

                        if self.context.cogTiffs:
                            isMask = re.match(r".*_((MSK)|(QLT))_*.\.DBL\.TIF", filename)
                            if isMask is None:
                                # check for Fmask mask rasters
                                isMask = re.match(
                                    r".*_(CLM|MG2|EDG|DFP)_.*\.tif", filename
                                )
                            if isMask is not None:
                                optgtiffArgs += " --resampler"
                                optgtiffArgs += " nearest"
                            else:
                                optgtiffArgs += " --resampler"
                                optgtiffArgs += " average"
                            optgtiffArgs += " --overviews"
                            optgtiffArgs += " --tiled"
                        else:
                            optgtiffArgs += " --no-overviews"
                            optgtiffArgs += " --strippped"
                        optgtiffArgs += " "
                        optgtiffArgs += tifFilePath
                        print(
                            "Running optimize_gtiff.py with params {}".format(
                                optgtiffArgs
                            )
                        )
                        os.system("optimize_gtiff.py" + optgtiffArgs)

    def manage_prods_status(
        self, preprocess_succesful, process_succesful, postprocess_succesful
    ):
        if (
            (preprocess_succesful == True)
            and (process_succesful == True)
            and (postprocess_succesful == True)
        ):
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
            self.move_to_destination()
        else:
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
            self.lin.should_retry = False #TBD

    def run(self):
        preprocess_succesful = False
        process_succesful = False
        postprocess_succesful = False

        # pre-processing
        if self.check_lin() and self.fmask_setup():
            preprocess_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful pre-processing = {}".format(
                self.context.worker_id, preprocess_succesful
            )
        )
        self.fmask_log("Successful pre-processing = {}".format(preprocess_succesful))

        # processing
        if preprocess_succesful:
            process_succesful = self.run_script()
        print(
            "\n(launcher info) <worker {}>: Successful processing = {}".format(
                self.context.worker_id, process_succesful
            )
        )
        self.fmask_log("Successful processing = {}".format(process_succesful))

        # postprocessing
        if self.get_fmask_footprint(self.fmask.output_path):
            postprocess_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful post-processing = {}".format(
                self.context.worker_id, postprocess_succesful
            )
        )
        self.fmask_log("Successful post-processing = {}".format(postprocess_succesful))

        self.manage_prods_status(
            preprocess_succesful, process_succesful, postprocess_succesful
        )
        return self.lin, self.fmask

class FMaskContext(object):
    def __init__(self, site_context, worker_id):
        self.working_dir = site_context.working_dir
        self.output_path = site_context.output_path
        self.worker_id = worker_id
        self.processor_log_dir = FMASK_LOG_DIR
        self.processor_log_file = FMASK_LOG_FILE_NAME
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))

class FMaskMaster(object):
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.master_q = Queue.Queue(maxsize=self.num_workers)
        self.workers = []
        self.in_processing = set()
        for worker_id in range(self.num_workers):
            self.workers.append(FmaskWorker(worker_id, self.master_q))
            self.workers[worker_id].daemon = True
            self.workers[worker_id].start()
            msg_to_master = MsgToMaster(worker_id, None, None, False)
            self.master_q.put(msg_to_master)

    def signal_handler(self, signum, frame):
        print("(launcher info) Signal caught: {}.".format(signum))
        self.stop_workers()

    def stop_workers(self):
        print("\n(launcher info) <master>: Stoping workers")
        for worker in self.workers:
            worker.worker_q.put(None)
        for worker in self.workers:
            worker.join(timeout=5)

        cmd = []
        containers = []
        cmd.append("docker")
        cmd.append("stop")
        container_types = ["fmask", "fmask_extractor"]
        for product_id in self.in_processing:
            for container_type in container_types:
                containers.append("{}_{}".format(container_type, product_id))

        if containers:
            print("\n(launcher info) <master>: Stoping containers")
            cmd.extend(containers)
            run_command(cmd, LAUNCHER_LOG_DIR, LAUNCHER_LOG_FILE_NAME)

        print("\n(launcher info) <master>: is stopped")
        os._exit(1)

    def run(self):
        sleeping_workers = []
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        try:
            while True:
                # wait for a worker to finish
                try:
                    msg_to_master = self.master_q.get(timeout=5)
                except Queue.Empty:
                    continue
                if msg_to_master.update_db:
                    db_postrun_update(msg_to_master.lin, msg_to_master.fmask)
                    self.in_processing.remove(msg_to_master.lin.product_id)
                sleeping_workers.append(msg_to_master.worker_id)
                while len(sleeping_workers) > 0:
                    unprocessed_tile = db_get_unprocessed_tile()
                    if unprocessed_tile is not None:
                        processing_context = db_get_processing_context()
                        site_context = processing_context.get_site_context(
                            unprocessed_tile.site_id
                        )
                        site_context.get_site_info()
                        site_context_valid = site_context.is_valid()
                        valid_tile = unprocessed_tile.is_valid()
                        if valid_tile == False:
                            print(
                                "\n(launcher error) <master>: Product {} has invalid tile info.".format(
                                    unprocessed_tile.downloader_history_id
                                )
                            )
                            log(
                                LAUNCHER_LOG_DIR,
                                "(launcher error) <master>: Product {} has invalid tile info.".format(
                                    unprocessed_tile.downloader_history_id
                                ),
                            )
                            db_prerun_update(
                                unprocessed_tile, "Invalid tile information."
                            )
                            continue

                        if site_context_valid == False:
                            print(
                                "\n(launcher error) <master>: Product {} has invalid site info.".format(
                                    unprocessed_tile.downloader_history_id
                                )
                            )
                            log(
                                LAUNCHER_LOG_DIR,
                                "(launcher error) <master>: Product {} has invalid site info.".format(
                                    unprocessed_tile.downloader_history_id
                                ),
                            )
                            db_prerun_update(unprocessed_tile, "Invalid site context.")
                            continue

                        if valid_tile and site_context_valid:
                            worker_id = sleeping_workers.pop()
                            msg_to_worker = MsgToWorker(unprocessed_tile, site_context)
                            self.workers[worker_id].worker_q.put(msg_to_worker)
                            self.in_processing.add(unprocessed_tile.downloader_history_id)
                            print(
                                "\n(launcher info) <master>: product {} assigned to <worker {}>".format(
                                    unprocessed_tile.downloader_history_id, worker_id
                                )
                            )
                    else:
                        break

                if len(sleeping_workers) == self.num_workers:
                    print("\n(launcher info) <master>: No more tiles to process")
                    break

        except Exception as e:
            print("\n(launcher error) <master>: Exception {} encountered".format(e))
            traceback.print_exc(limit=20, file=sys.stdout)
        finally:
            self.stop_workers()


class MsgToMaster(object):
    def __init__(self, worker_id, lin, fmask, update_db):
        self.worker_id = worker_id
        self.lin = lin
        self.fmask = fmask
        self.update_db = update_db

class MsgToWorker(object):
    def __init__(self, unprocessed_tile, site_context):
        self.unprocessed_tile = unprocessed_tile
        self.site_context = site_context

class FmaskWorker(threading.Thread):
    def __init__(self, worker_id, master_q):
        super(FmaskWorker, self).__init__()
        self.worker_id = worker_id
        self.master_q = master_q
        self.worker_q = Queue.Queue(maxsize=1)

    def notify_end_of_tile_processing(self, lin, fmask):
        notification = MsgToMaster(self.worker_id, lin, fmask, True)
        self.master_q.put(notification)

    def run(self):
        try:
            while True:
                msg_to_worker = self.worker_q.get()
                if msg_to_worker is None:
                    # efectively stops the worker
                    break
                if (
                    msg_to_worker.unprocessed_tile is None
                    or msg_to_worker.site_context is None
                ):
                    print(
                        "\n(launcher error) <worker {}>: Either the tile or site context is None".format(
                            self.worker_id
                        )
                    )
                    os._exit(1)
                else:
                    lin, fmask = self.process_tile(
                        msg_to_worker.unprocessed_tile, msg_to_worker.site_context
                    )
                    self.notify_end_of_tile_processing(lin, fmask)
                    self.worker_q.task_done()
        except Exception as e:
            print(
                "\n(launcher error) <worker {}>: Exception {} encountered".format(
                    self.worker_id, e
                )
            )
            traceback.print_exc(limit=20, file=sys.stdout)
            os._exit(1)
        finally:
            print("\n(launcher info) <worker {}>: is stopped".format(self.worker_id))

    def process_tile(self, tile, site_context):
        print("\n#################### Tile & Site Info ####################\n")
        print(
            "\n(launcher info) <worker {}>: site_id = {}".format(
                self.worker_id, tile.site_id
            )
        )
        print(
            "\n(launcher info) <worker {}>: satellite_id = {}".format(
                self.worker_id, tile.satellite_id
            )
        )
        print(
            "\n(launcher info) <worker {}>: downloader_history_id = {}".format(
                self.worker_id, tile.downloader_history_id
            )
        )
        print(
            "\n(launcher info) <worker {}>: path = {}".format(self.worker_id, tile.path)
        )

        print(
            "\n(launcher info) <worker {}>: site_id = {}".format(
                self.worker_id, tile.site_id
            )
        )

        fmask_context = FMaskContext(site_context, self.worker_id)
        fmask_processor = FmaskProcessor(fmask_context, tile)
        lin, fmask = fmask_processor.run()
        del fmask_processor
        return lin, fmask

class Tile(object):
    def __init__(self, tile_info):
        self.site_id = tile_info[0]
        self.satellite_id = tile_info[1]
        self.downloader_history_id = tile_info[2]
        self.path = tile_info[3]
 
    def is_valid(self):
        if not os.path.exists(self.path):
            log(
                LAUNCHER_LOG_DIR,
                ": Aborting processing for product with downloaded history id {} because the path {} is incorrect".format(
                    self.downloader_history_id, self.path
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        return True

class SiteContext(object):
    def __init__(self):
        self.site_id = None
        self.base_abs_path = None
        self.output_path = ""
        self.working_dir = ""

    def get_site_info(self):
        self.site_short_name = db_get_site_short_name(self.site_id)
        if "{site}" in self.output_path:
            self.output_path = self.output_path.replace("{site}", self.site_short_name)

    def is_valid(self):
        if len(self.output_path) == 0:
            print(
                "(launcher error) <master>: Invalid processing context output_path: {}.".format(
                    self.output_path
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher error) <master>: Invalid processing context output_path: {}.".format(
                    self.output_path
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if not os.path.isdir(self.working_dir) and not create_recursive_dirs(self.working_dir):
            print(
                "(launcher error) <master>: Invalid processing context working_dir: {}".format(
                    self.working_dir
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher error) <master>: Invalid processing context working_dir: {}".format(
                    self.working_dir
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        return True

class ProcessingContext(object):
    def __init__(self):
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.output_path = {"default": ""}
        self.working_dir = {"default": ""}
        self.num_workers = 2

    def get_site_context(self, site_id):
        site_context = SiteContext()

        site_context.base_abs_path = self.base_abs_path
        site_context.site_id = site_id      
        if site_id in self.working_dir:
            site_context.working_dir = self.working_dir[site_id]
        else:
            site_context.working_dir = self.working_dir["default"]
        if site_id in self.output_path:
            site_context.output_path = self.output_path[site_id]
        else:
            site_context.output_path = self.output_path["default"]

        return site_context

    def add_parameter(self, row):
        if len(row) == 3 and row[0] is not None and row[2] is not None:
            parameter = row[0]
            site = row[1]
            value = row[2]
            if parameter == "processor.fmask.optical.num-workers":
                self.num_workers = int(value)
            elif parameter == "processor.fmask.working-dir":
                if site is not None:
                    self.working_dir[site] = value
                else:
                    self.working_dir["default"] = value
            elif parameter == "processor.fmask.optical.output-path":
                if site is not None:
                    self.output_path[site] = value
                else:
                    self.output_path["default"] = value

class FMaskConfig(object):
    def __init__(self, output_path, working_dir):
        self.output_path = output_path
        self.working_dir = working_dir
        
        print("Fmask Using configuration:")
        print("\tOutput path: {}".format(output_path))
        print("\tWorking_dir: {}".format(working_dir))

class Database(object):
    def __init__(self, log_file=None):
        self.server_ip = ""
        self.database_name = ""
        self.user = ""
        self.password = ""
        self.is_connected = False;
        self.log_file = log_file

    def database_connect(self):
        if self.is_connected:
            print("(launcher info) <master>: Database is already connected.")
            return True
        connectString = "dbname='{}' user='{}' host='{}' password='{}'".format(self.database_name, self.user, self.server_ip, self.password)
        try:
            self.conn = psycopg2.connect(connectString)
            self.cursor = self.conn.cursor()
            self.is_connected = True
        except:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # Exit the script and print an error telling what happened.
            print("(launcher error) <master>: Database connection failed!\n ->{}".format(exceptionValue))
            self.is_connected = False
            return False
        return True

    def database_disconnect(self):
        try:
            if self.conn:
                self.conn.close()
                self.is_connected = False
        except Exception as e:
            print("(launcher error) <master>: Can NOT close database connection due to: {}".format(e))


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
                                self.database_name = elements[1]
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
        if len(self.host) <= 0 or len(self.database_name) <= 0:
            return False
        return True

def db_update(db_func):
    def wrapper_db_update(*args, **kwargs):
        nb_retries = 10
        max_sleep = 0.1
        db_updated = False
        if not products_db.database_connect():
            log(
                LAUNCHER_LOG_DIR,
                "{}: Database connection failed upon updating the database.".format(
                    threading.currentThread().getName()
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return db_updated
        while True:
            try:
                db_func(*args, **kwargs)
                products_db.conn.commit()
            except psycopg2.Error as e:
                products_db.conn.rollback()
                if (
                    e.pgcode
                    in (
                        SERIALIZATION_FAILURE,
                        DEADLOCK_DETECTED,
                    )
                    and nb_retries > 0
                ):
                    log(
                        LAUNCHER_LOG_DIR,
                        "{}: Exception {} when trying to update db: SERIALIZATION failure".format(
                            threading.currentThread().getName(), e.pgcode
                        ),
                        LAUNCHER_LOG_FILE_NAME,
                    )
                    time.sleep(random.uniform(0, max_sleep))
                    max_sleep *= 2
                    nb_retries -= 1
                    continue
                log(
                    LAUNCHER_LOG_DIR,
                    "{}: Exception {} when trying to update db".format(
                        threading.currentThread().getName(), e.pgcode
                    ),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            except Exception as e:
                products_db.conn.rollback()
                log(
                    LAUNCHER_LOG_DIR,
                    "{}: Exception {} when trying to update db".format(
                        threading.currentThread().getName(), e
                    ),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            else:
                log(
                    LAUNCHER_LOG_DIR,
                    "(launcher info) <master>: Successful db update by".format(db_func.__name__),
                    LAUNCHER_LOG_FILE_NAME,
                )
                db_updated = True
                break
            finally:
                products_db.database_disconnect()

        return db_updated

    return wrapper_db_update

def db_fetch(db_func):
    def wrapper_db_fetch(*args, **kwargs):
        nb_retries = 10
        max_sleep = 0.1
        ret_val = None
        if not products_db.database_connect():
            log(
                LAUNCHER_LOG_DIR,
                "(launcher error) <master>: Database connection failed upon fetching from the database.",
                LAUNCHER_LOG_FILE_NAME,
            )
            return ret_val
        while True:
            ret_val = None
            try:
                ret_val = db_func(*args, **kwargs)
                products_db.conn.commit()
            except psycopg2.Error as e:
                products_db.conn.rollback()
                if (
                    e.pgcode
                    in (
                        SERIALIZATION_FAILURE,
                        DEADLOCK_DETECTED,
                    )
                    and nb_retries > 0
                ):
                    log(
                        LAUNCHER_LOG_DIR,
                        "(launcher error) <master>: Exception {} when trying to fetch from db: SERIALIZATION failure".format(e.pgcode),
                        LAUNCHER_LOG_FILE_NAME,
                    )
                    time.sleep(random.uniform(0, max_sleep))
                    max_sleep *= 2
                    nb_retries -= 1
                    continue
                log(
                    LAUNCHER_LOG_DIR,
                    "(launcher error) <master>: Exception {} when trying to fetch from db by {}.".format(e.pgcode, db_func.__name__),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            except Exception as e:
                products_db.conn.rollback()
                log(
                    LAUNCHER_LOG_DIR,
                    "(launcher error) <master>: Exception {} when trying to fetch from db by {}.".format(e, db_func.__name__),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            else:
                log(
                    LAUNCHER_LOG_DIR,
                    "(launcher info) <master>: Successful db fetch by {}".format(db_func.__name__),
                    LAUNCHER_LOG_FILE_NAME
                )
                break
            finally:
                products_db.database_disconnect()

        return ret_val

    return wrapper_db_fetch

@db_fetch
def db_get_unprocessed_tile():
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute("select * from sp_start_fmask_l1_tile_processing();")
    tile_info = products_db.cursor.fetchall()
    if tile_info == []:
        return None
    else:
        return Tile(tile_info[0])

@db_fetch
def db_get_fmask_threshold(site_id):
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute("select value from sp_get_parameters('{}') where site_id is null or site_id = {} order by site_id limit 1".format(DATABASE_FMASK_THRESHOLD, site_id))
    rows = products_db.cursor.fetchall()
    if rows and isinstance(rows[0], int):
        return rows[0]
    else:
        return None

@db_fetch
def db_clear_pending_tiles():
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute("select * from sp_clear_pending_fmask_tiles();")
    return products_db.cursor.fetchall()

@db_fetch
def db_get_site_short_name(site_id):
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute(
        "select short_name from site where id={}".format(site_id)
    )
    rows = products_db.cursor.fetchall()
    if rows != []:
        return rows[0][0]
    else:
        return None

@db_fetch
def db_get_processing_context():

    processing_context = ProcessingContext()
    products_db.cursor.execute("select * from sp_get_parameters('processor.fmask.')")
    rows = products_db.cursor.fetchall()
    for row in rows:
        processing_context.add_parameter(row)

    return processing_context

@db_update
def db_postrun_update(input_prod, fmask_prod):
    processing_status = input_prod.processing_status
    downloader_product_id = input_prod.product_id
    reason = input_prod.rejection_reason
    should_retry = input_prod.should_retry
    site_id = input_prod.site_id
    full_path = fmask_prod.destination_path
    product_name = fmask_prod.name
    footprint = fmask_prod.footprint
    sat_id = fmask_prod.satellite_id
    acquisition_date = fmask_prod.acquisition_date

    products_db.cursor.execute("set transaction isolation level serializable;")

    # updating fmask_history
    if reason is not None:
        products_db.cursor.execute(
            """SELECT * FROM sp_mark_fmask_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                  %(reason)s, 
                                                                                  %(should_retry)s :: boolean);""",
            {
                "downloader_history_id" : downloader_product_id,
                "reason" : reason,
                "should_retry" : should_retry
            }
        )
    else:
        products_db.cursor.execute(
            """SELECT * FROM sp_mark_fmask_l1_tile_done(%(downloader_history_id)s :: integer);""",
                                 {
                                     "downloader_history_id" : downloader_product_id
                                 },
        )

    # update product table
    if reason is None and (
        processing_status == DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
    ):
        products_db.cursor.execute(
           """select * from sp_insert_product(%(product_type_id)s :: smallint,
                               %(processor_id)s :: smallint,
                               %(satellite_id)s :: smallint,
                               %(site_id)s :: smallint,
                               %(job_id)s :: smallint,
                               %(full_path)s :: character varying,
                               %(created_timestamp)s :: timestamp,
                               %(name)s :: character varying,
                               %(quicklook_image)s :: character varying,
                               %(footprint)s,
                               %(orbit_id)s :: integer,
                               %(tiles)s :: json,
                               %(orbit_type_id)s :: smallint,
                               %(downloader_history_id)s :: integer)""",
                                {
                                    "product_type_id" : 25,
                                    "processor_id" : 1,
                                    "satellite_id" : sat_id,
                                    "site_id" : site_id,
                                    "job_id" : None,
                                    "full_path" : full_path,
                                    "created_timestamp" : acquisition_date,
                                    "name" : product_name,
                                    "quicklook_image" : None, #TBD
                                    "footprint" : footprint,
                                    "orbit_id" : None,
                                    "tiles" : None,
                                    "orbit_type_id" : None,
                                    "downloader_history_id" : downloader_product_id
                                }
        )

@db_update
def db_prerun_update(tile, reason):
    downloader_history_id = tile.downloader_history_id
    should_retry = True

    products_db.cursor.execute("set transaction isolation level serializable;")
    # updating  fmask_l1_tile_history
    products_db.cursor.execute(
        """SELECT * FROM sp_mark_fmask_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                  %(reason)s, 
                                                                                  %(should_retry)s :: boolean);""",
        {
            "downloader_history_id" : downloader_history_id,
            "reason" : reason,
            "should_retry" : should_retry
        }
    )

parser = argparse.ArgumentParser(description="Launcher for FMASK script")
parser.add_argument('-c', '--config', default="/etc/sen2agri/sen2agri.conf", help="configuration file")
args = parser.parse_args()

# get the db configuration from cfg file
products_db = Database()
if not products_db.loadConfig(args.config):
    log(
        LAUNCHER_LOG_DIR,
        "Could not load the db config from configuration file",
        LAUNCHER_LOG_FILE_NAME,
    )
    sys.exit(1)

# get the processing context
default_processing_context = db_get_processing_context()
if default_processing_context is None:
    log(
        LAUNCHER_LOG_DIR,
        "Could not load the processing context from database",
        LAUNCHER_LOG_FILE_NAME,
    )
    sys.exit(1)

if default_processing_context.num_workers < 1:
    print(
        "(launcher err) <master>: Invalid processing context num_workers: {}".format(
            default_processing_context.num_workers
        )
    )
    log(
        LAUNCHER_LOG_DIR,
        "(launcher err) <master>: Invalid processing context num_workers: {}".format(
                    default_processing_context.num_workers
        ),
        LAUNCHER_LOG_FILE_NAME,
    )
    sys.exit(1)

# delete all the temporary content from working dir from a previous run
remove_dir_content(default_processing_context.working_dir["default"])

# clear pending tiless
db_clear_pending_tiles()
fmask_master = FMaskMaster(default_processing_context.num_workers)
fmask_master.run()

if not DEBUG:
    remove_dir_content("{}/".format(default_processing_context.working_dir["default"]))
