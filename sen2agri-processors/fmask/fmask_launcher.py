#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen4Cap-Processors
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
from logging import error
import sys
import argparse
import threading
import ogr
import os
import Queue
import re
import grp
import glob
import signal
import time
import datetime
import pipes
from psycopg2.sql import SQL
from l2a_commons import LogHandler, MASTER_ID
from l2a_commons import LANDSAT8_SATELLITE_ID, SENTINEL2_SATELLITE_ID
from l2a_commons import ArchiveHandler, translate, get_node_id, run_command, stop_containers
from l2a_commons import create_recursive_dirs, remove_dir, get_guid, get_footprint, remove_dir_content 
from db_commons import DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE, DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
from db_commons import DBConfig, handle_retries, db_get_site_short_name, db_get_processing_context

ARCHIVES_DIR_NAME = "archives"
LAUNCHER_LOG_DIR = "/var/log/sen2agri/"
LAUNCHER_LOG_FILE_NAME = "fmask_launcher.log"
FMASK_EXTRACTOR = "fmask_extractor.py"
DATABASE_FMASK_NUM_WORKERS = "processor.fmask.optical.num-workers"
DATABASE_FMASK_OUTPUT_PATH = "processor.fmask.optical.output-path"
DATABASE_FMASK_WORKING_DIR = "processor.fmask.working-dir"
DATABASE_FMASK_THRESHOLD = "processor.fmask.optical.threshold"
DATABASE_FMASK_THRESHOLD_S2 = "processor.fmask.optical.threshold.s2"
DATABASE_FMASK_THRESHOLD_L8 = "processor.fmask.optical.threshold.l8"
DATABASE_FMASK_EXTRACTOR_IMAGE = "processor.fmask.extractor_image"
DATABASE_FMASK_IMAGE = "processor.fmask.image"
DATABASE_FMASK_CLOUD_DILATION = 'processor.fmask.optical.dilation.cloud'
DATABASE_FMASK_CLOUD_SHADOW_DILATION = 'processor.fmask.optical.dilation.cloud-shadow'
DATABASE_FMASK_SNOW_DILATION = 'processor.fmask.optical.dilation.snow'
DATABASE_FMASK_COG_TIFFS = 'processor.fmask.optical.cog-tiffs'
DATABASE_FMASK_COMPRESS_TIFFS = 'processor.fmask.optical.compress-tiffs'
DATABASE_FMASK_GDAL_IMAGE = 'processor.fmask.gdal_image'
DB_PROCESSOR_NAME = "fmask"
S2_20M_RESOLUTION = "5490"
PRODUCT_STATUS_MSG_TYPE = 1
CONTAINER_STATUS_MSG_TYPE = 2


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

class FmaskProduct(object):
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
    def __init__(self, processor_context, unprocessed_tile, master_q, launcher_log):
        self.context = processor_context
        self.lin = L1CProduct(unprocessed_tile)
        self.fmask = FmaskProduct()
        self.log_file_name = "fmask_{}.log".format(self.lin.product_id)
        self.name = "Fmask"
        self.master_q = master_q
        self.launcher_log = launcher_log
        self.fmask_log = None # initialised in fmask_setup

    def __del__(self):
        if self.lin.was_archived and os.path.exists(self.lin.path):
            remove_dir(self.lin.path)

    def get_envelope(self, footprints):
        geomCol = ogr.Geometry(ogr.wkbGeometryCollection)

        for footprint in footprints:
            for pt in footprint:
                point = ogr.Geometry(ogr.wkbPoint)
                point.AddPoint_2D(pt[0], pt[1])
                geomCol.AddGeometry(point)

        hull = geomCol.ConvexHull()
        return hull.ExportToWkt()

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

        self.launcher_log.info(
            "Starting extract_from_archive_if_needed for product {}".format(
                self.lin.product_id
            )
        )
        archives_dir = os.path.join(self.context.working_dir, ARCHIVES_DIR_NAME)
        archive_handler = ArchiveHandler(archives_dir, self.launcher_log)
        self.lin.was_archived, self.lin.path = archive_handler.extract_from_archive_if_needed(
            self.lin.db_path
        )
        self.launcher_log.info(
            "Ended extract_from_archive_if_needed for product {}".format(
                self.lin.product_id
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
                rejection_reason = "The input product name is wrong - Fmask cannot be filled: {}".format(
                        lin_basename
                ) 
                self.update_rejection_reason(rejection_reason)
                self.launcher_log.error(rejection_reason)
                return False
        else:
            rejection_reason = (
                "The input product name is wrong: {}".format(lin_basename)
            )
            self.update_rejection_reason(rejection_reason)
            self.launcher_log.error(rejection_reason)
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
                rejection_reason = (
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                self.update_rejection_reason(rejection_reason)
                self.launcher_log.error(rejection_reason)
                return False
        elif lin_basename.startswith("LC"):
            result = re.findall(r"_\d{8}_", lin_basename)
            if result:
                acq_date = result[0].strip("_")
                acq_year = acq_date[:4]
                acq_month = acq_date[4:6]
                acq_day = acq_date[6:]
            else:
                rejection_reason = (
                    "Can NOT obtain the aquisition date on input product: {}".format(
                        lin_basename
                    )
                )
                self.update_rejection_reason(rejection_reason)
                self.launcher_log.error(rejection_reason)
                return False
        else:
            rejection_reason = (
                "Can NOT obtain the aquisition date on input product: {}".format(
                    lin_basename
                )
            )
            self.update_rejection_reason(rejection_reason)
            self.launcher_log.error(rejection_reason)
            return False

        # determine the path of the fmask product
        fmask_output_path = os.path.join(
            self.context.output_path, "output", fmask_basename
        )
        fmask_destination_path = os.path.join(
            self.context.output_path, acq_year, acq_month, acq_day, fmask_basename
        )
        if not create_recursive_dirs(fmask_output_path):
            rejection_reason = (
                "Can NOT create the output directory: {}".format(fmask_output_path)
            )
            self.launcher_log.error(rejection_reason)
            return False

        self.fmask.output_path = fmask_output_path
        self.fmask.destination_path = fmask_destination_path
        self.fmask.satellite_id = self.lin.satellite_id
        self.fmask.site_id = self.lin.site_id
        self.fmask.product_id = self.lin.product_id
        fmask_log_path = os.path.join(self.fmask.output_path, "fmask_{}.log".format(self.lin.product_id))
        self.fmask_log = LogHandler(
            fmask_log_path,
            "fmask_log",
            self.launcher_log.level,
            self.context.worker_id
        )

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
            if create_recursive_dirs(dst):
                os.rename(self.fmask.output_path, self.fmask.destination_path)
            else:
                self.update_rejection_reason(" Can NOT copy from output path {} to destination product path {}".format(self.fmask.output_path, self.fmask.destination_path))
        except Exception as e:
            rejection_reason = ("Can NOT copy from output path {} to destination product path {} due to: {}".format(self.fmask.output_path, self.fmask.destination_path, e))
            self.update_rejection_reason(rejection_reason)
            self.launcher_log.error(msg, print_msg = True, trace = True)

    def get_fmask_footprint(self, footprint_file) :
        try:
            wgs84_extent_list = []
            wgs84_extent_list.append(get_footprint(footprint_file)[0])
            self.fmask.footprint = self.get_envelope(wgs84_extent_list)
        except Exception as e:
            self.fmask_log.error(
                "Exception encouted upon extracting the footprint: {}".format(e),
                trace = True
            )

    def run_script(self):
        guid = get_guid(8)
        container_name = "fmask_extractor_{}_{}".format(self.lin.product_id, guid)
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
        script_command.append(container_name)
        script_command.append(self.context.fmask_extractor_image)

        #actual fmask_extractor command
        script_path = "/usr/share/fmask/fmask_extractor.py"
        script_command.append(script_path)
        script_command.append("--working-dir")
        script_command.append(self.context.working_dir)
        script_command.append("--delete-temp")
        script_command.append("--product-id")
        script_command.append(str(self.lin.product_id))
        script_command.append("--image-name")
        script_command.append(self.context.fmask_image)
        if self.context.cloud_dilation != '':
            script_command.append("--cloud-dilation")
            script_command.append(self.context.cloud_dilation)    
        if self.context.cloud_shadow_dilation != '':
            script_command.append("--cloud-shadow-dilation")
            script_command.append(self.context.cloud_shadow_dilation)
        if self.context.snow_dilation != '':
            script_command.append("--snow-dilation")
            script_command.append(self.context.snow_dilation) 
        script_command.append("-t")
        script_command.append(self.context.fmask_threshold)  
        script_command.append("--log-level")
        script_command.append(self.fmask_log.level)
        script_command.append(self.lin.path)
        script_command.append(self.fmask.output_path)
        
        print("Running Fmask, console output can be found at {}".format(self.fmask_log.path))
        cmd_str = " ".join(map(pipes.quote, script_command))
        self.launcher_log.info("Running command: " + cmd_str)
        start_time = time.time()
        notification = ContainerStatusMsg(container_name, True)
        self.master_q.put(notification)
        command_return = run_command(script_command, self.fmask_log)
        notification = ContainerStatusMsg(container_name, False)
        self.master_q.put(notification)
        end_time = time.time()
        self.launcher_log.info(
            "Command finished with return code {} in {}".format(command_return, datetime.timedelta(seconds=(end_time - start_time))),
            print_msg = True
        )
        if (command_return == 0) and os.path.isdir(self.fmask.output_path):
            return True
        else:
            self.lin.should_retry = False
            rejection_reason = "Can NOT run Fmask script, error code: {}.".format(command_return)
            self.update_rejection_reason(rejection_reason)
            self.fmask_log.error(rejection_reason)
            return False

    def manage_prods_status(
        self, preprocess_successful, process_successful, fmask_file_ok
    ):
        if (
            (preprocess_successful == True)
            and (process_successful == True)
            and (fmask_file_ok == True)
        ):
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
            self.move_to_destination()
        else:
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
            self.lin.should_retry = False #TBD

    def run(self):
        preprocess_successful = False
        process_successful = False
        fmask_file_ok = False

        # pre-processing
        if self.check_lin() and self.fmask_setup():
            preprocess_successful = True
        self.launcher_log.info(
            "Successful pre-processing = {}".format(
                self.context.worker_id, preprocess_successful
            ),
            print_msg = True
        )
        self.fmask_log.info(
            "Successful pre-processing = {}".format(preprocess_successful),
            print_msg = True
        )

        # processing
        if preprocess_successful:
            process_successful = self.run_script()
        self.launcher_log.info(
            "Successful processing = {}".format(
                self.context.worker_id, process_successful
            ),
            print_msg = True
        )
        self.fmask_log.info("Successful processing = {}".format(process_successful), print_msg = True)

        #checking the presence of fmask file
        fmask_file_pattern = "*_Fmask4.tif"
        fmask_file_path = os.path.join(self.fmask.output_path, fmask_file_pattern)
        fmask_files = glob.glob(fmask_file_path)
        if len(fmask_files) == 1:
            log_file = os.path.join(self.fmask.output_path, self.log_file_name)
            if self.lin.satellite_id == SENTINEL2_SATELLITE_ID:
                output_img_name = os.path.basename(fmask_files[0])[:-4] + "_20m.tif"
            elif self.lin.satellite_id == LANDSAT8_SATELLITE_ID:
                output_img_name = os.path.basename(fmask_files[0])[:-4] + "_30m.tif"
            container_name = "gdal_" + str(self.lin.product_id)
                         
            if self.context.cog_tiffs:
                output_format = "COG"
            else:
                output_format = "GTiff"
            #for S2 create a 10m resample copy 
            if self.lin.satellite_id == SENTINEL2_SATELLITE_ID:
                resampled_img_name = os.path.basename(fmask_files[0])[:-4] + "_10m.tif"
                guid = get_guid(8)
                container_name = "gdal_{}_{}".format(self.lin.product_id, guid)
                notification = ContainerStatusMsg(container_name, True)
                self.master_q.put(notification)
                translate(input_img = fmask_files[0],
                      output_dir = self.fmask.output_path,
                      output_img_name = resampled_img_name,
                      output_img_format = output_format,
                      log = self.fmask_log,
                      gdal_image = self.context.gdal_image,
                      name = container_name,
                      resample_res = 10,
                      compress = self.context.compress_tiffs,
                )
                notification = ContainerStatusMsg(container_name, False)
                self.master_q.put(notification)
            #translate to cog and/or compress
            if self.context.cog_tiffs or self.context.compress_tiffs:
                guid = get_guid(8)
                container_name = "gdal_{}_{}".format(self.lin.product_id, guid)
                notification = ContainerStatusMsg(container_name, True)
                self.master_q.put(notification)
                translate(input_img = fmask_files[0],
                        output_dir = self.fmask.output_path,
                        output_img_name = output_img_name,
                        output_img_format = output_format,
                        log = self.fmask_log,
                        gdal_image = self.context.gdal_image,
                        name = container_name,
                        compress = self.context.compress_tiffs,
                )
                notification = ContainerStatusMsg(container_name, False)
                self.master_q.put(notification)
                os.remove(fmask_files[0])
            else:
                os.rename(fmask_files[0], output_img_name)

            fmask_file_ok = True
        else:
            rejection_reason = "Can NOT find Fmask4.tif file in: {} ".format(self.fmask.output_path)
            self.update_rejection_reason(rejection_reason)
            self.launcher_log.error(rejection_reason)
            fmask_file_ok = False

        self.manage_prods_status(
            preprocess_successful, process_successful, fmask_file_ok
        )
        return self.lin, self.fmask

class FmaskContext(object):
    def __init__(self, site_context, worker_id, tile):
        self.working_dir = site_context.working_dir
        self.output_path = site_context.output_path
        if tile.satellite_id == SENTINEL2_SATELLITE_ID:
            self.fmask_threshold = site_context.fmask_threshold_s2
        elif tile.satellite_id == LANDSAT8_SATELLITE_ID:
            self.fmask_threshold = site_context.fmask_threshold_l8
        else:
            self.fmask_threshold = site_context.fmask_threshold
        self.worker_id = worker_id
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.fmask_extractor_image = site_context.fmask_extractor_image
        self.fmask_image = site_context.fmask_image
        self.gdal_image = site_context.fmask_gdal_image
        self.cloud_dilation = site_context.fmask_cloud_dilation
        self.cloud_shadow_dilation = site_context.fmask_cloud_shadow_dilation
        self.snow_dilation = site_context.fmask_snow_dilation
        self.cog_tiffs = site_context.fmask_cog_tiffs
        self.compress_tiffs = site_context.fmask_compress_tiffs

class FmaskMaster(object):
    def __init__(self, num_workers, db_config, node_id, launcher_log):
        self.num_workers = num_workers
        self.db_config = db_config
        self.node_id = node_id
        self.master_q = Queue.Queue(maxsize=self.num_workers)
        self.workers = []
        self.launcher_log = launcher_log
        self.running_containers = set()
        for worker_id in range(self.num_workers):
            self.workers.append(FmaskWorker(worker_id, self.master_q, self.launcher_log))
            self.workers[worker_id].daemon = True
            self.workers[worker_id].start()
            msg_to_master = ProductStatusMsg(worker_id, None, None, False)
            self.master_q.put(msg_to_master)

    def signal_handler(self, signum, frame):
        self.launcher_log.info("Signal caught: {}.".format(signum), print_msg = True)
        self.stop_workers()

    def stop_workers(self):
        self.launcher_log.info("Stoping workers", print_msg = True)
        for worker in self.workers:
            worker.worker_q.put(None)
        for worker in self.workers:
            worker.join(timeout=5)

        stop_containers(self.running_containers, self.launcher_log)

        os._exit(0)

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
                if msg_to_master.message_type == CONTAINER_STATUS_MSG_TYPE:
                    if msg_to_master.is_running:
                        self.running_containers.add(msg_to_master.container_name)
                    else:
                        self.running_containers.remove(msg_to_master.container_name)
                    continue
                elif msg_to_master.message_type == PRODUCT_STATUS_MSG_TYPE:
                    if msg_to_master.update_db:
                        db_postrun_update(self.db_config, msg_to_master.lin, msg_to_master.fmask, self.launcher_log)
                else:
                    msg = "Unrecognized type of message in the master queue with id: {}".format(msg_to_master.message_type),
                    self.launcher_log.error(msg, print_msg = True)
                    continue                    
                sleeping_workers.append(msg_to_master.worker_id)
                while len(sleeping_workers) > 0:
                    tile_info = db_get_unprocessed_tile(self.db_config, self.node_id, self.launcher_log)
                    if tile_info is not None:
                        unprocessed_tile = Tile(tile_info)
                        processing_context = ProcessingContext()
                        db_get_processing_context(
                            self.db_config,
                            processing_context,
                            DB_PROCESSOR_NAME,
                            self.launcher_log
                        )
                        site_context = processing_context.get_site_context(
                            unprocessed_tile.site_id
                        )
                        site_context.get_site_info(self.db_config, self.launcher_log)
                        site_context_valid, site_context_rejection_reason = site_context.is_valid(self.launcher_log)
                        valid_tile, tile_rejection_reason = unprocessed_tile.is_valid(self.launcher_log)
                        if not valid_tile:
                            db_prerun_update(
                                self.db_config, unprocessed_tile, tile_rejection_reason, self.launcher_log
                            )
                            continue
                        if not site_context_valid:
                            db_prerun_update(
                                self.db_config, unprocessed_tile, site_context_rejection_reason, self.launcher_log
                            )
                            continue

                        if valid_tile and site_context_valid:
                            worker_id = sleeping_workers.pop()
                            msg_to_worker = MsgToWorker(unprocessed_tile, site_context)
                            self.workers[worker_id].worker_q.put(msg_to_worker)
                            self.launcher_log.info(
                                "Product {} assigned to <worker {}>".format(
                                    unprocessed_tile.downloader_history_id, worker_id
                                ),
                                print_msg = True
                            )
                    else:
                        break

                if len(sleeping_workers) == self.num_workers:
                    self.launcher_log.info("No more tiles to process", print_msg = True)
                    break

        except Exception as e:
            msg = "Exception {} encountered".format(e) 
            self.launcher_log.critical(msg, print_msg = True, trace = True)
        finally:
            self.stop_workers()

class ProductStatusMsg(object):
    def __init__(self, worker_id, lin, fmask, update_db):
        self.message_type = PRODUCT_STATUS_MSG_TYPE
        self.worker_id = worker_id
        self.lin = lin
        self.fmask = fmask
        self.update_db = update_db

class ContainerStatusMsg(object):
    def __init__(self, container_name, is_running):
        self.message_type = CONTAINER_STATUS_MSG_TYPE
        self.container_name = container_name
        self.is_running = is_running

class MsgToWorker(object):
    def __init__(self, unprocessed_tile, site_context):
        self.unprocessed_tile = unprocessed_tile
        self.site_context = site_context

class FmaskWorker(threading.Thread):
    def __init__(self, worker_id, master_q, launcher_log):
        super(FmaskWorker, self).__init__()
        self.worker_id = worker_id
        self.master_q = master_q
        self.worker_q = Queue.Queue(maxsize=1)
        self.launcher_log = LogHandler(
            launcher_log.path,
            launcher_log.name,
            launcher_log.level,
            self.worker_id
        )

    def notify_end_of_tile_processing(self, lin, fmask):
        notification = ProductStatusMsg(self.worker_id, lin, fmask, True)
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
                    msg = "Either the tile or site context is None"
                    self.launcher_log.critical(msg, print_msg = True)
                    os._exit(1)
                else:
                    lin, fmask = self.process_tile(
                        msg_to_worker.unprocessed_tile, msg_to_worker.site_context
                    )
                    self.notify_end_of_tile_processing(lin, fmask)
                    self.worker_q.task_done()
        except Exception as e:
            msg = "Exception encoutered: {}".format(e) 
            self.launcher_log.critical(msg, print_msg = True, trace = True)
            os._exit(1)
        finally:
            self.launcher_log.info("Current worker is stopped".format(self.worker_id), print_msg = True)

    def process_tile(self, tile, site_context):
        print("<worker {}> Tile & Site Info:".format(self.worker_id))
        print(
            "<worker {}> site_id = {}".format(
                self.worker_id, tile.site_id
            )
        )
        print(
            "<worker {}> satellite_id = {}".format(
                self.worker_id, tile.satellite_id
            )
        )
        print(
            "<worker {}> downloader_history_id = {}".format(
                self.worker_id, tile.downloader_history_id
            )
        )
        print(
            "<worker {}> path = {}".format(self.worker_id, tile.path)
        )

        print(
            "<worker {}> site_id = {}".format(
                self.worker_id, tile.site_id
            )
        )

        fmask_context = FmaskContext(site_context, self.worker_id, tile)
        fmask_processor = FmaskProcessor(fmask_context, tile, self.master_q, self.launcher_log)
        lin, fmask = fmask_processor.run()
        del fmask_processor
        return lin, fmask

class Tile(object):
    def __init__(self, tile_info):
        self.site_id = tile_info[0]
        self.satellite_id = tile_info[1]
        self.downloader_history_id = tile_info[2]
        self.path = tile_info[3]
 
    def is_valid(self, log):
        if self.downloader_history_id is None:
            rejection_reason = "Aborting processing for product because the downloader_history_id is incorrect"
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason
        
        if self.site_id is None:
            rejection_reason = "Aborting processing for product with downloaded history id {} because the site_id is incorrect".format(
                self.downloader_history_id
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason

        if self.satellite_id is None:
            rejection_reason = "Aborting processing for product with downloaded history id {} because the satellite_id is incorrect".format(
                self.downloader_history_id
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason

        if not os.path.exists(self.path):
            rejection_reason = "Aborting processing for product with downloaded history id {} because the path is incorrect".format(
                self.downloader_history_id
            )
            log.error(rejection_reason, print_msg = True)
            return False ,rejection_reason

        return True, None

class SiteContext(object):
    def __init__(self):
        self.site_id = None
        self.base_abs_path = None
        self.output_path = ""
        self.working_dir = ""
        self.fmask_threshold = ''
        self.fmask_threshold_s2 = ''
        self.fmask_threshold_l8 = ''
        self.fmask_extractor_image = ''
        self.fmask_image = ''
        self.fmask_gdal_image = ''
        self.fmask_cloud_dilation = ''
        self.fmask_cloud_shadow_dilation = ''
        self.fmask_snow_dilation = ''
        self.fmask_compress_tiffs = ''
        self.fmask_cog_tiffs = ''
        
    def get_site_info(self, db_config, log):
        self.site_short_name = db_get_site_short_name(db_config, self.site_id, log)
        if "{site}" in self.output_path:
            self.output_path = self.output_path.replace("{site}", self.site_short_name)

    def is_valid(self, log):
        if len(self.output_path) == 0:
            rejection_reason = "Invalid processing context output_path: {}.".format(
                self.output_path
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason

        if not create_recursive_dirs(self.working_dir):
            rejection_reason = "Invalid processing context working_dir: {}".format(
                self.working_dir
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason

        if len(self.fmask_image) == 0:
            rejection_reason = "Invalid processing context fmask_image"
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason    
        
        if len(self.fmask_extractor_image) == 0:
            rejection_reason = "Invalid processing context fmask_extractor_image"
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason   

        if len(self.fmask_gdal_image) == 0:
            rejection_reason = "Invalid processing context fmask_gdal_image"
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason

        if (float(self.fmask_threshold) > 100) or (float(self.fmask_threshold) > 100):
            rejection_reason = "Invalid processing context fmask_threshold: {}".format(
                self.fmask_threshold
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason  

        if (float(self.fmask_threshold_s2) > 100) or (float(self.fmask_threshold_s2) > 100):
            rejection_reason = "Invalid processing context fmask_threshold_s2: {}".format(
                self.fmask_threshold_s2
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason    

        if (float(self.fmask_threshold_l8) > 100) or (float(self.fmask_threshold_l8) > 100):
            rejection_reason = "Invalid processing context fmask_threshold_l8: {}".format(
                self.fmask_threshold_l8
            )
            log.error(rejection_reason, print_msg = True)
            return False, rejection_reason       

        return True, None

class ProcessingContext(object):
    def __init__(self):
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.output_path = {"default": ""}
        self.working_dir = {"default": ""}
        self.num_workers = 2
        self.fmask_threshold = {"default": ''}
        self.fmask_threshold_s2 = {"default": ''}
        self.fmask_threshold_l8 = {"default": ''}
        self.fmask_extractor_image = {"default": ''}
        self.fmask_image = {"default": ''}
        self.fmask_cloud_dilation = {"default": ''}
        self.fmask_cloud_shadow_dilation = {"default": ''}
        self.fmask_snow_dilation = {"default": ''}
        self.fmask_cog_tiffs = {"default": ''}
        self.fmask_compress_tiffs = {"default":''}
        self.fmask_gdal_image = {"default":''}

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
        if site_id in self.fmask_threshold:
            site_context.fmask_threshold = self.fmask_threshold[site_id]
        else:
            site_context.fmask_threshold = self.fmask_threshold["default"]
        if site_id in self.fmask_threshold_s2:
            site_context.fmask_threshold_s2 = self.fmask_threshold_s2[site_id]
        else:
            site_context.fmask_threshold_s2 = self.fmask_threshold_s2["default"]
        if site_id in self.fmask_threshold_l8:
            site_context.fmask_threshold_l8 = self.fmask_threshold_l8[site_id]
        else:
            site_context.fmask_threshold_l8 = self.fmask_threshold_l8["default"]
        if site_id in self.fmask_extractor_image:
            site_context.fmask_extractor_image = self.fmask_extractor_image[site_id]
        else:
            site_context.fmask_extractor_image= self.fmask_extractor_image["default"]
        if site_id in self.fmask_image:
            site_context.fmask_image = self.fmask_image[site_id]
        else:
            site_context.fmask_image= self.fmask_image["default"]
        if site_id in self.fmask_cloud_dilation :
            site_context.fmask_cloud_dilation = self.fmask_cloud_dilation[site_id]
        else:
            site_context.fmask_cloud_dilation= self.fmask_cloud_dilation["default"]
        if site_id in self.fmask_cloud_shadow_dilation :
            site_context.fmask_cloud_shadow_dilation = self.fmask_cloud_shadow_dilation[site_id]
        else:
            site_context.fmask_cloud_shadow_dilation= self.fmask_cloud_shadow_dilation["default"]
        if site_id in self.fmask_snow_dilation :
            site_context.fmask_snow_dilation = self.fmask_snow_dilation[site_id]
        else:
            site_context.fmask_snow_dilation= self.fmask_snow_dilation["default"]
        if site_id in self.fmask_cog_tiffs:
            site_context.fmask_cog_tiggs = self.fmask_cog_tiffs[site_id]
        else:
            site_context.fmask_cog_tiffs = self.fmask_cog_tiffs["default"]
        if site_id in self.fmask_compress_tiffs:
            site_context.fmask_compress_tiffs = self.fmask_compress_tiffs[site_id]
        else:
            site_context.fmask_compress_tiffs = self.fmask_compress_tiffs["default"]
        if site_id in self.fmask_gdal_image:
            site_context.fmask_gdal_image = self.fmask_gdal_image[site_id]
        else:
            site_context.fmask_gdal_image = self.fmask_gdal_image["default"]
        

        return site_context

    def add_parameter(self, row):
        if len(row) == 3 and row[0] is not None and row[2] is not None:
            parameter = row[0]
            site = row[1]
            value = row[2]
            if parameter == DATABASE_FMASK_NUM_WORKERS:
                self.num_workers = int(value)
            elif parameter == DATABASE_FMASK_WORKING_DIR:
                if site is not None:
                    self.working_dir[site] = value
                else:
                    self.working_dir["default"] = value
            elif parameter == DATABASE_FMASK_OUTPUT_PATH:
                if site is not None:
                    self.output_path[site] = value
                else:
                    self.output_path["default"] = value
            elif parameter == DATABASE_FMASK_THRESHOLD:
                if site is not None:
                    self.fmask_threshold[site] = value
                else:
                    self.fmask_threshold["default"] = value
            elif parameter == DATABASE_FMASK_THRESHOLD_S2:
                if site is not None:
                    self.fmask_threshold_s2[site] = value
                else:
                    self.fmask_threshold_s2["default"] = value
            elif parameter == DATABASE_FMASK_THRESHOLD_L8:
                if site is not None:
                    self.fmask_threshold_l8[site] = value
                else:
                    self.fmask_threshold_l8["default"] = value
            elif  parameter == DATABASE_FMASK_EXTRACTOR_IMAGE:
                if site is not None:
                    self.fmask_extractor_image[site] = value
                else:
                    self.fmask_extractor_image["default"] = value
            elif  parameter == DATABASE_FMASK_IMAGE:
                if site is not None:
                    self.fmask_image[site] = value
                else:
                    self.fmask_image["default"] = value
            elif  parameter == DATABASE_FMASK_CLOUD_DILATION:
                if site is not None:
                    self.fmask_cloud_dilation[site] = value
                else:
                    self.fmask_cloud_dilation["default"] = value
            elif  parameter == DATABASE_FMASK_CLOUD_SHADOW_DILATION:
                if site is not None:
                    self.fmask_cloud_shadow_dilation[site] = value
                else:
                    self.fmask_cloud_shadow_dilation["default"] = value
            elif  parameter == DATABASE_FMASK_SNOW_DILATION :
                if site is not None:
                    self.fmask_snow_dilation[site] = value
                else:
                    self.fmask_snow_dilation["default"] = value
            elif  parameter == DATABASE_FMASK_COG_TIFFS:
                if site is not None:
                    self.fmask_cog_tiffs[site] = value
                else:
                    self.fmask_cog_tiffs["default"] = value
            elif  parameter == DATABASE_FMASK_COMPRESS_TIFFS:
                if site is not None:
                    self.fmask_compress_tiffs[site] = value
                else:
                    self.fmask_compress_tiffs["default"] = value
            elif  parameter == DATABASE_FMASK_GDAL_IMAGE:
                if site is not None:
                    self.fmask_gdal_image[site] = value
                else:
                    self.fmask_gdal_image["default"] = value

def db_get_unprocessed_tile(db_config, node_id, log):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        cursor.execute("""select * from sp_start_fmask_l1_tile_processing(%(node_id)s);""",{"node_id" : node_id})
        tile_info = cursor.fetchone()
        return tile_info

    with db_config.connect() as connection:
        tile_info = handle_retries(connection, _run, log)
        log.debug("Unprocessed tile info: {}".format(tile_info))
        return tile_info

def db_clear_pending_tiles(db_config, node_id, log):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        cursor.execute("""select * from sp_clear_pending_fmask_tiles(%(node_id)s);""",{"node_id" : node_id})

    with db_config.connect() as connection:
        handle_retries(connection, _run, log)

def db_postrun_update(db_config, input_prod, fmask_prod, log):
    def _run(cursor):   
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

        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)

        # updating fmask_history
        if reason is not None:
            cursor.execute(
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
            cursor.execute(
                """SELECT * FROM sp_mark_fmask_l1_tile_done(%(downloader_history_id)s :: integer);""",
                                     {
                                         "downloader_history_id" : downloader_product_id
                                     },
            )

        # update product table
        if reason is None and (
            processing_status == DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
        ):
                cursor.execute(
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
                                        "quicklook_image" : None,
                                        "footprint" : footprint,
                                        "orbit_id" : None,
                                        "tiles" : None,
                                        "orbit_type_id" : None,
                                        "downloader_history_id" : downloader_product_id
                                    }
            )
    
    with db_config.connect() as connection:
        handle_retries(connection, _run, log)

def db_prerun_update(db_config, tile, reason, log):
    def _run(cursor):
        downloader_history_id = tile.downloader_history_id
        should_retry = False

        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)

        # updating  fmask_l1_tile_history
        cursor.execute(
            """SELECT * FROM sp_mark_fmask_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                      %(reason)s, 
                                                                                      %(should_retry)s :: boolean);""",
            {
                "downloader_history_id" : downloader_history_id,
                "reason" : reason,
                "should_retry" : should_retry
            }
        )

    with db_config.connect() as connection:
        handle_retries(connection, _run, log)

        
parser = argparse.ArgumentParser(description="Launcher for FMASK script")
parser.add_argument('-c', '--config', default="/etc/sen2agri/sen2agri.conf", help="configuration file")
parser.add_argument('-l', '--log-level', default = 'info',
                    choices = ['debug' , 'info', 'warning' , 'error', 'critical'], 
                    help = 'Minimum logging level')
args = parser.parse_args()
launcher_log_path = os.path.join(LAUNCHER_LOG_DIR, LAUNCHER_LOG_FILE_NAME)
launcher_log = LogHandler(launcher_log_path, "launcher_log", args.log_level, MASTER_ID)

# get the processing context
db_config = DBConfig.load(args.config, launcher_log)
default_processing_context = ProcessingContext()
db_get_processing_context(db_config, default_processing_context, DB_PROCESSOR_NAME, launcher_log)
if default_processing_context is None:
    msg = "Could not load the processing context from database" 
    launcher_log.critical(msg, print_msg = True)
    sys.exit(1)

if default_processing_context.num_workers< 1:
    msg = "Invalid processing context num_workers: {}".format(
                    default_processing_context.num_workers
    )
    launcher_log.critical(msg, print_msg = True)
    sys.exit(1)

# woking dir operations
# create working dir
if not create_recursive_dirs(
    default_processing_context.working_dir["default"]
):
    msg = "Could not create the work base directory {}".format(
            default_processing_context.working_dir["default"]
    )
    launcher_log.critical(msg, print_msg = True)
    sys.exit(1)

# delete all the temporary content from working dir from a previous run
remove_dir_content(default_processing_context.working_dir["default"])

#get node id
node_id = get_node_id()

# clear pending tiles
db_clear_pending_tiles(db_config, node_id, launcher_log)
fmask_master = FmaskMaster(default_processing_context.num_workers, db_config, node_id, launcher_log)
fmask_master.run()

if launcher_log.level != 'debug':
    remove_dir_content("{}/".format(default_processing_context.working_dir["default"]))
