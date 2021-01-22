#!/usr/bin/env python2
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
from __future__ import absolute_import
import argparse
import re
import os
import glob
import sys
import time
import datetime
import Queue
from osgeo import ogr
import threading
from bs4 import BeautifulSoup as Soup
import zipfile
import tarfile
import tempfile
import ntpath
from lxml import etree
import random
import psycopg2
from psycopg2.errorcodes import SERIALIZATION_FAILURE, DEADLOCK_DETECTED
import grp
import shutil
import subprocess
import signal
from l2a_commons import log, remove_dir, create_recursive_dirs, get_footprint, manage_log_file, remove_dir_content, run_command
from l2a_commons import UNKNOWN_SATELLITE_ID, SENTINEL2_SATELLITE_ID, LANDSAT8_SATELLITE_ID
from l2a_commons import DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE, DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
from l2a_commons import DEBUG, MAJA_LOG_DIR, MAJA_LOG_FILE_NAME, SEN2COR_LOG_DIR, SEN2COR_LOG_FILE_NAME
from l2a_commons import SEN2COR_PROCESSOR_OUTPUT_FORMAT, MACCS_PROCESSOR_OUTPUT_FORMAT, THEIA_MUSCATE_OUTPUT_FORMAT

MIN_VALID_PIXELS_THRESHOLD = 10.0
MAX_CLOUD_COVERAGE = 90.0
LAUNCHER_LOG_DIR = "/tmp/"
LAUNCHER_LOG_FILE_NAME = "l2a_launcher.log"
ARCHIVES_DIR_NAME = "archives"
SQL_MAX_NB_RETRIES = 3
MAJA_CONFIGURATION_FILE_NAME = "UserConfiguration"
DEFAULT_GDAL_IMAGE_NAME = "osgeo/gdal:ubuntu-full-3.2.0"
DEFAULT_DEM_IMAGE_NAME = "lnicola/sen2agri-dem"
DEFAULT_MAJA3_IMAGE_NAME = "lnicola/maja:3.2.2-centos-7"
DEFAULT_MAJA4_IMAGE_NAME = "lnicola/maja:4.2.1-centos-7"
DEFAULT_L8ALIGN_IMAGE_NAME = "lnicola/sen2agri-l8-alignment"
DEFAULT_SEN2COR_IMAGE_NAME = "lnicola/sen2cor:2.8.0-ubuntu-20.04"
DEFAULT_L2APROCESSORS_IMAGE_NAME = "lnicola/sen2agri-l2a-processors"


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
        return True

    def database_disconnect(self):
        if self.conn:
            self.conn.close()
            self.is_connected = False

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


class ProcessingContext(object):
    def __init__(self):
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.output_path = {"default": ""}
        self.dem_path = {"default": ""}
        self.swbd_path = {"default": ""}
        self.maja_launcher = {"default": ""}
        self.working_dir = {"default": ""}
        self.base_output_path = {"default": ""}
        self.compressTiffs = {"default": False}
        self.cogTiffs = {"default": False}
        self.removeSreFiles = {"default": False}
        self.removeFreFiles = {"default": False}
        self.implementation = {"default": None}
        self.num_workers = {"default": 0}
        self.sen2cor_gipp = {"default": ""}
        self.maja_gipp = {"default": ""}

    def add_parameter(self, row):
        if len(row) == 3 and row[0] is not None and row[2] is not None:
            parameter = row[0]
            site = row[1]
            value = row[2]
            if parameter == "processor.l2a.s2.implementation":
                if site is not None:
                    self.implementation[site] = value
                else:
                    self.implementation["default"] = value
            elif parameter == "processor.l2a.srtm-path":
                if site is not None:
                    self.dem_path[site] = value
                else:
                    self.dem_path["default"] = value
            elif parameter == "processor.l2a.swbd-path":
                if site is not None:
                    self.swbd_path[site] = value
                else:
                    self.swbd_path["default"] = value
            elif parameter == "processor.l2a.working-dir":
                if site is not None:
                    self.working_dir[site] = value
                else:
                    self.working_dir["default"] = value
            elif parameter == "processor.l2a.optical.output-path":
                if site is not None:
                    self.output_path[site] = value
                else:
                    self.output_path["default"] = value
            elif parameter == "processor.l2a.optical.num-workers":
                if site is not None:
                    self.num_workers[site] = int(value)
                else:
                    self.num_workers["default"] = int(value)
            elif parameter == "processor.l2a.optical.cog-tiffs":
                if (value == "1") or (value.lower() == "true"):
                    cogTiffs = True
                else:
                    cogTiffs = False
                if site is not None:
                    self.cogTiffs[site] = cogTiffs
                else:
                    self.cogTiffs["default"] = cogTiffs
            elif parameter == "processor.l2a.optical.compress-tiffs":
                if (value == "1") or (value.lower() == "true"):
                    compressTiffs = True
                else:
                    compressTiffs = False
                if site is not None:
                    self.compressTiffs[site] = compressTiffs
                else:
                    self.compressTiffs["default"] = compressTiffs
            elif parameter == "processor.l2a.maja.gipp-path":
                if site is not None:
                    self.maja_gipp[site] = value
                else:
                    self.maja_gipp["default"] = value
            elif parameter == "processor.l2a.maja.launcher":
                if site is not None:
                    self.maja_launcher[site] = value
                else:
                    self.maja_launcher["default"] = value
            elif parameter == "processor.l2a.maja.remove-fre":
                if (value == "1") or (value.lower() == "true"):
                    removeFreFiles = True
                else:
                    removeFreFiles = False
                if site is not None:
                    self.removeFreFiles[site] = removeFreFiles
                else:
                    self.removeFreFiles["default"] = removeFreFiles
            elif parameter == "processor.l2a.maja.remove-sre":
                if (value == "1") or (value.lower() == "true"):
                    removeSreFiles = True
                else:
                    removeSreFiles = False
                if site is not None:
                    self.removeSreFiles[site] = removeSreFiles
                else:
                    self.removeSreFiles["default"] = removeSreFiles
            elif parameter == "processor.l2a.sen2cor.gipp-path":
                if site is not None:
                    self.sen2cor_gipp[site] = value
                else:
                    self.sen2cor_gipp["default"] = value
            else:
                pass
        else:
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid database row: {}".format(row),
                LAUNCHER_LOG_FILE_NAME,
            )

    def get_site_context(self, site_id):
        site_context = SiteContext()
        site_context.base_abs_path = self.base_abs_path
        if site_id in self.output_path:
            site_context.output_path = self.output_path[site_id]
            site_context.base_output_path = site_context.output_path.replace(
                "{processor}", "l2a"
            )
        else:
            site_context.output_path = self.output_path["default"]
            site_context.base_output_path = site_context.output_path.replace(
                "{processor}", "l2a"
            )
        if site_id in self.dem_path:
            site_context.dem_path = self.dem_path[site_id]
        else:
            site_context.dem_path = self.dem_path["default"]
        if site_id in self.swbd_path:
            site_context.swbd_path = self.swbd_path[site_id]
        else:
            site_context.swbd_path = self.swbd_path["default"]
        if site_id in self.maja_launcher:
            site_context.maja_launcher = self.maja_launcher[site_id]
        else:
            site_context.maja_launcher = self.maja_launcher["default"]
        if site_id in self.working_dir:
            site_context.working_dir = self.working_dir[site_id]
        else:
            site_context.working_dir = self.working_dir["default"]
        if site_id in self.compressTiffs:
            site_context.compressTiffs = self.compressTiffs[site_id]
        else:
            site_context.compressTiffs = self.compressTiffs["default"]
        if site_id in self.cogTiffs:
            site_context.cogTiffs = self.cogTiffs[site_id]
        else:
            site_context.cogTiffs = self.cogTiffs["default"]
        if site_id in self.removeSreFiles:
            site_context.removeSreFiles = self.removeSreFiles[site_id]
        else:
            site_context.removeSreFiles = self.removeSreFiles["default"]
        if site_id in self.removeFreFiles:
            site_context.removeFreFiles = self.removeFreFiles[site_id]
        else:
            site_context.removeFreFiles = self.removeFreFiles["default"]
        if site_id in self.implementation:
            site_context.implementation = self.implementation[site_id]
        else:
            site_context.implementation = self.implementation["default"]
        if site_id in self.num_workers:
            site_context.num_workers = self.num_workers[site_id]
        else:
            site_context.num_workers = self.num_workers["default"]
        if site_id in self.sen2cor_gipp:
            site_context.sen2cor_gipp = self.sen2cor_gipp[site_id]
        else:
            site_context.sen2cor_gipp = self.sen2cor_gipp["default"]
        if site_id in self.maja_gipp:
            site_context.maja_gipp = self.maja_gipp[site_id]
        else:
            site_context.maja_gipp = self.maja_gipp["default"]
        site_context.maja_conf = os.path.join(site_context.maja_gipp, MAJA_CONFIGURATION_FILE_NAME)

        return site_context


class SiteContext(object):
    def __init__(self):
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.output_path = ""
        self.dem_path = ""
        self.swbd_path = ""
        self.maja_launcher = ""
        self.working_dir = ""
        self.base_output_path = ""
        self.compressTiffs = False
        self.cogTiffs = False
        self.removeSreFiles = False
        self.removeFreFiles = False
        self.implementation = None
        self.num_workers = 0
        self.sen2cor_gipp = ""
        self.maja_gipp = ""
        self.maja_conf = ""

    def is_valid(self):
        if len(self.output_path) == 0:
            print(
                "(launcher err) Invalid processing context output_path: {}.".format(
                    self.output_path
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context output_path: {}.".format(
                    self.output_path
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if self.num_workers < 1:
            print(
                "(launcher err) Invalid processing context num_workers: {}".format(
                    self.num_workers
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context num_workers: {}".format(
                    self.num_workers
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if not os.path.isdir(self.swbd_path):
            print(
                "(launcher err) Invalid processing context swbd_path: {}".format(
                    self.swbd_path
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context swbd_path: {}".format(
                    self.swbd_path
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if not os.path.isdir(self.dem_path):
            print(
                "(launcher err) Invalid processing context dem_path: {}".format(
                    self.dem_path
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context dem_path: {}".format(
                    self.dem_path
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if not os.path.isdir(self.working_dir) and not create_recursive_dirs(self.working_dir):
            print(
                "(launcher err) Invalid processing context working_dir: {}".format(
                    self.working_dir
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context working_dir: {}".format(
                    self.working_dir
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if self.implementation not in ["sen2cor", "maja"]:
            print(
                "(launcher err) Invalid processing context implementation: {}".format(
                    self.implementation
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context implementation: {}".format(
                    self.implementation
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if self.implementation == "sen2cor" and not os.path.isdir(self.sen2cor_gipp):
            print(
                "(launcher err) Invalid processing context sen2cor_gipp: {}".format(
                    self.sen2cor_gipp
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context sen2cor_gipp: {}".format(
                    self.sen2cor_gipp
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if self.implementation == "maja" and not os.path.isdir(self.maja_gipp):
            print(
                "(launcher err) Invalid processing context maja_gipp: {}".format(
                    self.maja_gipp
                )
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context maja_gipp: {}".format(
                    self.maja_gipp
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        if self.removeFreFiles and self.removeSreFiles:
            print(
                "(launcher err) Invalid processing context both removeFreFiles and removeSreFiles are True."
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid processing context both removeFreFiles and removeSreFiles are True.",
            )
            return False

        if self.implementation == "maja" and not os.path.isdir(self.maja_conf):
            print(
                "(launcher err) Invalid Maja configuration file {}.".format(self.maja_conf)
            )
            log(
                LAUNCHER_LOG_DIR,
                "(launcher err) Invalid Maja configuration file.".format(self.maja_conf),
            )
            return False

        return True


class MajaContext(object):
    def __init__(self, site_context, worker_id):
        self.dem_path = site_context.dem_path
        self.working_dir = site_context.working_dir
        self.processor_launch_path = site_context.maja_launcher
        self.swbd_path = site_context.swbd_path
        self.gips_path = site_context.maja_gipp
        self.worker_id = worker_id
        self.processor_log_dir = MAJA_LOG_DIR
        self.processor_log_file = MAJA_LOG_FILE_NAME
        self.removeFreFiles = site_context.removeFreFiles
        self.removeSreFiles = site_context.removeSreFiles
        self.compressTiffs = site_context.compressTiffs
        self.cogTiffs = site_context.cogTiffs
        self.maja_launcher = site_context.maja_launcher
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))
        self.conf = site_context.maja_conf


class Sen2CorContext(object):
    def __init__(self, processing_context, worker_id):
        self.dem_path = processing_context.dem_path
        self.working_dir = processing_context.working_dir
        self.processor_launch_path = processing_context.maja_launcher
        self.gips_path = processing_context.sen2cor_gipp
        self.worker_id = worker_id
        self.processor_log_dir = SEN2COR_LOG_DIR
        self.processor_log_file = SEN2COR_LOG_FILE_NAME
        self.compressTiffs = processing_context.compressTiffs
        self.cogTiffs = processing_context.cogTiffs
        self.base_abs_path = os.path.dirname(os.path.abspath(__file__))


class L2aMaster(object):
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.master_q = Queue.Queue(maxsize=self.num_workers)
        self.workers = []
        self.in_processing = set()
        for worker_id in range(self.num_workers):
            self.workers.append(L2aWorker(worker_id, self.master_q))
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
        for product_id in self.in_processing:
            container_types = ["l2a_processors", "dem", "l8align", "maja", "sen2cor", "gdal"]
            for product_id in self.in_processing:
                for container_type in container_types:
                    containers.append("{}_{}".format(container_type, product_id))

        if containers:
            print("\n(launcher info) <master>: Stoping containers")
            cmd.extend(containers)
            run_command(cmd, LAUNCHER_LOG_DIR, LAUNCHER_LOG_FILE_NAME)

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
                    db_postrun_update(msg_to_master.lin, msg_to_master.l2a)
                    self.in_processing.remove(msg_to_master.lin.product_id)
                sleeping_workers.append(msg_to_master.worker_id)
                while len(sleeping_workers) > 0:
                    unprocessed_tile = db_get_unprocessed_tile()
                    if unprocessed_tile is not None:
                        processing_context = db_get_processing_context()
                        site_context = processing_context.get_site_context(
                            unprocessed_tile.site_id
                        )
                        site_context_valid = site_context.is_valid()
                        unprocessed_tile.get_site_info(site_context.base_output_path)
                        tile_validity = unprocessed_tile.is_valid()

                        if tile_validity == False:
                            print(
                                "\n(launcher err) <master>: Product {} has invalid tile info.".format(
                                    unprocessed_tile.downloader_history_id
                                )
                            )
                            log(
                                LAUNCHER_LOG_DIR,
                                "(launcher err) <master>: Product {} has invalid tile info.".format(
                                    unprocessed_tile.downloader_history_id
                                ),
                            )
                            db_prerun_update(
                                unprocessed_tile, "Invalid tile information."
                            )
                            continue

                        if site_context_valid == False:
                            print(
                                "\n(launcher err) <master>: Product {} has invalid site info.".format(
                                    unprocessed_tile.downloader_history_id
                                )
                            )
                            log(
                                LAUNCHER_LOG_DIR,
                                "(launcher err) <master>: Product {} has invalid site info.".format(
                                    unprocessed_tile.downloader_history_id
                                ),
                            )
                            db_prerun_update(unprocessed_tile, "Invalid site context.")
                            continue

                        if tile_validity and site_context_valid:
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
            print("\n(launcher err) <master>: Exception encountered: {}.".format(e))
        finally:
            self.stop_workers()


class MsgToMaster(object):
    def __init__(self, worker_id, lin, l2a, update_db):
        self.worker_id = worker_id
        self.lin = lin
        self.l2a = l2a
        self.update_db = update_db


class MsgToWorker(object):
    def __init__(self, unprocessed_tile, site_context):
        self.unprocessed_tile = unprocessed_tile
        self.site_context = site_context


class L2aWorker(threading.Thread):
    def __init__(self, worker_id, master_q):
        super(L2aWorker, self).__init__()
        self.worker_id = worker_id
        self.master_q = master_q
        self.worker_q = Queue.Queue(maxsize=1)

    def notify_end_of_tile_processing(self, lin, l2a):
        notification = MsgToMaster(self.worker_id, lin, l2a, True)
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
                        "\n(launcher err) <worker {}>: Either the tile or site context is None".format(
                            self.worker_id
                        )
                    )
                    os._exit(1)
                else:
                    lin, l2a = self.process_tile(
                        msg_to_worker.unprocessed_tile, msg_to_worker.site_context
                    )
                    self.notify_end_of_tile_processing(lin, l2a)
                    self.worker_q.task_done()
        except Exception as e:
            print(
                "\n(launcher err) <worker {}>: Exception {} encountered".format(
                    self.worker_id, e
                )
            )
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
            "\n(launcher info) <worker {}>: orbit_id = {}".format(
                self.worker_id, tile.orbit_id
            )
        )
        print(
            "\n(launcher info) <worker {}>: tile_id = {}".format(
                self.worker_id, tile.tile_id
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
            "\n(launcher info) <worker {}>: previous_l2a_path = {}".format(
                self.worker_id, tile.previous_l2a_path
            )
        )
        print(
            "\n(launcher info) <worker {}>: site_short_name = {}".format(
                self.worker_id, tile.site_short_name
            )
        )
        print(
            "\n(launcher info) <worker {}>: site_output_path = {}".format(
                self.worker_id, tile.site_output_path
            )
        )

        if tile.satellite_id == LANDSAT8_SATELLITE_ID:
            # only MAJA can process L8 images
            maja_context = MajaContext(site_context, self.worker_id)
            maja = Maja(maja_context, tile)
            lin, l2a = maja.run()
            del maja
            return lin, l2a
        else:
            # Sentinels can be processed either with Sen2cor or MAJA
            if site_context.implementation == "sen2cor":
                sen2cor_context = Sen2CorContext(site_context, self.worker_id)
                sen2cor = Sen2Cor(sen2cor_context, tile)
                lin, l2a = sen2cor.run()
                del sen2cor
                return lin, l2a
            elif site_context.implementation == "maja":
                maja_context = MajaContext(site_context, self.worker_id)
                maja = Maja(maja_context, tile)
                lin, l2a = maja.run()
                del maja
                return lin, l2a
            else:
                log(
                    LAUNCHER_LOG_DIR,
                    "{}: Aborting processing for site {} because the processor name {} is not recognized".format(
                        self.worker_id, site_context.site_id, site_context.implementation
                    ),
                    LAUNCHER_LOG_FILE_NAME,
                )
                return None, None


class Tile(object):
    def __init__(self, tile_info):
        self.site_id = tile_info[0]
        self.satellite_id = tile_info[1]
        self.orbit_id = tile_info[2]
        self.tile_id = tile_info[3]
        self.downloader_history_id = tile_info[4]
        self.path = tile_info[5]
        self.previous_l2a_path = tile_info[6]
        self.site_short_name = ""
        self.site_output_path = ""

    def is_valid(self):
        if len(self.site_short_name) == 0:
            log(
                LAUNCHER_LOG_DIR,
                ": Aborting processing for tile {} because site short name {} is incorrect".format(
                    self.tile_id, self.site_short_name
                ),
                LAUNCHER_LOG_FILE_NAME,
            )
            return False

        return True

    def get_site_info(self, base_output_path):
        self.site_short_name = db_get_site_short_name(self.site_id)
        self.site_output_path = base_output_path.replace("{site}", self.site_short_name)


class L1CL8Product(object):
    def __init__(self, tile):
        self.site_id = tile.site_id
        self.satellite_id = tile.satellite_id
        self.orbit_id = tile.orbit_id
        self.tile_id = tile.tile_id
        self.product_id = tile.downloader_history_id
        self.db_path = tile.path
        self.previous_l2a_path = tile.previous_l2a_path
        self.site_output_path = tile.site_output_path
        self.site_short_name = tile.site_short_name
        self.was_archived = False
        self.path = None
        self.rejection_reason = None
        self.should_retry = True
        self.processing_status = None


class L2aProduct(object):
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
        self.orbit_id = None
        self.cloud_coverage_assessment = None
        self.snow_ice_percentage = None
        self.valid_pixels_percentage = None
        self.footprint = None
        self.basename = None
        self.output_format = None


class L2aProcessor(object):
    def __init__(self, processor_context, unprocessed_tile):
        self.context = processor_context
        self.lin = L1CL8Product(unprocessed_tile)
        self.l2a = L2aProduct()
        self.l2a_log_file = None

    def __del__(self):
        if self.lin.was_archived and os.path.exists(self.lin.path):
            remove_dir(self.lin.path)

    def l2a_log(self, log_msg):
        log_msg = "<worker {}>: ".format(self.context.worker_id) + log_msg
        log(self.l2a.output_path, log_msg, self.l2a_log_file)

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
            "{}: Starting extract_from_archive_if_needed for tile {}".format(
                self.context.worker_id, self.lin.tile_id
            )
        )
        self.lin.was_archived, self.lin.path = self.extract_from_archive_if_needed(
            self.lin.db_path
        )
        self.launcher_log(
                "{}: Ended extract_from_archive_if_needed for tile {}".format(
                    self.context.worker_id, self.lin.tile_id
                )
            )
        if self.lin.path is not None:
            return self.validate_input_product_dir()
        else:
            return False

    def get_l2a_info(self, product_name):
        acquisition_date = None
        satellite_id = UNKNOWN_SATELLITE_ID
        if product_name.startswith("S2"):
            m = re.match(r"\w+_V(\d{8}T\d{6})_\w+.SAFE", product_name)
            # check if the new convention naming aplies
            if m is None:
                m = re.match(r"\w+_(\d{8}T\d{6})_\w+.SAFE", product_name)
            if m is not None:
                satellite_id = SENTINEL2_SATELLITE_ID
                acquisition_date = m.group(1)
        elif product_name.startswith("LC8"):
            m = re.match(r"LC8\d{6}(\d{7})[A-Z]{3}\d{2}", product_name)
            if m is not None:
                acquisition_date = datetime.datetime.strptime(
                    "{} {}".format(m.group(1)[0:4], m.group(1)[4:]), "%Y %j"
                ).strftime("%Y%m%dT%H%M%S")
                satellite_id = LANDSAT8_SATELLITE_ID
        elif product_name.startswith("LC08"):
            m = re.match(
                r"LC08_[A-Z0-9]+_\d{6}_(\d{8})_\d{8}_\d{2}_[A-Z0-9]{2}", product_name
            )
            if m is not None:
                acquisition_date = "{}T000000".format(m.group(1))
            if m is not None:
                satellite_id = LANDSAT8_SATELLITE_ID

        return satellite_id, acquisition_date

    def l2a_setup(self):
        # determine the name of the L2A output dir
        name_determined = True
        if self.lin.path.endswith("/"):
            lin_basename = os.path.basename(self.lin.path[:-1])
        else:
            lin_basename = os.path.basename(self.lin.path)
        if lin_basename.startswith("S2"):
            l2a_basename = lin_basename.replace("L1C", "L2A")
        elif lin_basename.startswith("LC8"):
            l2a_basename = lin_basename + "_L2A"
        elif lin_basename.startswith("LC08"):
            if lin_basename.find("_L1TP_") > 0:
                l2a_basename = lin_basename.replace("_L1TP_", "_L2A_")
            elif lin_basename.find("_L1GS_") > 0:
                l2a_basename = lin_basename.replace("_L1GS_", "_L2A_")
            elif lin_basename.find("_L1GT_") > 0:
                l2a_basename = lin_basename.replace("_L1GT_", "_L2A_")
            else:
                self.update_rejection_reason(
                    "The input product name is wrong - L2A cannot be filled: {}".format(
                        lin_basename
                    )
                )
                self.launcher_log(
                    "The input product name is wrong - L2A cannot be filled: {}".format(
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
        self.l2a.basename = l2a_basename

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

        # determine the path of the l2a product
        l2a_output_path = os.path.join(
            self.lin.site_output_path, "output", l2a_basename
        )
        l2a_destination_path = os.path.join(
            self.lin.site_output_path, acq_year, acq_month, acq_day, l2a_basename
        )
        if not create_recursive_dirs(l2a_output_path):
            self.update_rejection_reason(
                "Can NOT create the output directory: {}".format(l2a_output_path)
            )
            self.launcher_log(
                "Can NOT create the output directory: {}".format(l2a_output_path)
            )
            return False

        self.l2a.output_path = l2a_output_path
        self.l2a.destination_path = l2a_destination_path
        self.l2a.satellite_id = self.lin.satellite_id
        self.l2a.site_id = self.lin.site_id
        self.l2a.product_id = self.lin.product_id
        self.l2a.orbit_id = self.lin.orbit_id
        self.l2a_log_file = "l2a_{}.log".format(self.lin.tile_id)
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
            dst = os.path.join(self.l2a.destination_path, os.pardir)
            shutil.rmtree(self.l2a.destination_path, ignore_errors=True)
            if not os.path.isdir(dst):
                create_recursive_dirs(dst)
            os.rename(self.l2a.output_path, self.l2a.destination_path)
        except Exception as e:
            self.update_rejection_reason("Can NOT copy from output path {} to destination product path {} due to: {}".format(self.l2a.output_path, self.l2a.destination_path, e))

class Maja(L2aProcessor):
    def __init__(self, processor_context, input_context):
        super(Maja, self).__init__(processor_context, input_context)
        self.name = "maja"
        self.eef_available = False

    def get_l2a_footprint(self):
        wgs84_extent_list = []
        tile_img = ""

        if self.l2a.output_format == MACCS_PROCESSOR_OUTPUT_FORMAT:
            if self.lin.satellite_id == SENTINEL2_SATELLITE_ID:
                footprint_tif_pattern = "**/*_R1.DBL.TIF"
            elif self.lin.satellite_id == LANDSAT8_SATELLITE_ID:
                footprint_tif_pattern = "**/*.DBL.TIF"
            else:
                self.update_rejection_reason(
                    "Can NOT create the footprint, invalid satelite id."
                )
                self.l2a_log("Can NOT create the footprint, invalid satelite id.")
                return False
        elif self.l2a.output_format == THEIA_MUSCATE_OUTPUT_FORMAT:
            footprint_tif_pattern = "**/*_B2.tif"
        else:
            self.update_rejection_reason(
                    "Can NOT create the footprint, invalid output format."
                )
            self.l2a_log("Can NOT create the footprint, invalid output format.")
            return False

        footprint_tif_path = os.path.join(self.l2a.output_path, footprint_tif_pattern)
        tile_img = glob.glob(footprint_tif_path)

        if len(tile_img) > 0:
            wgs84_extent_list.append(get_footprint(tile_img[0])[0])
            self.l2a_log("MAJA common footprint tif file: {}".format(tile_img))
            self.l2a.footprint = self.get_envelope(wgs84_extent_list)
            return True

        self.update_rejection_reason(
            "Can NOT create the footprint, no FRE tif file exists."
        )
        self.l2a_log("Can NOT create the footprint, no FRE tif file exists.")
        return False

    def get_quality_indicators(self):
        maja_report_file_name = "MACCS_L2REPT_{}.EEF".format(self.lin.tile_id)
        maja_report_file_path = os.path.join(
            self.l2a.output_path, maja_report_file_name
        )
        if os.path.isfile(maja_report_file_path):
            #up to version 3.2.2 of maja the cloud coverage was present in the eef file
            cloud_coverage_acq = False
            snow_coverage_acq = False
            try:
                xml_handler = open(maja_report_file_path).read()
                soup = Soup(xml_handler, "lxml")
                for message in soup.find_all("message"):
                    msg_text = message.find("text").get_text()
                    if re.search(
                        "cloud percentage computed is", msg_text, re.IGNORECASE
                    ):
                        numbers = [int(s) for s in re.findall(r"\d+", msg_text)]
                        if len(numbers) > 0:
                            self.l2a.cloud_coverage_assessment = numbers[0]
                            cloud_coverage_acq = True
                    if re.search(
                        "snow percentage computed is", msg_text, re.IGNORECASE
                    ):
                        numbers = [int(s) for s in re.findall(r"\d+", msg_text)]
                        if len(numbers) > 0:
                            self.l2a.snow_ice_percentage = numbers[0]
                            snow_coverage_acq = True
            except Exception as e:
                self.l2a_log(
                    "Exception received when trying to read  file {} due to: {}".format(
                        maja_report_file_path, e
                    )
                )

            if not cloud_coverage_acq:
                self.l2a_log(
                    "Can NOT extract cloud coverage from {}".format(maja_report_file_path)
                )

            if not snow_coverage_acq:
                self.l2a_log(
                    "Can NOT extract snow ice coverage from {}".format(
                        maja_report_file_path
                    )
                )
        else:
            #starting with maja 4.2.1 the cloud coverage is read from maja.log, not from eef
            maja_log_path = os.path.join(self.l2a.output_path,"maja.log")
            if os.path.isfile(maja_log_path):
                with open(maja_log_path) as log_file:
                    for line in log_file:
                        res = re.findall(r"Cloud Rate on the Product : (\d*[.]?\d+)",line)
                        if len(res) == 1:
                            self.l2a.cloud_coverage_assessment = float(res[0])


    def check_report_file(self):
        maja_report_file_name = "MACCS_L2REPT_{}.EEF".format(self.lin.tile_id)
        maja_report_file_path = os.path.join(
            self.l2a.output_path, maja_report_file_name
        )
        if os.path.isfile(maja_report_file_path):
            try:
                xml_handler = open(maja_report_file_path).read()
                soup = Soup(xml_handler, "lxml")
                for message in soup.find_all("message"):
                    msg_type = message.find("type").get_text()
                    msg_text = message.find("text").get_text()
                    if msg_type == "W" or msg_type == "E":
                        self.update_rejection_reason(msg_text)
                    if msg_type == "I" and re.search(
                        "code return: 0", msg_text, re.IGNORECASE
                    ):
                        pass
            except Exception as e:
                self.update_rejection_reason(
                    "Exception received when trying to read file {}: {}".format(
                        maja_report_file_path, e
                    )
                )
                self.l2a_log(
                    "Exception received when trying to read file {}: {}".format(
                        maja_report_file_path, e
                    )
                )

    def check_jpi_file(self):
        jpi_file_pattern = "*_JPI_ALL.xml"
        jpi_file_path = os.path.join(self.l2a.output_path, jpi_file_pattern)
        tmp_jpi = glob.glob(jpi_file_path)
        if len(tmp_jpi) > 0:
            jpi_file = tmp_jpi[0]
            try:
                # Normally, if the file exists here it would be enough but we check just to be sure that we also have L2NOTV
                xml_handler = open(jpi_file).read()
                soup = Soup(xml_handler, "lxml")
                for message in soup.find_all("processing_flags_and_modes"):
                    key = message.find("key").get_text()
                    if key == "Validity_Flag":
                        value = message.find("value").get_text()
                        if value == "L2NOTV":
                            self.l2a_log(
                                "L2NOTV found in the MAJA JPI file {}. The product will not be retried ... ".format(
                                    jpi_file
                                )
                            )
                            self.lin.should_retry = False
                            self.update_rejection_reason(
                                "L2NOTV found in the MAJA JPI file {}. The product will not be retried!".format(
                                    jpi_file
                                )
                            )
            except Exception as e:
                self.update_rejection_reason(
                    "Exception received when trying to read the MAJA JPI from file {}: {}".format(
                        jpi_file, e
                    )
                )
                self.l2a_log(
                    "Exception received when trying to read the MAJA JPI from file {}: {}".format(
                        jpi_file, e
                    )
                )

    def check_l2a_log(self):
        tile_log_filename = "l2a_{}.log".format(self.lin.tile_id)
        tile_log_filepath = os.path.join(self.l2a.output_path, tile_log_filename)
        try:
            with open(tile_log_filepath) as in_file:
                contents = in_file.readlines()
                for line in contents:
                    index = line.find("Tile failure: ")
                    if index != -1:
                        self.update_rejection_reason(line[index + 14 :])
                        break
        except IOError as e:
            self.update_rejection_reason(
                "Could not read l2a product log at {}".format(tile_log_filepath)
            )

    def create_mosaic(self):
        mosaic = ""
        if self.l2a.output_format == MACCS_PROCESSOR_OUTPUT_FORMAT:
            qkl_pattern = "*DBL.JPG"
        elif self.l2a.output_format == THEIA_MUSCATE_OUTPUT_FORMAT:
            qkl_pattern = "*QKL*.jpg"
        else:
            self.update_rejection_reason("Can NOT find QKL file, invalid output format.")
            self.l2a_log("Can NOT find QKL file, invalid output format.")
            return False
        qkl_search_path = os.path.join(self.l2a.product_path, qkl_pattern)
        qkl_files = glob.glob(qkl_search_path)
        if (len(qkl_files) == 1) and os.path.isfile(qkl_files[0]):
            mosaic = os.path.join(self.l2a.output_path, "mosaic.jpg")
            qkl = qkl_files[0]
            try:
                shutil.copy(qkl, mosaic)
                return True
            except:
                self.update_rejection_reason(
                    "Can NOT copy QKL {} to {}".format(qkl, mosaic)
                )
                self.l2a_log("Can NOT copy QKL {} to {}".format(qkl, mosaic))
                return False
        else:
            self.update_rejection_reason("Can NOT find QKL file.")
            self.l2a_log("Can NOT find QKL file.")
            return False

    def check_theia_muscate_format(self):
        if self.l2a.product_path.endswith("/"):
            search_pattern = self.l2a.product_path + "*"
        else:
            search_pattern = self.l2a.product_path + "/*"
        dir_content = glob.glob(search_pattern)
        atb_files_count = 0
        fre_files_count = 0
        sre_files_count = 0
        qkl_file = False
        mtd_file = False
        data_dir = False
        masks_dir = False
        for filename in dir_content:
            if (
                os.path.isfile(filename)
                and re.search(
                    r"_L2A_.*ATB.*\.tif$",
                    filename,
                    re.IGNORECASE,
                )
                is not None
            ):
                atb_files_count += 1
            if (
                os.path.isfile(filename)
                and re.search(
                    r"_L2A_.*FRE.*\.tif$",
                    filename,
                    re.IGNORECASE,
                )
                is not None
            ):
                fre_files_count += 1
            if (
                os.path.isfile(filename)
                and re.search(
                    r"_L2A_.*SRE.*\.tif$",
                    filename,
                    re.IGNORECASE,
                )
                is not None
            ):
                sre_files_count += 1
            if (
                os.path.isfile(filename)
                and re.search(
                    r"_L2A_.*MTD.*\.xml$",
                    filename,
                    re.IGNORECASE,
                )
                is not None
            ):
                mtd_file = True
            if (
                os.path.isfile(filename)
                and re.search(
                    r"_L2A_.*QKL.*\.jpg$",
                    filename,
                    re.IGNORECASE,
                )
                is not None
            ):
                qkl_file = True
            if os.path.isdir(filename) and re.search(r".*\DATA$", filename) is not None:
                data_dir = True
            if os.path.isdir(filename) and re.search(r".*\MASKS$", filename) is not None:
                masks_dir = True

        if atb_files_count == 0:
            self.update_rejection_reason("Can NOT find ATB files.")
            self.l2a_log("Can NOT find ATB files.")
            return False
        if (fre_files_count == 0) and (self.context.removeFreFiles == False):
            self.update_rejection_reason("Can NOT find FRE files.")
            self.l2a_log("Can NOT find FRE files.")
            return False
        if (sre_files_count == 0) and (self.context.removeSreFiles == False):
            self.update_rejection_reason("Can NOT find SRE files.")
            self.l2a_log("Can NOT find SRE files.")
            return False
        if qkl_file == False:
            self.update_rejection_reason("Can NOT find QKL files.")
            self.l2a_log("Can NOT find QKL files.")
            return False
        if mtd_file == False:
            self.update_rejection_reason("Can NOT find MTD files.")
            self.l2a_log("Can NOT find MTD files.")
            return False
        if data_dir == False:
            self.update_rejection_reason("Can NOT find DATA dir.")
            self.l2a_log("Can NOT find DATA dir.")
            return False
        if masks_dir == False:
            self.update_rejection_reason("Can NOT find MASKS dir.")
            self.l2a_log("Can NOT find MASKS dir.")
            return False

        return True

    def get_rejection_reason(self):
        self.check_report_file()
        self.check_jpi_file()
        self.check_l2a_log()

    def get_output_format(self):
        # check for MACCS format
        maccs_dbl_dir_pattern = "*_L2VALD_{}*.DBL.DIR".format(self.lin.tile_id)
        maccs_dbl_dir_path = os.path.join(self.l2a.output_path, maccs_dbl_dir_pattern)
        maccs_dbl_dir = glob.glob(maccs_dbl_dir_path)
        maccs_hdr_file_pattern = "*_L2VALD_{}*.HDR".format(self.lin.tile_id)
        maccs_hdr_file_path = os.path.join(self.l2a.output_path, maccs_hdr_file_pattern)
        maccs_hdr_file = glob.glob(maccs_hdr_file_path)
        if len(maccs_dbl_dir) >= 1 and len(maccs_hdr_file) >= 1:
            self.l2a.output_format = MACCS_PROCESSOR_OUTPUT_FORMAT

        # check for THEIA/MUSCATE format
        theia_muscate_dir_pattern = "*_L2A_*"
        theia_muscate_dir_path = os.path.join(
            self.l2a.output_path, theia_muscate_dir_pattern
        )
        theia_muscate_dir = glob.glob(theia_muscate_dir_path)
        if len(theia_muscate_dir) >= 1:
            self.l2a.output_format = THEIA_MUSCATE_OUTPUT_FORMAT

    def check_l2a(self, run_script_ok):
        #check the processed l2a product
        l2a_found = False

        #get and check the product acquistion date and satelite id
        if self.l2a.output_path.endswith("/"):
            tmp_path = self.l2a.output_path[:-1]
        else:
            tmp_path = self.l2a.output_path
        l2a_product_name = os.path.basename(tmp_path)
        satellite_id, acquisition_date = self.get_l2a_info(l2a_product_name)
        if acquisition_date is None:
            self.update_rejection_reason("Acquisition date could not be retrieved.")
            self.l2a_log("Acquisition date could not be retrieved.")
            return False
        else:
            self.l2a.acquisition_date = acquisition_date
        if satellite_id == UNKNOWN_SATELLITE_ID:
            self.update_rejection_reason("UNKNOWN SATELLITE ID: {}.".format(satellite_id))
            self.l2a_log("UNKNOWN SATELLITE ID: {}.".format(satellite_id))
            return False

        #determine the l2a product format
        self.get_output_format()
        #based on the output format determine l2a product name, product_path and tile
        if self.l2a.output_format == MACCS_PROCESSOR_OUTPUT_FORMAT:
            tile_dir_list_pattern = "*.DBL.DIR"
            tile_dir_list_path = os.path.join(
                self.l2a.output_path, tile_dir_list_pattern
            )
            tile_dbl_dir = glob.glob(tile_dir_list_path)[0]
            tile = None
            if self.lin.satellite_id == SENTINEL2_SATELLITE_ID:
                tile = re.search(r"_L2VALD_(\d\d[a-zA-Z]{3})____[\w\.]+$", tile_dbl_dir)
            elif self.lin.satellite_id == LANDSAT8_SATELLITE_ID:
                tile = re.search(r"_L2VALD_([\d]{6})_[\w\.]+$", tile_dbl_dir)
            if (tile is not None) and (tile.group(1) == self.lin.tile_id):
                self.l2a.processed_tiles.append(self.lin.tile_id)
                self.l2a.product_path = tile_dbl_dir
                self.l2a.name = os.path.basename(self.l2a.output_path)
                l2a_found = True
            else:
                self.update_rejection_reason("None or multiple tiles were processed.")
                self.l2a_log("None or multiple tiles were processed.")
        elif self.l2a.output_format == THEIA_MUSCATE_OUTPUT_FORMAT:
            if satellite_id == LANDSAT8_SATELLITE_ID:
                name_pattern = "*_[CHD]_V*"
            elif satellite_id == SENTINEL2_SATELLITE_ID:
                name_pattern = "*_T{}_[CHD]_V*".format(self.lin.tile_id)
            else:
                self.update_rejection_reason("Invalid satelite id.")
                self.l2a_log("Invalid satelite id.")
                return False
            search_path = os.path.join(self.l2a.output_path, name_pattern)
            l2a_products = glob.glob(search_path)
            if len(l2a_products) == 1 and os.path.isdir(l2a_products[0]):
                self.l2a.name = os.path.basename(self.l2a.output_path)
                self.l2a.product_path = l2a_products[0]
                if self.check_theia_muscate_format() == True:
                    self.l2a.processed_tiles.append(self.lin.tile_id)
                    l2a_found = True
            else:
                self.update_rejection_reason("None or multiple tiles were processed.")
                self.l2a_log("None or multiple tiles were processed.")
        else:
            self.update_rejection_reason("Invalid output format.")
            self.l2a_log("Invalid output format.")

        #get the cloud and/or snow coverage
        self.get_quality_indicators()

        if l2a_found and run_script_ok:
            return True
        else:
            self.get_rejection_reason()
            return False

    def run_script(self):
        prev_l2a_tiles_paths = []
        prev_l2a_tiles = []
        if self.lin.previous_l2a_path is not None:
            prev_l2a_tiles.append(self.lin.tile_id)
            prev_l2a_tiles_paths.append(self.lin.previous_l2a_path)

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
        maja_general_log_path = os.path.join(MAJA_LOG_DIR, MAJA_LOG_FILE_NAME)
        script_command.append("-v")
        script_command.append("{}:{}".format(maja_general_log_path, maja_general_log_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.dem_path, self.context.dem_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.swbd_path, self.context.swbd_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.conf, self.context.conf))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.gips_path, self.context.gips_path))
        if len(prev_l2a_tiles_paths) > 0:
            for path in prev_l2a_tiles_paths:
                script_command.append("-v")
                script_command.append("{}:{}".format(path, path))

        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.working_dir, self.context.working_dir))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.lin.path, self.lin.path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.l2a.output_path, self.l2a.output_path))
        script_command.append("--name")
        script_command.append("l2a_processors_{}".format(self.lin.product_id))
        script_command.append(DEFAULT_L2APROCESSORS_IMAGE_NAME)

        script_name = "maja.py"
        script_path = os.path.join("/usr/share/l2a_processors/", script_name)
        script_command.append(script_path)
        script_command.append("--dem")
        script_command.append(self.context.dem_path)
        script_command.append("--swbd")
        script_command.append(self.context.swbd_path)
        script_command.append("--conf")
        script_command.append(self.context.conf)
        script_command.append("--processes-number-dem")
        script_command.append("1")
        script_command.append("--processes-number-maccs")
        script_command.append("1")
        script_command.append("--gipp-dir")
        script_command.append(self.context.gips_path)
        script_command.append("--working-dir")
        script_command.append(self.context.working_dir)
        script_command.append("--maccs-launcher")
        script_command.append(self.context.maja_launcher)
        script_command.append("--delete-temp")
        script_command.append("--product-id")
        script_command.append("{}".format(self.lin.product_id))
        script_command.append(self.lin.path)
        script_command.append(self.l2a.output_path)
        if len(self.lin.tile_id) > 0:
            script_command.append("--tiles-to-process")
            tiles = []
            tiles.append(self.lin.tile_id)
            script_command += tiles

        if len(prev_l2a_tiles) > 0:
            script_command.append("--prev-l2a-tiles")
            script_command += prev_l2a_tiles
            script_command.append("--prev-l2a-products-paths")
            script_command += prev_l2a_tiles_paths

        if self.context.removeSreFiles:
            script_command.append("--removeSreFiles")

        if self.context.removeFreFiles:
            script_command.append("--removeFreFiles")

        if self.context.compressTiffs:
            script_command.append("--compressTiffs")

        if self.context.cogTiffs:
            script_command.append("--cogTiffs")

        script_command.append("--docker-image-l8align")
        script_command.append(DEFAULT_L8ALIGN_IMAGE_NAME)
        script_command.append("--docker-image-dem")
        script_command.append(DEFAULT_DEM_IMAGE_NAME)
        script_command.append("--docker-image-maja3")
        script_command.append(DEFAULT_MAJA3_IMAGE_NAME)
        script_command.append("--docker-image-maja4")
        script_command.append(DEFAULT_MAJA4_IMAGE_NAME)
        script_command.append("--docker-image-gdal")
        script_command.append(DEFAULT_GDAL_IMAGE_NAME)

        majalog_path = os.path.join(self.l2a.output_path,"maja.log")
        majalog_file = open(majalog_path, "w")
        command_string =""
        for argument in script_command:
            command_string = command_string + " " + str(argument)
        self.l2a_log("Running command: {}".format(command_string))
        print("Running Maja, console output can be found at {}".format(majalog_path))
        command_return = subprocess.call(script_command, stdout = majalog_file, stderr = majalog_file)

        if (command_return == 0) and os.path.isdir(self.l2a.output_path):
            return True
        else:
            self.update_rejection_reason(
                "Can NOT run MAJA script, error code: {}.".format(command_return)
            )
            self.l2a_log(
                "Can NOT run MAJA script, error code: {}.".format(command_return)
            )
            return False


    def manage_prods_status(
        self, preprocess_succesful, process_succesful, l2a_ok):
        #if the following the messages are encountered within the rejection reasons the l2a product should not be processed again
        maja_text_to_stop_retrying = [
            "The number of cloudy pixel is too high",
            "algorithm processing is stopped",
            "The dark surface reflectance associated to the value of AOT index min is lower than the dark surface reflectance threshold",
            "The number of NoData pixel in the output L2 composite product is too high",
            "PersistentStreamingConditionalStatisticsImageFilter::Synthetize.No pixel is valid. Return null statistics",
        ]

        if (
            (preprocess_succesful == True)
            and (process_succesful == True)
            and (l2a_ok == True)
        ):
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
            self.move_to_destination()
        else:
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
            for text in maja_text_to_stop_retrying:
                if text in self.lin.rejection_reason:
                    self.lin.should_retry = False
                    break

            if self.l2a.cloud_coverage_assessment > MAX_CLOUD_COVERAGE:
                self.lin.should_retry = False

    def run(self):
        preprocess_succesful = False
        process_succesful = False
        postprocess_succesful = False
        l2a_ok = False

        # pre-processing
        if self.check_lin() and self.l2a_setup():
            preprocess_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful pre-processing = {}".format(
                self.context.worker_id, preprocess_succesful
            )
        )
        self.l2a_log("Successful pre-processing = {}".format(preprocess_succesful))

        # processing
        if preprocess_succesful:
            process_succesful = self.run_script()
        print(
            "\n(launcher info) <worker {}>: Successful processing = {}".format(
                self.context.worker_id, process_succesful
            )
        )
        self.l2a_log("Successful processing = {}".format(process_succesful))

        # processing checks
        l2a_ok = self.check_l2a(process_succesful)
        print(
            "\n(launcher info) <worker {}>: Valid L2a product = {}".format(
                self.context.worker_id, l2a_ok
            )
        )
        self.l2a_log("Valid L2a product = {}".format(l2a_ok))

        #post-processing
        if l2a_ok and self.get_l2a_footprint():
            print(
                "\n(launcher info) <worker {}>: Footprint computed: {}".format(
                    self.context.worker_id, self.l2a.footprint)
                )
            self.l2a_log("Footprint computed: {}".format(self.l2a.footprint))

        self.manage_prods_status(preprocess_succesful, process_succesful, l2a_ok)
        return self.lin, self.l2a


class Sen2Cor(L2aProcessor):
    def __init__(self, processor_context, input_context):
        super(Sen2Cor, self).__init__(processor_context, input_context)
        self.name = "sen2cor"

    def get_l2a_footprint(self):
        wgs84_extent_list = []
        footprint_files = []

        B02_R10_file_pattern = "GRANULE/L2A*/IMG_DATA/R10m/*_B02_10m.tif"
        B02_R10_path_pattern = os.path.join(self.l2a.product_path, B02_R10_file_pattern)
        footprint_files.extend(glob.glob(B02_R10_path_pattern))
        B02_R20_file_pattern = "GRANULE/L2A*/IMG_DATA/R20m/*_B02_20m.tif"
        B02_R20_path_pattern = os.path.join(self.l2a.product_path, B02_R20_file_pattern)
        footprint_files.extend(glob.glob(B02_R20_path_pattern))
        B02_R60_file_pattern = "GRANULE/L2A*/IMG_DATA/R60m/*_B02_60m.tif"
        B02_R60_path_pattern = os.path.join(self.l2a.product_path, B02_R60_file_pattern)
        footprint_files.extend(glob.glob(B02_R60_path_pattern))

        if len(footprint_files) > 0:
            footprint_file = os.path.abspath(footprint_files[0])
            self.l2a_log("Sen2Cor common footprint file: {}".format(footprint_file))
            wgs84_extent_list.append(get_footprint(footprint_file)[0])
            self.l2a.footprint = self.get_envelope(wgs84_extent_list)
            return True
        else:
            self.update_rejection_reason(
                "Can NOT create the footprint, no B02_10m/20m/60m.tif file exists."
            )
            self.l2a_log(
                "Can NOT create the footprint, no B02_10m/20m/60m.tif file exists."
            )
            return False

    def get_quality_indicators(self):
        # extract snow coverage and cloud coverage information
        mtd_name = "MTD_MSIL2A.xml"
        mtd_path = os.path.join(self.l2a.product_path, mtd_name)
        if os.path.isfile(mtd_path):
            try:
                tree = etree.parse(mtd_path)
                ns = "{https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-2A.xsd}"
                quality_indicators_info = tree.find(ns + "Quality_Indicators_Info")
                cloud_coverage_assessment = float(
                    quality_indicators_info.findtext("Cloud_Coverage_Assessment")
                )
                self.l2a.cloud_coverage_assessment = cloud_coverage_assessment
                image_content_qi = quality_indicators_info.find("Image_Content_QI")
                nodata_pixel_percentage = float(
                    image_content_qi.findtext("NODATA_PIXEL_PERCENTAGE")
                )
                saturated_defective_pixel_percentage = float(
                    image_content_qi.findtext("SATURATED_DEFECTIVE_PIXEL_PERCENTAGE")
                )
                dark_features_percentage = float(
                    image_content_qi.findtext("DARK_FEATURES_PERCENTAGE")
                )
                cloud_shadow_percentage = float(
                    image_content_qi.findtext("CLOUD_SHADOW_PERCENTAGE")
                )
                snow_ice_percentage = float(
                    image_content_qi.findtext("SNOW_ICE_PERCENTAGE")
                )
                self.l2a.snow_ice_percentage = snow_ice_percentage
                self.l2a.valid_pixels_percentage = (
                    (100 - nodata_pixel_percentage)
                    / 100.0
                    * (
                        100
                        - cloud_coverage_assessment
                        - saturated_defective_pixel_percentage
                        - dark_features_percentage
                        - cloud_shadow_percentage
                        - snow_ice_percentage
                    )
                )
            except:
                self.l2a_log(
                    "Can NOT parse {} for quality indicators extraction.".format(
                        mtd_path
                    )
                )
        else:
            self.l2a_log(
                "Can NOT find MTD_MSIL2A.xml file in location.".format(mtd_path)
            )

    def get_rejection_reason(self):
        log_path = os.path.join(
            self.context.processor_log_dir, self.context.processor_log_file
        )
        if os.path.isfile(log_path):
            try:
                with open(log_path, "rb") as file:
                    file.seek(-2, os.SEEK_END)
                    while file.read(1) != b"\n":
                        file.seek(-2, os.SEEK_CUR)
                    rejection_reason = file.readline().decode()
            except IOError as e:
                rejection_reason = "Can NOT read from sen2cor log file {} to get the rejection reason".format(
                    log_path
                )
                self.l2a_log(
                    "Can NOT read from sen2cor log file {} to get the rejection reason".format(
                        log_path
                    )
                )
        else:
            rejection_reason = (
                "Can NOT find sen2cor log file {} to get the rejection reason".format(
                    log_path
                )
            )
            self.l2a_log(
                "Can NOT find sen2cor log file {} to get the rejection reason".format(
                    log_path
                )
            )

        return rejection_reason

    def create_mosaic(self):
        # Copy the PVI file to the output path as mosaic.jpg
        pvi_pattern = "GRANULE/L2A*_T{}_*/QI_DATA/*PVI.jpg".format(
            self.l2a.processed_tiles[0]
        )
        pvi_path = os.path.join(self.l2a.product_path, pvi_pattern)
        pvi_files = glob.glob(pvi_path)
        if len(pvi_files) == 1 and os.path.isfile(pvi_files[0]):
            mosaic = os.path.join(self.l2a.output_path, "mosaic.jpg")
            try:
                shutil.copy(pvi_files[0], mosaic)
                return True
            except:
                self.update_rejection_reason(
                    "Can NOT copy PVI {} to {}".format(pvi_files[0], mosaic)
                )
                self.l2a_log("Can NOT copy PVI {} to {}".format(pvi_files[0], mosaic))
                return False
        else:
            self.update_rejection_reason(
                "Can NOT indentify a PVI image to create mosaic.jpg ."
            )
            self.l2a_log("Can NOT indentify a PVI image to create mosaic.jpg .")
            return False

    def check_l2a(self):
        l2a_product_name_pattern = "S2[A|B|C|D]_MSIL2A_*_T{}_*.SAFE".format(
            self.lin.tile_id
        )
        l2a_product_path = os.path.join(self.l2a.output_path, l2a_product_name_pattern)
        l2a_products = glob.glob(l2a_product_path)
        if len(l2a_products) == 1:
            if l2a_products[0].endswith("/"):
                l2a_product_name = os.path.basename(l2a_products[0][:-1])
            else:
                l2a_product_name = os.path.basename(l2a_products[0])

            satellite_id, acquisition_date = self.get_l2a_info(l2a_product_name)
            if self.lin.satellite_id != satellite_id:
                self.update_rejection_reason(
                    "L2A and input product have different satellite ids: {} vs {} .".format(
                        satellite_id, self.lin.satellite_id
                    )
                )
                self.l2a_log(
                    "L2A and input product have different satellite ids: {} vs {} .".format(
                        satellite_id, self.lin.satellite_id
                    )
                )
                return False
        else:
            self.update_rejection_reason(
                "No product or multiple L2A products are present in the output directory."
            )
            self.l2a_log(
                "No product or multiple L2A products are present in the output directory."
            )
            return False

        self.l2a.name = l2a_product_name
        self.l2a.product_path = os.path.join(self.l2a.output_path, self.l2a.name)
        self.l2a.acquisition_date = acquisition_date
        self.l2a.output_format = SEN2COR_PROCESSOR_OUTPUT_FORMAT
        self.l2a.processed_tiles.append(self.lin.tile_id)
        return True

    def run_script(self):
        gipp_l2a_path = os.path.join(self.context.gips_path, "L2A_GIPP.xml")
        lc_snow_cond_path = os.path.join(
            self.context.gips_path, "ESACCI-LC-L4-Snow-Cond-500m-P13Y7D-2000-2012-v2.0"
        )
        lc_lccs_map_path = os.path.join(
            self.context.gips_path, "ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif"
        )
        lc_wb_map_path = os.path.join(
            self.context.gips_path, "ESACCI-LC-L4-WB-Map-150m-P13Y-2000-v4.0.tif"
        )
        wrk_dir = os.path.join(self.context.working_dir, self.l2a.basename)
        if not create_recursive_dirs(wrk_dir):
            self.update_rejection_reason(
                "Can NOT create wrk dir {}".format(wrk_dir)
            )
            self.l2a_log("Can NOT create wrk dir {}".format(wrk_dir))
            return False

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
        sen2cor_general_log_path = os.path.join(SEN2COR_LOG_DIR, SEN2COR_LOG_FILE_NAME)
        script_command.append("-v")
        script_command.append("{}:{}".format(sen2cor_general_log_path, sen2cor_general_log_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.context.dem_path, self.context.dem_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(gipp_l2a_path, gipp_l2a_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(lc_snow_cond_path, lc_snow_cond_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(lc_lccs_map_path, lc_lccs_map_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(lc_wb_map_path, lc_wb_map_path))
        script_command.append("-v")
        script_command.append("{}:{}".format(wrk_dir, wrk_dir))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.lin.path, self.lin.path))
        script_command.append("-v")
        script_command.append("{}:{}".format(self.l2a.output_path, self.l2a.output_path))
        script_command.append("--name")
        script_command.append("l2a_processors_{}".format(self.lin.product_id))
        script_command.append(DEFAULT_L2APROCESSORS_IMAGE_NAME)
        #actual script command
        script_name = "sen2cor.py"
        script_path = os.path.join("/usr/share/l2a_processors", script_name)
        script_command.append(script_path)
        script_command.append("--dem_path")
        script_command.append(self.context.dem_path)

        if os.path.isfile(gipp_l2a_path):
            script_command.append("--GIP_L2A")
            script_command.append(gipp_l2a_path)
        else:
            self.l2a_log(
                "(launcher warning) Can NOT find L2A_GIPP.xml at {}, proceeding with default GIPP".format(
                    gipp_l2a_path
                )
            )

        if os.path.isdir(lc_snow_cond_path):
            script_command.append("--lc_snow_cond_path")
            script_command.append(lc_snow_cond_path)
        else:
            self.l2a_log(
                "(launcher warning) Can NOT find ESACCI-LC-L4-Snow-Cond-500m-P13Y7D-2000-2012-v2.0 at {}, proceeding with default GIPP".format(
                    lc_snow_cond_path
                )
            )

        if os.path.isfile(lc_lccs_map_path):
            script_command.append("--lc_lccs_map_path")
            script_command.append(lc_lccs_map_path)
        else:
            self.l2a_log(
                "(launcher warning) Can NOT find ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif at {}, proceeding with default GIPP".format(
                    lc_lccs_map_path
                )
            )

        if os.path.isfile(lc_wb_map_path):
            script_command.append("--lc_wb_map_path")
            script_command.append(lc_wb_map_path)
        else:
            self.l2a_log(
                "(launcher warning) Can NOT find ESACCI-LC-L4-WB-Map-150m-P13Y-2000-v4.0.tif at {}, proceeding with default GIPP".format(
                    lc_wb_map_path
                )
            )

        script_command.append("--working_dir")
        script_command.append(wrk_dir)
        script_command.append("--input_dir")
        script_command.append(self.lin.path)
        script_command.append("--output_dir")
        script_command.append(self.l2a.output_path)
        script_command.append("--product_id")
        script_command.append(str(self.lin.product_id))
        if self.context.cogTiffs:
            script_command.append("--cog")
        else:
            script_command.append("--tif")
        if self.context.compressTiffs:
            script_command.append("--compressTiffs")
        script_command.append("--docker-image-sen2cor")
        script_command.append(DEFAULT_SEN2COR_IMAGE_NAME)
        script_command.append("--docker-image-gdal")
        script_command.append(DEFAULT_GDAL_IMAGE_NAME)
        #tmp only for testing purposes
        #script_command.append("--resolution")
        #script_command.append(str(60))
        #tmp

        sen2corlog_path = os.path.join(self.l2a.output_path,"sen2cor.log")
        sen2corlog_file = open(sen2corlog_path, "w")
        cmd_str =""
        for elem in script_command:
            cmd_str = cmd_str + " " + elem
        self.l2a_log("Running command: {}".format(cmd_str))
        print("Running Sen2Cor, console output can be found at {}".format(sen2corlog_path))
        command_return = subprocess.call(script_command, stdout = sen2corlog_file, stderr = sen2corlog_file)

        if (command_return == 0) and os.path.isdir(self.l2a.output_path):
            return True
        else:
            reason = self.get_rejection_reason()
            self.update_rejection_reason(reason)
            self.l2a_log(
                "Sen2Cor returned error code: {} due to : {}.".format(
                    command_return, reason
                )
            )
            return False

    def manage_prods_status(
        self, preprocess_succesful, process_succesful, l2a_ok, postprocess_succesful
    ):
        if (
            (preprocess_succesful == True)
            and (process_succesful == True)
            and (l2a_ok == True)
            and (postprocess_succesful)
        ):
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE
            self.get_quality_indicators()
            if self.l2a.valid_pixels_percentage is not None:
                if self.l2a.valid_pixels_percentage < MIN_VALID_PIXELS_THRESHOLD:
                    self.lin.should_retry = False
                    self.update_rejection_reason(
                        "The valid pixels percentage is {} which is less that the threshold of {}%".format(
                            self.l2a.valid_pixels_percentage, MIN_VALID_PIXELS_THRESHOLD
                        )
                    )
                    self.l2a_log(
                        "The valid pixels percentage is {} which is less that the threshold of {}%".format(
                            self.l2a.valid_pixels_percentage, MIN_VALID_PIXELS_THRESHOLD
                        )
                    )
                    if DEBUG == False:
                        remove_dir(self.l2a.product_path)
                else:
                    self.move_to_destination()
        else:
            self.lin.processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE

    def run(self):
        preprocess_succesful = False
        process_succesful = False
        postprocess_succesful = False
        l2a_ok = False

        if self.check_lin() and self.l2a_setup():
            preprocess_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful  pre-processing = {}".format(
                self.context.worker_id, preprocess_succesful
            )
        )
        self.l2a_log(
            "Successful  pre-processing = {}".format(
                self.context.worker_id, preprocess_succesful
            )
        )

        if preprocess_succesful and self.run_script():
            process_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful Sen2Cor processing = {}".format(
                self.context.worker_id, process_succesful
            )
        )
        self.l2a_log(
            "Successful Sen2Cor processing = {}".format(
                self.context.worker_id, process_succesful
            )
        )

        if process_succesful and self.check_l2a():
            l2a_ok = True
        print(
            "\n(launcher info) <worker {}>: Valid L2a product = {}".format(
                self.context.worker_id, l2a_ok
            )
        )
        self.l2a_log("Valid L2a product = {}".format(self.context.worker_id, l2a_ok))

        if l2a_ok and self.get_l2a_footprint() and self.create_mosaic():
            postprocess_succesful = True
        print(
            "\n(launcher info) <worker {}>: Successful post-processing = {}".format(
                self.context.worker_id, postprocess_succesful
            )
        )
        self.l2a_log(
            "Successful post-processing = {}".format(
                self.context.worker_id, postprocess_succesful
            )
        )

        self.manage_prods_status(
            preprocess_succesful, process_succesful, l2a_ok, postprocess_succesful
        )
        return self.lin, self.l2a


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
                    "{}: Successful db update".format(threading.currentThread().getName()),
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
                "{}: Database connection failed upon updating the database.".format(
                    threading.currentThread().getName()
                ),
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
                        "{}: Exception {} when trying to fetch from db: SERIALIZATION failure".format(
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
                    "{}: Exception {} when trying to fetch from db".format(
                        threading.currentThread().getName(), e.pgcode
                    ),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            except Exception as e:
                products_db.conn.rollback()
                log(
                    LAUNCHER_LOG_DIR,
                    "{}: Exception {} when trying to fetch from db".format(
                        threading.currentThread().getName(), e
                    ),
                    LAUNCHER_LOG_FILE_NAME,
                )
                raise
            else:
                log(
                    LAUNCHER_LOG_DIR,
                    "{}: Successful db fetch".format(threading.currentThread().getName()),
                    LAUNCHER_LOG_FILE_NAME,
                )
                break
            finally:
                products_db.database_disconnect()

        return ret_val

    return wrapper_db_fetch


@db_update
def db_postrun_update(input_prod, l2a_prod):
    processing_status = input_prod.processing_status
    downloader_product_id = input_prod.product_id
    tile_id = input_prod.tile_id
    reason = input_prod.rejection_reason
    should_retry = input_prod.should_retry
    cloud_coverage = l2a_prod.cloud_coverage_assessment
    snow_coverage = l2a_prod.snow_ice_percentage
    processor_id = 1
    site_id = input_prod.site_id
    l1c_id = input_prod.product_id
    l2a_processed_tiles = l2a_prod.processed_tiles
    full_path = l2a_prod.destination_path
    product_name = l2a_prod.name
    footprint = l2a_prod.footprint
    sat_id = l2a_prod.satellite_id
    acquisition_date = l2a_prod.acquisition_date
    orbit_id = l2a_prod.orbit_id

    products_db.cursor.execute("set transaction isolation level serializable;")

    # updating l1_tile_history
    if reason is not None:
        products_db.cursor.execute(
            """SELECT * FROM sp_mark_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                        %(tile_id)s,
                                                                                        %(reason)s,
                                                                                        %(should_retry)s :: boolean,
                                                                                        %(cloud_coverage)s :: integer,
                                                                                        %(snow_coverage)s :: integer);""",
            {
                "downloader_history_id": downloader_product_id,
                "tile_id": tile_id,
                "reason": reason,
                "should_retry": should_retry,
                "cloud_coverage": cloud_coverage,
                "snow_coverage": snow_coverage,
            },
        )
    else:
        products_db.cursor.execute(
            """SELECT * FROM sp_mark_l1_tile_done(%(downloader_history_id)s :: integer,
                                                                                    %(tile_id)s,
                                                                                    %(cloud_coverage)s :: integer,
                                                                                    %(snow_coverage)s :: integer);""",
            {
                "downloader_history_id": downloader_product_id,
                "tile_id": tile_id,
                "cloud_coverage": cloud_coverage,
                "snow_coverage": snow_coverage,
            },
        )

    # update donwloader_history
    products_db.cursor.execute(
        """update downloader_history set status_id = %(status_id)s :: smallint where id=%(l1c_id)s :: integer;""",
        {"status_id": processing_status, "l1c_id": downloader_product_id},
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
                                %(downloader_history_id)s :: integer);""",
            {
                "product_type_id": 1,
                "processor_id": processor_id,
                "satellite_id": sat_id,
                "site_id": site_id,
                "job_id": None,
                "full_path": full_path,
                "created_timestamp": acquisition_date,
                "name": product_name,
                "quicklook_image": "mosaic.jpg",
                "footprint": footprint,
                "orbit_id": orbit_id,
                "tiles": "["
                + ", ".join(['"' + t + '"' for t in l2a_processed_tiles])
                + "]",
                "orbit_type_id": None,
                "downloader_history_id": downloader_product_id,
            },
        )


@db_update
def db_prerun_update(tile, reason):
    processing_status = DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
    downloader_history_id = tile.downloader_history_id
    tile_id = tile.tile_id
    should_retry = True
    cloud_coverage = None
    snow_coverage = None
    site_id = tile.site_id

    products_db.cursor.execute("set transaction isolation level serializable;")
    # updating l1_tile_history
    products_db.cursor.execute(
        """SELECT * FROM sp_mark_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                        %(tile_id)s,
                                                                                        %(reason)s,
                                                                                        %(should_retry)s :: boolean,
                                                                                        %(cloud_coverage)s :: integer,
                                                                                        %(snow_coverage)s :: integer);""",
        {
            "downloader_history_id": downloader_history_id,
            "tile_id": tile_id,
            "reason": reason,
            "should_retry": should_retry,
            "cloud_coverage": cloud_coverage,
            "snow_coverage": snow_coverage,
        },
    )
    # update donwloader_history
    products_db.cursor.execute(
        """update downloader_history set status_id = %(status_id)s :: smallint where id=%(l1c_id)s :: integer;""",
        {"status_id": processing_status, "l1c_id": downloader_history_id},
    )


@db_fetch
def db_get_unprocessed_tile():
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute("select * from sp_start_l1_tile_processing();")
    tile_info = products_db.cursor.fetchall()
    if tile_info == []:
        return None
    else:
        return Tile(tile_info[0])


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
    products_db.cursor.execute("select * from sp_get_parameters('processor.l2a.')")
    rows = products_db.cursor.fetchall()
    for row in rows:
        processing_context.add_parameter(row)

    return processing_context


@db_fetch
def db_clear_pending_tiles():
    products_db.cursor.execute("set transaction isolation level serializable;")
    products_db.cursor.execute("select * from sp_clear_pending_l1_tiles();")
    return products_db.cursor.fetchall()


parser = argparse.ArgumentParser(description="Launcher for MAJA/Sen2Cor script")
parser.add_argument(
    "-c", "--config", default="/etc/sen2agri/sen2agri.conf", help="configuration file"
)
args = parser.parse_args()

#Create and manage log files
manage_log_file(LAUNCHER_LOG_DIR, LAUNCHER_LOG_FILE_NAME)
sen2corlog_path = os.path.join(SEN2COR_LOG_DIR,SEN2COR_LOG_FILE_NAME)
if os.path.isfile(sen2corlog_path) == False:
    with open(sen2corlog_path, "w") as log_file:
        log_file.write("#### Creation of sen2cor log file ####")
else:
    manage_log_file(SEN2COR_LOG_DIR, SEN2COR_LOG_FILE_NAME)
majalog_path = os.path.join(MAJA_LOG_DIR,MAJA_LOG_FILE_NAME)
if os.path.isfile(majalog_path) == False:
    with open(majalog_path, "w") as log_file:
        log_file.write("#### Creation of maja log file ####")
else:
    manage_log_file(MAJA_LOG_DIR, MAJA_LOG_FILE_NAME)

# get the db configuration from cfg file
products_db = Database()
if not products_db.loadConfig(args.config):
    log(
        LAUNCHER_LOG_DIR,
        "Could not load the config from configuration file",
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

default_site_context = default_processing_context.get_site_context("default")
if default_site_context.is_valid() == False:
    log(LAUNCHER_LOG_DIR, "Invalid processing context", LAUNCHER_LOG_FILE_NAME)
    sys.exit(1)

# woking dir operations
# create working dir
if not os.path.isdir(default_site_context.working_dir) and not create_recursive_dirs(
    default_site_context.working_dir
):
    log(
        LAUNCHER_LOG_DIR,
        "Could not create the work base directory {}".format(
            default_site_context.working_dir
        ),
        LAUNCHER_LOG_FILE_NAME,
    )
    sys.exit(1)
# delete all the temporary content from a previous run
remove_dir_content(default_site_context.working_dir)

# clear pending tiless
db_clear_pending_tiles()
l2a_master = L2aMaster(default_site_context.num_workers)
l2a_master.run()

if DEBUG == False:
    remove_dir_content("{}/".format(default_site_context.working_dir))
