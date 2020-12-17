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
import argparse
import re
import fnmatch
import os, errno
from os.path import isfile, isdir, join
import glob
import sys
import time, datetime
import gdal
import Queue
from osgeo import ogr
from multiprocessing import Pool
from sen2agri_common_db import *
from threading import Thread
import threading
import zipfile
import tarfile
import tempfile
import ntpath

from datetime import date
import multiprocessing.dummy
from osgeo import osr
from gdal import gdalconst
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
import subprocess
from time import sleep

ARCHIVES = "archives"
general_log_path = "/tmp/"
general_log_filename = "fmask_extractor.log"

FMASK_EXTRACTOR = "fmask_extractor.py"

DATABASE_FMASK_OUTPUT_PATH = "fmask.output-path"
DATABASE_FMASK_WORKING_DIR = "fmask.working-dir"
DATABASE_FMASK_THRESHOLD = "fmask.threshold"

def get_envelope(footprints):
    geomCol = ogr.Geometry(ogr.wkbGeometryCollection)

    for footprint in footprints:
        #ring = ogr.Geometry(ogr.wkbLinearRing)
        for pt in footprint:
            #ring.AddPoint(pt[0], pt[1])
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint_2D(pt[0], pt[1])
            geomCol.AddGeometry(point)

        #poly = ogr.Geometry(ogr.wkbPolygon)
        #poly

    hull = geomCol.ConvexHull()
    return hull.ExportToWkt()

class FMaskConfig(object):
    def __init__(self, output_path, working_dir):
        self.output_path = output_path
        self.working_dir = working_dir
        
        print("Using configuration:")
        print("\tOutput path: {}".format(output_path))
        print("\tWorking_dir: {}".format(working_dir))

class FMaskL1CInfo(L1CInfo):
    def get_fmask_config(self) :
        if not self.database_connect():
            return None
        try:
            self.cursor.execute("select * from sp_get_parameters('fmask')")
            rows = self.cursor.fetchall()
        except:
            self.database_disconnect()
            return None
        
        output_path = "/mnt/archive/fmask_def/{site}/"
        working_dir = "/mnt/archive/fmask_tmp/"

        for row in rows:
            if len(row) != 3:
                continue
            if row[0] == DATABASE_FMASK_OUTPUT_PATH:
                output_path = row[2]
            elif row[0] == DATABASE_FMASK_WORKING_DIR:
                working_dir = row[2]
                
        self.database_disconnect()
        if len(output_path) == 0 or len(working_dir) == 0:
            return None

        fmaskConfig = FMaskConfig(output_path, working_dir)
        
        return fmaskConfig     

    def get_fmask_unprocessed_l1c_tile(self):
        strings = []
        strings.append("set transaction isolation level serializable;")
        strings.append("select * from sp_start_fmask_l1_tile_processing();")
        return self.general_sql_query(strings)
        
    def clear_pending_fmask_l1c_tiles(self):
        strings = []
        strings.append("select * from sp_clear_pending_fmask_tiles();")
        return self.general_sql_query(strings)

    def mark_fmask_l1_tile_done(self, downloader_product_id):
        strings = []
        if not self.database_connect():
            print("Database connection failed...")
            return False
        strings.append("set transaction isolation level serializable;")
        strings.append(self.cursor.mogrify("""SELECT * FROM sp_mark_fmask_l1_tile_done(%(downloader_history_id)s :: integer);""",
                                 {
                                     "downloader_history_id" : downloader_product_id
                                 }))
        #print("{}:{}: sp_mark_fmask_l1_tile_done: strings = {}".format(threading.currentThread().getName(), id(self), strings))
        ret_array = self.general_sql_query(strings, False, False)
        if ret_array == None:
            # an unhandled exception came from db
            print("sp_mark_fmask_l1_tile_done: Unhandled exception from general_sql_query")
        else:
            if ret_array[0] != None and ret_array[0][0] != None and ret_array[0][0][0] != None and ret_array[0][0][0] == True:
                # commit to database will be perfomed later within set_processed_product function
                print("The query to mark the product with id {} as done will be commited later when insertion in product table is performed".format(downloader_product_id))
                return True
        
        return False

    def mark_fmask_l1_tile_failed(self, downloader_product_id, reason, should_retry):   
        strings = []
        pg_should_retry = False
        product_finished = False
        if not self.database_connect():
            print("Database connection failed...")
            return pg_should_retry, product_finished
        strings.append("set transaction isolation level serializable;")
        strings.append(self.cursor.mogrify("""SELECT * FROM sp_mark_fmask_l1_tile_failed(%(downloader_history_id)s :: integer,
                                                                                  %(reason)s, 
                                                                                  %(should_retry)s :: boolean);""",
                                {
                                    "downloader_history_id" : downloader_product_id,
                                    "reason" : reason,
                                    "should_retry" : should_retry
                                }))
        
        #print("{}:{}: sp_mark_fmask_l1_tile_failed: strings = {}".format(threading.currentThread().getName(), id(self), strings))
        ret_array = self.general_sql_query(strings, False, False)
        if ret_array == None:
            # an unhandled exception came from db
            print("sp_mark_fmask_l1_tile_failed: Unhandled exception from general_sql_query")
        else:
            if ret_array[0] != None and ret_array[0][0] != None and ret_array[0][0][0] != None and ret_array[0][0][0] == True:
                # commit to database will be perfomed later within set_processed_product function
                print("The query to mark the product with id {} as failed will be commited later when insertion in product table is performed".format(downloader_product_id))
                return True
        
        return False

    def set_fmask_product(self, site_id, l1c_id, l2a_processed_prd, full_path, product_name, footprint, sat_id, acquisition_date):
        if not self.database_connect():
            return False, False
        try:
            serialization_failure = False
            ret_val = True
            if l2a_processed_prd != "":
                self.cursor.execute("""select * from sp_insert_product(%(product_type_id)s :: smallint,
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
                                    "product_type_id" : 15,
                                    "processor_id" : 1,
                                    "satellite_id" : sat_id,
                                    "site_id" : site_id,
                                    "job_id" : None,
                                    "full_path" : full_path,
                                    "created_timestamp" : acquisition_date,
                                    "name" : product_name,
                                    "quicklook_image" : "mosaic.jpg",
                                    "footprint" : footprint,
                                    "orbit_id" : None,
                                    "tiles" : None,
                                    "orbit_type_id" : None,
                                    "downloader_history_id" : l1c_id
                                })            
        except psycopg2.Error as e:
            ret_val = False
            if e.pgcode in (SERIALIZATION_FAILURE, DEADLOCK_DETECTED):
                print("{}:{}: Exception when setting fmask product: SERIALIZATION_FAILURE when trying to execute sql queries".format(threading.currentThread().getName(), id(self)))
                serialization_failure = True
            else:
                print("{}:{}: Exception {} when trying to set fmask product".format(threading.currentThread().getName(), id(self), e))                
        return serialization_failure, ret_val
        
    def get_fmask_threshold(self, site_id) :
            if not self.database_connect():
                return None
            try:
                cmd = "select value from sp_get_parameters('{}') where site_id is null or site_id = {} order by site_id limit 1".format(DATABASE_FMASK_THRESHOLD, site_id)
                print ("!!!!!!!!!!!!!")
                print("Executing command : {}".format(cmd))
                print ("!!!!!!!!!!!!!")
                self.cursor.execute("select value from sp_get_parameters('{}') where site_id is null or site_id = {} order by site_id limit 1".format(DATABASE_FMASK_THRESHOLD, site_id))
                rows = self.cursor.fetchall()
            except:
                self.database_disconnect()
                return None
            
            fmask_threshold = ""

            for row in rows:
                fmask_threshold = row[0]
                break
                    
            self.database_disconnect()

            # Check if the threshold is set and if it an integer
            try: 
                if len(fmask_threshold) > 0 :
                    int(fmask_threshold)
            except ValueError:
                fmask_threshold = ""

            return fmask_threshold        

class FmaskL1CContext(object):
    def __init__(self, l1c_list, base_output_path):
        self.l1c_list = l1c_list
        self.base_output_path = base_output_path

def set_l1_tile_status(l1c_db_thread, product_id, reason = None, should_retry = None):
    retries = 0
    max_number_of_retries = 3
    while True:
        if reason is not None and should_retry is not None:
            is_product_finished = l1c_db_thread.mark_fmask_l1_tile_failed(product_id, reason, should_retry)
        else:
            is_product_finished = l1c_db_thread.mark_fmask_l1_tile_done(product_id)
        if is_product_finished == False:
            serialization_failure, commit_result = l1c_db_thread.sql_commit()
            if commit_result == False:
                l1c_db_thread.sql_rollback()
            if serialization_failure and retries < max_number_of_retries:
                time.sleep(2)
                retries += 1
                continue
            l1c_db_thread.database_disconnect()
        return is_product_finished

def validate_L1C_product_dir(l1cDir):
    # First check if the product path actually exists
    try:
        os.stat(l1cDir)
    except OSError, e:
            print ("###################################")
            print ("Cannot check if product root dir path {} exists or it is a valid symlink. Error was: {}".format(l1cDir, e.errno))
            print ("###################################")
            return False
    
    print('--\nChecking ROOT for valid symlink = ' + l1cDir)
    for root, subdirs, files in os.walk(l1cDir):
#        print('--\nChecking ROOT for valid symlink = ' + root)

        for subdir in subdirs:
            subdir_path = os.path.join(root, subdir)
#            print('\t- subdirectory ' + subdir_path)
            try:
                os.stat(subdir_path)
            except OSError, e:
                    print ("###################################")
                    print ("Cannot check if dir path {} exists or it is a valid symlink. Error was: {}".format(subdir_path, e.errno))
                    print ("###################################")
                    return False

        for filename in files:
            file_path = os.path.join(root, filename)
            #print('\t- file %s (full path: %s)' % (filename, file_path))
            try:
                os.stat(file_path)
            except OSError, e:
                print ("###################################")
                print ("Cannot check if file path {} exists or is a valid symlink. Error was: {}".format(subdir_path, e.errno))
                print ("###################################")
                return False

    return True
    
def path_filename(path):
    head, filename = ntpath.split(path)
    return filename or ntpath.basename(head)
    
def check_if_flat_archive(output_dir, archive_filename):
    dir_content = os.listdir(output_dir)
    print("check_if_flat_archive dir_content = {} / len = {}".format(dir_content, len(dir_content)))
    if len(dir_content) == 1 and os.path.isdir(os.path.join(output_dir, dir_content[0])):
        return os.path.join(output_dir, dir_content[0])
    if len(dir_content) > 1:
        #use the archive filename, strip it from extension
        product_name, file_ext = os.path.splitext(path_filename(archive_filename))
        #handle .tar.gz case
        if product_name.endswith(".tar"):
            product_name, file_ext = os.path.splitext(product_name)        
        product_path = os.path.join(output_dir, product_name)
        if create_recursive_dirs(product_path):            
            #move the list to this directory:
            for name in dir_content:
                shutil.move(os.path.join(output_dir, name), os.path.join(product_path, name))
            return product_path
    print("Checking if the archive is flat: returns None")
    return None

def unzip(output_dir, input_file):
    global general_log_path
    global general_log_filename
    log(general_log_path, "Unzip archive = {} to {}".format(input_file, output_dir), general_log_filename)
    try:
        with zipfile.ZipFile(input_file) as zip_archive:
            zip_archive.extractall(output_dir)
            return check_if_flat_archive(output_dir, path_filename(input_file))
    except Exception, e:
        log(general_log_path, "Exception when trying to unzip file {}:  {} ".format(input_file, e), general_log_filename)
    return None

def untar(output_dir, input_file):
    global general_log_path
    global general_log_filename
    log(general_log_path, "Untar archive = {} to {}".format(input_file, output_dir), general_log_filename)
    try:
        tar_archive = tarfile.open(input_file)
        tar_archive.extractall(output_dir)
        tar_archive.close()
        return check_if_flat_archive(output_dir, path_filename(input_file))
    except Exception, e:
        log(general_log_path, "Exception when trying to untar file {}:  {} ".format(input_file, e), general_log_filename)
    return None
    
def extract_from_archive_if_needed(archive_file):
    #create the temporary path where the archive will be extracted
    extracted_archive_dir = tempfile.mkdtemp(dir = os.path.join(fmask_config.working_dir, ARCHIVES))
    print("ARCHIVES DIRECTORY = {}".format(extracted_archive_dir))
    extracted_file_path = None
    # check if the file is indeed an archive
    # exception raised only if the archive_file does not exist
    try:
        if zipfile.is_zipfile(archive_file):
            extracted_file_path = unzip(extracted_archive_dir, archive_file)
    except Exception, e:
        print("is_zipfile: The object (directory or file) {} is not an archive: {} !".format(archive_file, e))
        extracted_file_path = None            
    try:
        if tarfile.is_tarfile(archive_file):
            extracted_file_path = untar(extracted_archive_dir, archive_file)
    except Exception, e:
        print("is_tarfile: The object (directory or file) {} is not an archive: {} !".format(archive_file, e))
        extracted_file_path = None            
    if extracted_file_path is not None:
        print("Archive extracted to: {}".format(extracted_file_path))
        return True, extracted_file_path
    # this isn't and archive, so no need for the temporary directory
    print("This wasn't an archive, so continue as is. Deleting {}".format(extracted_archive_dir))
    remove_dir(extracted_archive_dir)
    return False, archive_file    

def get_fmask_raster_path(output_path, tile_log_filename) :
    tile_img = ""
    for root, dirs, filenames in os.walk(output_path):
        for filename in fnmatch.filter(filenames, "*_Fmask4.tif"):
            tile_img = os.path.join(root, filename)
            log(output_path, "{}: FMask footprint tif file: {}".format(threading.currentThread().getName(), tile_img), tile_log_filename)
            break
        if len(tile_img) > 0:
            break
    return tile_img

def get_product_footprint(fmask_raster_path):
    wgs84_extent_list = []
    wgs84_extent_list.append(get_footprint(fmask_raster_path)[0])
    wkt = get_envelope(wgs84_extent_list)
    return wkt
    
def launch_fmask(l1c_db_thread):
    global general_log_path
    global general_log_filename
    print("Starting thread {}".format(threading.currentThread().getName()))
    while True:
        # get the tile to process. The object from the queue is L1CContext
        l1c_context = l1c_queue.get()
        thread_finished_queue.put("started")
        print("{} will consume: {} | {}".format(threading.currentThread().getName(), l1c_context.l1c_list, l1c_context.base_output_path))
        if l1c_context.l1c_list == None:
            # no more tiles to process in db, so exit from thread
            log(general_log_path, "{}: No tile to process. Gracefully closing...".format(threading.currentThread().getName()), general_log_filename)
            thread_finished_queue.get()
            l1c_queue.task_done()
            log(general_log_path, "{}: Exit thread".format(threading.currentThread().getName()), general_log_filename)
            return
        if len(l1c_context.l1c_list) != 1:
            # input error, length of the list has to be 1, this is in fact the result from database, 
            # query: select * from sp_start_l1_tile_processing()
            log(general_log_path, "{}: Input error from database".format(threading.currentThread().getName()), general_log_filename)
            thread_finished_queue.get()
            l1c_queue.task_done()
            continue
        try :
            l1c = l1c_context.l1c_list[0]
            # l1c is the only record from sp_start_fmask_l1_tile_processing() function. the cells are :
            # 0       | 1            |  2                     | 3   
            # site_id | satellite_id |  downloader_history_id | path 
            site_id = l1c[0][0]
            satellite_id = l1c[0][1]
            product_id = l1c[0][2]
            product_path = l1c[0][3]
            log(general_log_path, "{}: Starting extract_from_archive_if_needed for product {}".format(threading.currentThread().getName(), product_path), general_log_filename)
            l1c_was_archived, full_path = extract_from_archive_if_needed(product_path)
            log(general_log_path, "{}: Ended extract_from_archive_if_needed for product {}".format(threading.currentThread().getName(), product_path), general_log_filename)
            print("{}: site_id = {}".format(threading.currentThread().getName(), site_id))
            print("{}: satellite_id = {}".format(threading.currentThread().getName(), satellite_id))
            print("{}: dh_id = {}".format(threading.currentThread().getName(), product_id))
            print("{}: path = {}".format(threading.currentThread().getName(), full_path))

            # processing the tile
            #get site short name
            site_short_name = l1c_db_thread.get_short_name("site", site_id)

            #create the output_path. it will hold all the tiles found inside the l1c
            #for sentinel, this output path will be something like /path/to/poduct/site_name/processor_name/....MSIL2A.../
            #for landsat, this output path will be something like /path/to/poduct/site_name/processor_name/LC8...._L2A/
            site_output_path = l1c_context.base_output_path.replace("{site}", site_short_name)
            if not site_output_path.endswith("/"):
                site_output_path += "/"

            if not l1c_db_thread.is_site_enabled(site_id):
                log(general_log_path, "{}: Aborting processing for site {} because it is marked as being deactivated".format(threading.currentThread().getName(), site_id), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()
                continue

            if not l1c_db_thread.is_sensor_enabled(site_id, satellite_id):
                log(general_log_path, "{}: Aborting processing for site {} because sensor downloading for {} is marked as being disabled".format(threading.currentThread().getName(), site_id, satellite_id), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()
                continue

            if not validate_L1C_product_dir(full_path):
                log(general_log_path, "{}: The product {} is not valid or temporary unavailable...".format(threading.currentThread().getName(), full_path), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()
                continue

            l2a_basename = os.path.basename(full_path[:len(full_path) - 1]) if full_path.endswith("/") else os.path.basename(full_path)
            satellite_id = int(satellite_id)
            if satellite_id != SENTINEL2_SATELLITE_ID and satellite_id != LANDSAT8_SATELLITE_ID:
                log(general_log_path, "{}: Unkown satellite id :{}".format(threading.currentThread().getName(), satellite_id), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()            
                continue
            if l2a_basename.startswith("S2"):
                l2a_basename = l2a_basename.replace("L1C", "FMASK")
            elif l2a_basename.startswith("LC8"):
                l2a_basename += "_FMASK"
            elif l2a_basename.startswith("LC08"):
                if l2a_basename.find("_L1TP_") > 0 :
                    l2a_basename = l2a_basename.replace("_L1TP_", "_FMASK_")
                elif l2a_basename.find("_L1GS_") > 0 :
                    l2a_basename = l2a_basename.replace("_L1GS_", "_FMASK_")
                elif l2a_basename.find("_L1GT_") > 0 :
                    l2a_basename = l2a_basename.replace("_L1GT_", "_FMASK_")
                else:
                    log(general_log_path, "{}: The L1C product name is wrong - L2 cannot be filled: {}".format(threading.currentThread().getName(), l2a_basename), general_log_filename)
                    if l1c_was_archived:
                        remove_dir(full_path)
                    thread_finished_queue.get()
                    l1c_queue.task_done()
                    continue
            else:
                log(general_log_path, "{}: The L1C product name is wrong: {}".format(threading.currentThread().getName(), l2a_basename), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()            
                continue

            output_path = site_output_path + l2a_basename + "/"
            tile_log_filename = "fmask.log"
            log(output_path, "{}: Starting the process for product id {}".format(threading.currentThread().getName(), product_id), tile_log_filename)
            # the output_path should be created by the fmask_extractor.py script itself, but for log reason it will be created here
            if not create_recursive_dirs(output_path):
                log(general_log_path, "{}: Could not create the output directory".format(threading.currentThread().getName()), general_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
                thread_finished_queue.get()
                l1c_queue.task_done()
                continue            

            threshold = l1c_db.get_fmask_threshold(site_id)
            print ("Using threshold = {}".format(threshold))    
            
            l2a_processed_tiles = []
            wkt = []
            sat_id = 0
            acquisition_date = ""
            base_abs_path = os.path.dirname(os.path.abspath(__file__)) + "/"
            fmask_command = [base_abs_path + FMASK_EXTRACTOR, "--working-dir", fmask_config.working_dir, "--delete-temp", "False"]
            if threshold != '' : 
                fmask_command += ["-t", threshold]
            fmask_command += [full_path, output_path]
            
            create_footprint = False
            should_retry = None
            reason = None
            log(output_path, "{}: Starting fmask".format(threading.currentThread().getName()), tile_log_filename)
            if run_command(fmask_command, output_path, tile_log_filename) == 0 and os.path.exists(output_path) and os.path.isdir(output_path):
                # mark the tile as done
                # if there are still tiles to be processed within this product, the commit call for database will be performed 
                # inside mark_l1_tile_done or mark_l1_tile_failed. If this was the last tile within this product (the mark_l1_tile_done or 
                # mark_l1_tile_failed functions return the true) the footprint of the product has to be processed 
                # before calling set_l2a_product function. The set_l2a_product function will also close the transaction by calling commit 
                # for database. In this case the mark_l1_tile_done or mark_l1_tile_failed functions will not call the commit for database       
                db_result_tile_processed = set_l1_tile_status(l1c_db_thread, product_id)
                log(output_path, "{}: Product with id {} marked as DONE, with db_result_tile_processed = {}".format(threading.currentThread().getName(), product_id, db_result_tile_processed), tile_log_filename)
            else:
                log(output_path, "{}: {} script failed!".format(threading.currentThread().getName()), FMASK_EXTRACTOR, tile_log_filename)
                should_retry = False    # TODO
                reason = "Unknown" # fmask_log_extract.error_message TODO
                db_result_tile_processed = set_l1_tile_status(l1c_db_thread, product_id, reason, should_retry)
                log(output_path, "{}: Product with id {} marked as FAILED (should the process be retried: {}). The L1C product {} finished: {}. Reason for failure: {}".format(threading.currentThread().getName(), product_id, should_retry, l2a_basename, db_result_tile_processed, reason), tile_log_filename)

            if db_result_tile_processed:
                # to end the transaction started in mark_l1_tile_done or mark_l1_tile_failed functions,
                # the sql commit for database has to be called within set_l2a_product function below
                
                            
                # create the footprint for the whole product
                #wkt = get_product_footprint(tiles_dir_list, satellite_id)
                fmask_raster_path = get_fmask_raster_path(output_path, tile_log_filename)
                wkt = get_product_footprint(fmask_raster_path)

                l2a_processed_prd = ""
                if len(wkt) == 0:
                    log(output_path, "{}: Could not create the footprint".format(threading.currentThread().getName()), tile_log_filename)
                else:
                    sat_id, acquisition_date = get_product_info(os.path.basename(output_path[:len(output_path) - 1]))
                    if sat_id > 0 and acquisition_date != None:
                        l2a_processed_prd = fmask_raster_path
                    else:
                        log(output_path,"{}: Could not get the acquisition date from the product name {}".format(threading.currentThread().getName(), output_path), tile_log_filename)
                if l2a_processed_prd != "":
                    log(output_path, "{}: Processing for tile {} finished. Insert info in product table for {}.".format(threading.currentThread().getName(), fmask_raster_path, output_path), tile_log_filename)
                else:
                    log(output_path, "{}: Processing for product {} finished. No fmask tiles found after FMask finished for product {}".format(threading.currentThread().getName(), fmask_raster_path, output_path), tile_log_filename)
                retries = 0
                max_number_of_retries = 3
                # the postgres SERIALIZATION_FAILURE exception has to be handled
                # this has to be done somehow here at the higher level instead of the database level l1c_db_thread
                while True:
                    serialization_failure, commit_result = l1c_db_thread.set_fmask_product(site_id, product_id, l2a_processed_prd, fmask_raster_path, os.path.basename(output_path[:len(output_path) - 1]), wkt, sat_id, acquisition_date)
                    if commit_result == False:                    
                        l1c_db_thread.sql_rollback()
                        log(output_path, "{}: Rolling back for {}".format(threading.currentThread().getName(), output_path), tile_log_filename)
                        if serialization_failure == True and retries < max_number_of_retries and set_l1_tile_status(l1c_db_thread, product_id, reason, should_retry):
                            log(output_path, "{}: Exception when inserting to product table: SERIALIZATION_FAILURE, rolling back and will retry".format(threading.currentThread().getName()), tile_log_filename)
                            time.sleep(2)
                            retries += 1
                            continue                
                        log(output_path, "{}: Couldn't insert the product {}".format(threading.currentThread().getName(), output_path), tile_log_filename)
                        l1c_db_thread.database_disconnect()
                        break
                    retries = 0
                    serialization_failure, commit_result = l1c_db_thread.sql_commit()
                    if commit_result == False:                    
                        l1c_db_thread.sql_rollback()
                        log(output_path, "{}: Commit returned false, rolling back for {}".format(threading.currentThread().getName(), output_path), tile_log_filename)
                    else:
                        log(output_path, "{}: Product {} inserted".format(threading.currentThread().getName(), output_path), tile_log_filename)
                    if serialization_failure == True and retries < max_number_of_retries and set_l1_tile_status(l1c_db_thread, product_id, reason, should_retry):
                        log(output_path, "{}: Exception when inserting to product table: SERIALIZATION_FAILURE, rolling back and will retry".format(threading.currentThread().getName()), tile_log_filename)
                        time.sleep(2)
                        retries += 1
                        continue
                    l1c_db_thread.database_disconnect()
                    break
                # create mosaic - TODO 
                #if l2a_processed_prd != "":
                #    if run_command([os.path.dirname(os.path.abspath(__file__)) + "/mosaic_l2a.py", "-i", output_path, "-w", fmask_config.working_dir], output_path, tile_log_filename) != 0:
                #        log(output_path, "{}: Mosaic didn't work".format(threading.currentThread().getName()), tile_log_filename)
                if l1c_was_archived:
                    remove_dir(full_path)
            # end of tile processing
            # remove the tile from queue
            thread_finished_queue.get()
            l1c_queue.task_done()
        except (KeyboardInterrupt, SystemExit):
            thread_finished_queue.get()
            l1c_queue.task_done()
            
parser = argparse.ArgumentParser(description="Launcher for FMASK script")
parser.add_argument('-c', '--config', default="/etc/sen2agri/sen2agri.conf", help="configuration file")
parser.add_argument('-p', '--processes-number', default=4, help="Number of products to be processed at the same time.")

args = parser.parse_args()
manage_log_file(general_log_path, general_log_filename)

# get the db configuration from cfg file
config = Config()
if not config.loadConfig(args.config):
    log(general_log_path, "Could not load the config from configuration file", general_log_filename)
    sys.exit(-1)

#load configuration from db for fmask processor
l1c_db = FMaskL1CInfo(config.host, config.database, config.user, config.password)
fmask_config = l1c_db.get_fmask_config()
if fmask_config is None:
    log(general_log_path, "Could not load the config from database", general_log_filename)
    sys.exit(-1)

if not os.path.isdir(fmask_config.working_dir) and not create_recursive_dirs(fmask_config.working_dir):
    log(general_log_path, "Could not create the work base directory {}".format(fmask_config.working_dir), general_log_filename)
    sys.exit(-1)

#delete all the temporary content from a previous run
remove_dir_content(fmask_config.working_dir)
#create directory for the eventual archives like l1c products
create_recursive_dirs(os.path.join(fmask_config.working_dir, ARCHIVES))
l1c_queue = Queue.Queue(maxsize=int(args.processes_number))
thread_finished_queue = Queue.Queue(maxsize=int(args.processes_number))

for i in range(int(args.processes_number)):
    t = Thread(name="dmworker_{}".format(i), target=launch_fmask, args=(FMaskL1CInfo(config.host, config.database, config.user, config.password), ))
    t.daemon = False
    t.start()

l1c_db.clear_pending_fmask_l1c_tiles()
l1c_tile_to_process = l1c_db.get_fmask_unprocessed_l1c_tile()
if(l1c_tile_to_process == None):
    sys.exit(1)
while True:
    if len(l1c_tile_to_process) > 0:
        prev_queue_size = thread_finished_queue.qsize()
        #print("{}: Feeding the queue ...")
        l1c_queue.put(FmaskL1CContext(l1c_tile_to_process, fmask_config.output_path))
        print("######")
        print ("thread_finished_queue size = {}".format(thread_finished_queue.qsize()))
        print ("l1c_queue size = {}".format(l1c_queue.qsize()))
        print("FMask main thread: feeding the queue for workers with: {}".format(l1c_tile_to_process))
        print("######")
        l1c_tile_to_process = []
        time.sleep(1)
        #print("{}: Queue feeded")
    else:
        #print("Main thread is sleeping....")
        time.sleep(5)
    if thread_finished_queue.qsize() < int(args.processes_number):
        l1c_tile_to_process = l1c_db.get_fmask_unprocessed_l1c_tile()    
    if (l1c_tile_to_process == None) or (len(l1c_tile_to_process) == 0 and thread_finished_queue.qsize() == 0):
        for i in range(int(args.processes_number)):
            l1c_queue.put(FmaskL1CContext(None, fmask_config.output_path))
        print("Waiting for queue to join...")
        l1c_queue.join()
        print("All the workers finished their job. Exiting...")
        break

remove_dir_content("{}/".format(fmask_config.working_dir))
