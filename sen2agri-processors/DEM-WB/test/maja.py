#!/usr/bin/env python3
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
from __future__ import absolute_import
import os
import argparse
import glob
import re
import time
import datetime
import pipes
import shutil
import signal
from multiprocessing import Pool
from l2a_commons import run_command, create_recursive_dirs, remove_dir, translate, get_guid, stop_containers, remove_dir_content
from l2a_commons import UNKNOWN_SATELLITE_ID, SENTINEL2_SATELLITE_ID, LANDSAT8_SATELLITE_ID
from l2a_commons import MACCS_PROCESSOR_OUTPUT_FORMAT, THEIA_MUSCATE_OUTPUT_FORMAT, UNKNOWN_PROCESSOR_OUTPUT_FORMAT
from l2a_commons import LogHandler, NO_ID
import tempfile
from bs4 import BeautifulSoup as Soup

MAJA_LOG_FILE_NAME = "maja.log"

def get_product_info(product_name):
    acquisition_date = None
    sat_id = UNKNOWN_SATELLITE_ID
    if product_name.startswith("S2"):
        m = re.match(r"\w+_V(\d{8}T\d{6})_\w+.SAFE", product_name)
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
        if os.path.isdir(filename) and re.search(r".*DATA$", filename) is not None:
            data_dir = True
        if os.path.isdir(filename) and re.search(r".*MASKS$", filename) is not None:
            masks_dir = True

        if args.removeFreFiles:
            fre_files_count = 1

        if args.removeSreFiles:
            sre_files_count = 1
            
    res = (atb_files_count > 0 and
           fre_files_count > 0 and
           sre_files_count > 0 and
           qkl_file and mtd_file and
           data_dir and masks_dir)

    return res

def get_l1c_processor_output_format(working_directory, tile_id):
    if not os.path.isdir(working_directory):
        return UNKNOWN_PROCESSOR_OUTPUT_FORMAT, None
    #check for MACCS output
    maccs_dbl_dir = glob.glob("{}/*_L2VALD_{}*.DBL.DIR".format(working_directory, tile_id))
    maccs_hdr_file = glob.glob("{}/*_L2VALD_{}*.HDR".format(working_directory, tile_id))
    if len(maccs_dbl_dir) >= 1 and len(maccs_hdr_file) >= 1:
        return MACCS_PROCESSOR_OUTPUT_FORMAT, None
    #check for THEIA/MUSCATE format
    working_dir_content = glob.glob("{}/*".format(working_directory))
    for maja_out in working_dir_content:
        if os.path.isdir(maja_out) and re.search(r".*_L2A_.*", maja_out, re.IGNORECASE) and check_maja_valid_output(maja_out, tile_id):
            return THEIA_MUSCATE_OUTPUT_FORMAT, maja_out
		
    return UNKNOWN_PROCESSOR_OUTPUT_FORMAT, None

def create_sym_links(filenames, target_directory, log):

    for file_to_sym_link in filenames:
        #target name
        if file_to_sym_link.endswith("/"):
            basename = os.path.basename(file_to_sym_link[:len(file_to_sym_link) - 1])
        else:
            basename = os.path.basename(file_to_sym_link)
        target = os.path.join(target_directory, basename)
        #does it already exist?
        if os.path.isfile(target) or os.path.isdir(target):
            #skip it
            continue
        #create it
        if run_command(["ln", "-s", file_to_sym_link, target_directory], log) != 0:
            return False
    return True


def remove_sym_links(filenames, target_directory):
    for sym_link in filenames:
        if sym_link.endswith("/"):
            basename = os.path.basename(sym_link[:len(sym_link) - 1])
        else:
            basename = os.path.basename(sym_link)
        if run_command(["rm", os.path.join(target_directory, basename)], l2a_log) != 0:
            continue
    return True


def get_prev_l2a_tile_path(tile_id, prev_l2a_product_path):
    tile_files = []
    print("START get_prev_l2a_tile_path")
    print("Tile_id = {} | prev_l2a_product_path = {}".format(tile_id, prev_l2a_product_path))
    if os.path.exists(prev_l2a_product_path) and os.path.isdir(prev_l2a_product_path):
        all_files = glob.glob("{}/*".format(prev_l2a_product_path))
        print("all_files = {}".format(all_files))
        for filename in all_files:
            if not filename.endswith(".log"):
                print("added: {}".format(filename))
                tile_files.append(filename)
    else:
        print("The dir {} does not exist or is not a dir".format(prev_l2a_product_path))
    print("STOP get_prev_l2a_tile_path")
    return tile_files

def get_maja_jpi_log_extract(maja_working_dir, demmaccs_context):
    l2a_log.info("Checking for MAJA JPI file in directory {} ...".format(maja_working_dir))
    
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(maja_working_dir):
        for file in f:
            if '_JPI_ALL.xml' in file:
                files.append(os.path.join(r, file))
    if (len(files) == 0) :
        # No JPI file found ... return true from here
        l2a_log.info("No _JPI_ALL.xml found in the maja working dir {}. Not considering an error ...".format(maja_working_dir))
        return True
    
    # normally we should have only one file like this
    maja_jpi_file = files[0]
    
    l2a_log.info("Checking MAJA JPI file {} ...".format(maja_jpi_file))

    try:
        xml_handler = open(maja_jpi_file).read()
        soup = Soup(xml_handler,"lxml")
        for message in soup.find_all('processing_flags_and_modes'):
            key = message.find('key').get_text()
            if key == 'Validity_Flag' :
                value = message.find('value').get_text()
                if value == 'L2NOTV' :
                    l2a_log.error("L2NOTV found in the Validity_Flag Processing_Flags_And_Modes element from MAJA JPI file {}. The product will be invalidated ... ".format(maja_jpi_file), print_msg = True)
                    # copy the file also in the MAJA root directory (near EEF)
                    new_file = os.path.join(demmaccs_context.output, os.path.basename(maja_jpi_file))
                    shutil.copy(maja_jpi_file, new_file)
                    return False
    except Exception as e:
        print("Exception received when trying to read the MAJA JPI from file {}: {}".format(maja_jpi_file, e))
        l2a_log.error("Exception received when trying to read the MAJA JPI from file {}: {}".format(maja_jpi_file, e), print_msg = True, trace = True)
        pass
    return True

def copy_common_gipp_file(working_dir, gipp_base_dir, gipp_sat_dir, gipp_sat_prefix, full_gipp_sat_prefix, gipp_tile_type, gipp_tile_prefix, tile_id, common_tile_id):
    #take the common one
    tmp_tile_gipp = glob.glob("{}/{}/{}*{}_S_{}{}*.EEF".format(gipp_base_dir, gipp_sat_dir, gipp_sat_prefix, gipp_tile_type, gipp_tile_prefix, common_tile_id))
    print ("copy_common_gipp_file: found the following files {}".format(tmp_tile_gipp))
    #if found, copy it (not sym link it)
    if len(tmp_tile_gipp) > 0:
        common_gipp_file = tmp_tile_gipp[0];
        basename_tile_gipp_file = os.path.basename(common_gipp_file[:len(common_gipp_file) - 1]) if common_gipp_file.endswith("/") else os.path.basename(common_gipp_file)
        basename_tile_gipp_file = basename_tile_gipp_file.replace(common_tile_id, tile_id)
        
        for cmn_gipp_file_tmp in tmp_tile_gipp :
            tmpFile1 = os.path.basename(cmn_gipp_file_tmp[:len(cmn_gipp_file_tmp) - 1]) if cmn_gipp_file_tmp.endswith("/") else os.path.basename(cmn_gipp_file_tmp)
            tmpFile1 = tmpFile1.replace(common_tile_id, tile_id)
            if tmpFile1.startswith(full_gipp_sat_prefix) :
                print ("Selecting the file {}".format(tmpFile1))
                common_gipp_file = cmn_gipp_file_tmp
                basename_tile_gipp_file = tmpFile1
                break

        tile_gipp_file = "{}/{}".format(working_dir, basename_tile_gipp_file)  
        try: 
            with open(common_gipp_file, 'r') as handler_common_gipp_file, open(tile_gipp_file, 'w') as handler_tile_gipp_file:
                for line in handler_common_gipp_file:
                    if "<File_Name>" in line or "<Applicability_NickName>" in line or "<Applicable_SiteDefinition_Id" in line:
                        line = line.replace(common_tile_id, tile_id)
                    handler_tile_gipp_file.write(line)
        except EnvironmentError:
            return False, "Could not transform / copy the common GIPP tile file {} to {} ".format(common_gipp_file, tile_gipp_file)
    else:
        return False, "Could not find common gip tile id {}".format(common_tile_id)
    return True, "Copied {} to {}".format(common_gipp_file, tile_gipp_file)


class DEMMACCSContext(object):
    def __init__(self, base_working_dir, dem_hdr_file, gipp_base_dir, prev_l2a_tiles, prev_l2a_products_paths, maccs_address, l1c_input, l2a_output):
        self.base_working_dir = base_working_dir
        self.dem_hdr_file = dem_hdr_file
        self.dem_output_dir = dem_output_dir
        self.gipp_base_dir = gipp_base_dir
        self.prev_l2a_tiles = prev_l2a_tiles
        self.prev_l2a_products_paths = prev_l2a_products_paths
        self.maccs_address = maccs_address
        self.input = l1c_input
        self.output = l2a_output

def maccs_launcher(demmaccs_context, dem_output_dir):
    global running_containers

    if not os.path.isfile(demmaccs_context.dem_hdr_file):
        l2a_log.error("General failure: There is no such DEM file {}".format(demmaccs_context.dem_hdr_file), print_msg = True)
        return ""
    product_name = os.path.basename(demmaccs_context.input[:len(demmaccs_context.input) - 1]) if demmaccs_context.input.endswith("/") else os.path.basename(demmaccs_context.input)
    sat_id, acquistion_date = get_product_info(product_name)
    gipp_sat_prefix = ""
    basename = os.path.basename(demmaccs_context.dem_hdr_file)
    dem_dir_list = glob.glob("{0}/{1}.DBL.DIR".format(dem_output_dir, basename[0:len(basename) - 4]))

    if len(dem_dir_list) != 1 or not os.path.isdir(dem_dir_list[0]):
        l2a_log.error("General failure: No {}.DBL.DIR found for DEM ".format(demmaccs_context.dem_hdr_file[0:len(demmaccs_context.dem_hdr_file) - 4]), print_msg = True)
        return ""
    dem_dir = dem_dir_list[0]
    tile_id = ""
    gipp_sat_dir = ""
    gipp_tile_prefix = ""

    eucmn00_file_path = os.path.join(demmaccs_context.gipp_base_dir, "LANDSAT8/*EUCMN00*")
    eucmn00_file = glob.glob(eucmn00_file_path)
    if len(eucmn00_file) > 0:
        common_tile_id = "CMN00"
        if sat_id == LANDSAT8_SATELLITE_ID:
            gipp_tile_prefix = "EU"
    else:
        common_tile_id = "ALLSITES"

    if sat_id == SENTINEL2_SATELLITE_ID:
        gipp_sat_prefix = "S2"
        full_gipp_sat_prefix = gipp_sat_prefix
        m = re.match(r"(S2[A-D])_\w+_V\d{8}T\d{6}_\w+.SAFE", product_name)
        # check if the new convention naming aplies
        if m == None:
            m = re.match(r"(S2[A-D])_\w+_\d{8}T\d{6}_\w+.SAFE", product_name)
        if m != None:
            full_gipp_sat_prefix = m.group(1)
        
        print ("full_gipp_sat_prefix is {}".format(full_gipp_sat_prefix))
        
        gipp_sat_dir = "SENTINEL2"
        tile = re.match(r"S2\w+_REFDE2_(\w{5})\w+", basename[0:len(basename) - 4])
        if tile is not None:
            tile_id = tile.group(1)
    elif sat_id == LANDSAT8_SATELLITE_ID:
        gipp_sat_prefix = "L8"
        full_gipp_sat_prefix = "L8"
        gipp_sat_dir = "LANDSAT8"
        tile = re.match(r"L8\w+_REFDE2_(\w{6})\w+", basename[0:len(basename) - 4])
        if tile is not None:
            tile_id = tile.group(1)
    else:
        l2a_log.error("General failure: Unknown satellite id {} found for {}".format(sat_id, demmaccs_context.input), print_msg = True)
        return ""

    if len(tile_id) == 0:
        l2a_log.error("General failure: Could not get the tile id from DEM file {}".format(demmaccs_context.dem_hdr_file), print_msg = True)
        return ""

    working_dir = os.path.join(demmaccs_context.base_working_dir, tile_id)
    maccs_working_dir = "{}/maccs_{}".format(demmaccs_context.base_working_dir[:len(demmaccs_context.base_working_dir) - 1] if demmaccs_context.base_working_dir.endswith("/") else demmaccs_context.base_working_dir, tile_id)
    if not create_recursive_dirs(working_dir):
        l2a_log.error("Tile failure: Could not create the working directory {}".format(working_dir), print_msg = True)
        return ""

    if not create_recursive_dirs(maccs_working_dir):
        l2a_log.error("Tile failure: Could not create the MACCS/MAJA working directory {}".format(maccs_working_dir), print_msg = True)
        return ""

    if not create_sym_links([demmaccs_context.input], working_dir, l2a_log):
        l2a_log.error("Tile failure: Could not create sym links for {}".format(demmaccs_context.input), print_msg = True)
        return ""
    common_gipps = glob.glob("{}/{}/{}*_L_*.*".format(demmaccs_context.gipp_base_dir, gipp_sat_dir, full_gipp_sat_prefix))
    if len(common_gipps) == 0:
        common_gipps = glob.glob("{}/{}/{}*_L_*.*".format(demmaccs_context.gipp_base_dir, gipp_sat_dir, gipp_sat_prefix))
    
    print ("common_gipps is {}".format(common_gipps))
    if not create_sym_links(common_gipps, working_dir, l2a_log):
        l2a_log.error("Tile failure: Symbolic links for GIPP files could not be created in the output directory", print_msg = True)
        return ""
    gipp_tile_types = ["L2SITE", "CKEXTL", "CKQLTL"]

    tmp_tile_gipp = []
    for gipp_tile_type in gipp_tile_types:
        #search for the specific gipp tile file. if it will not be found, the common one (if exists) will be used
        tmp_tile_gipp .extend(glob.glob("{}/{}/{}*{}_S_{}{}*.EEF".format(demmaccs_context.gipp_base_dir, gipp_sat_dir, full_gipp_sat_prefix, gipp_tile_type, gipp_tile_prefix, tile_id)))
        if len(tmp_tile_gipp) == 0:
            tmp_tile_gipp.extend(glob.glob("{}/{}/{}*{}_S_{}{}*.EEF".format(demmaccs_context.gipp_base_dir, gipp_sat_dir, gipp_sat_prefix, gipp_tile_type, gipp_tile_prefix, tile_id)))
        
        print ("tmp_tile_gipp is {}".format(tmp_tile_gipp))
        if len(tmp_tile_gipp) > 0:
            if not create_sym_links(tmp_tile_gipp, working_dir, l2a_log):
                l2a_log.error("Tile failure: Symbolic links for tile id {} GIPP files could not be created in the output directory".format(tile_id), print_msg = True)
                return ""
        else:
            #search for the gipp common tile file
            l2a_log.error("Symbolic link {} for tile id {} GIPP file could not be found. Searching for the common one ".format(gipp_tile_type, tile_id), print_msg = True)
            ret, log_gipp = copy_common_gipp_file(working_dir, demmaccs_context.gipp_base_dir, gipp_sat_dir, gipp_sat_prefix, full_gipp_sat_prefix, gipp_tile_type, gipp_tile_prefix, tile_id, common_tile_id)
            if len(log_gipp) > 0:
                l2a_log.info(log_gipp, print_msg = True)
            if not ret:
                l2a_log.error("Tile failure: {}".format(log_gipp), print_msg = True)
                return ""

    if not create_sym_links([demmaccs_context.dem_hdr_file, dem_dir], working_dir, l2a_log):
        l2a_log.error("Tile failure: Could not create symbolic links for {0} and {1}".format(demmaccs_context.dem_hdr_file, dem_dir), print_msg = True)
        return ""

    start = time.time()
    maccs_mode = "L2INIT"
    prev_l2a_tile_path = []
    try:
        #demmaccs_context.prev_l2a_tiles.index will throw an exception if it will not find the tile_id inside prev_l2a_tiles
        #so the maccs mode will not be set to nominal
        idx = demmaccs_context.prev_l2a_tiles.index(tile_id)
        product_path = demmaccs_context.prev_l2a_products_paths[idx]
        print("product_path = {}".format(product_path))

        prev_l2a_tile_path = get_prev_l2a_tile_path(tile_id, product_path)

        l2a_log.error("Creating sym links for NOMINAL MACCS/MAJA mode: l2a prev tiles {}".format(prev_l2a_tile_path))
        if len(prev_l2a_tile_path) > 0 and create_sym_links(prev_l2a_tile_path, working_dir, l2a_log):
            #set MACCS mode to NOMINAL
            l2a_log.error("Created sym links for NOMINAL MACCS/MAJA mode for {}".format(prev_l2a_tile_path))
            maccs_mode = "L2NOMINAL"
        else:
            # something went wrong. shall this be an exit point?
            # shall the mode remain to L2INIT? This behavior may as well hide a bug in a previous demmaccs run (it's possible)...
            l2a_log.error("Tile failure: Could not create sym links for NOMINAL MACCS/MAJA mode for {}. Exit".format(prev_l2a_tile_path), print_msg = True)
            return ""
    except SystemExit:
        l2a_log.error("Tile failure: SystemExit caught when trying to create sym links for NOMINAL MACCS/MAJA mode, product {}. Exit!".format(demmaccs_context.input), print_msg = True)
        return ""
    except:
        l2a_log.info("No previous processed l2a tile found for {} in product {}. Running MACCS/MAJA in L2INIT mode".format(tile_id, product_name), print_msg = True)
        pass
    #MACCS bug. In case of setting the file status from VALD to NOTV, MACCS will try to create a directory LTC in the current running directory
    #which is / Of course, it will fail. That's why we have to move the current running directory to the MACCS temporary directory
    os.chdir(maccs_working_dir)

    guid = get_guid(8)
    if args.product_id:
        container_name = "maja_{}_{}".format(args.product_id, guid)
    else:
        container_name = "maja_{}".format(guid)

    cmd_array = []
    #docker run cmds
    cmd_array.append("docker")
    cmd_array.append("run")
    cmd_array.append("--rm")
    cmd_array.append("-u")
    cmd_array.append("{}:{}".format(os.getuid(), os.getgid()))

    for tile in prev_l2a_tile_path:
        cmd_array.append("-v")
        cmd_array.append("{}:{}".format(tile, tile))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(demmaccs_context.dem_hdr_file, demmaccs_context.dem_hdr_file))
    for tmp in tmp_tile_gipp:
        cmd_array.append("-v")
        cmd_array.append("{}:{}".format(tmp, tmp))  
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(dem_dir, dem_dir))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(args.gipp_dir, args.gipp_dir))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(demmaccs_context.input, demmaccs_context.input))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(working_dir, working_dir))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(maccs_working_dir, maccs_working_dir))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(args.conf, args.conf))
    cmd_array.append("--name")
    cmd_array.append(container_name)
    cmd_array.append(args.docker_image_maja)
    #actual maja command
    if demmaccs_context.maccs_address is not None:
        cmd_array.append("ssh")
        cmd_array.append(demmaccs_context.maccs_address)
    cmd_array += ["--input", working_dir,
                    "--TileId", tile_id,
                    "--output", maccs_working_dir,
                    "--mode", maccs_mode]
    if args.conf != "":
        cmd_array += ["--conf", args.conf]
    l2a_log.info("sat_id = {} | acq_date = {}".format(sat_id, acquistion_date))
    l2a_log.info("Starting MACCS/MAJA in {} for {} | TileID: {}".format(maccs_mode, demmaccs_context.input, tile_id))
    maja_log_path = os.path.join(demmaccs_context.output, MAJA_LOG_FILE_NAME)
    maja_log = LogHandler(maja_log_path, MAJA_LOG_FILE_NAME, l2a_log.level, NO_ID)
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    l2a_log.info("Running command: " + cmd_str, print_msg = True)
    l2a_log.info(
        "Running Maja, console output can be found at {}".format(maja_log.path),
        print_msg = True
    )
    running_containers.add(container_name)
    maja_run_val = run_command(cmd_array, maja_log)
    running_containers.remove(container_name)
    if maja_run_val:
        l2a_log.error("MACCS/MAJA mode {} didn't work for {} | TileID: {}. Location {}".format(maccs_mode, demmaccs_context.input, tile_id, demmaccs_context.output), print_msg = True)
        return ""
    else:
        l2a_log.info("MACCS/MAJA mode {} for {} tile {} finished in: {}. Location: {}".format(maccs_mode, demmaccs_context.input, tile_id, datetime.timedelta(seconds=(time.time() - start)), demmaccs_context.output), print_msg = True)
    
    
    #postprocess tif/cog translation and/or sre/fre removable
    if postprocess(maccs_working_dir):
        l2a_log.info("Maja postprocessing succesfull for: {} ".format(demmaccs_context.input))
    else:
        l2a_log.error("Maja postprocessing did NOT work for: {} ".format(demmaccs_context.input), print_msg = True)
        return ""
    
    # move the maccs output to the output directory.
    # only the valid files should be moved
    maccs_report_file = glob.glob("{}/*_L*REPT*.EEF".format(maccs_working_dir))
    new_maccs_report_file = ""
    return_tile_id = ""
    try:
        # First, move the report log that maccs created it. Take care, first maccs creates a report file named PMC_LxREPT.EEF.
        # When it finishes, maccs will rename this file to something like S2A_OPER_PMC_L2REPT_{tile_id}____{dateofl1cproduct}.EEF
        # Sometimes (usually when it crashes or for different reasons stops at the beginning), this renaming does not take place,
        # so the report file will remain PMC_LxREPT.EEF
        # This report file (doesn't matter the name) will be kept and save to the working_dir with the name MACCS_L2REPT_{tile_id}.EEF
        l2a_log.info("Searching for report maccs file (REPT) in: {}".format(maccs_working_dir))
        if len(maccs_report_file) >= 1:
            if len(maccs_report_file) > 1:
                l2a_log.warning("More than one report maccs file (REPT) found in {}. Only the first one will be kept. Report files list: {}.".format(maccs_working_dir, maccs_report_file), print_msg = True)
            l2a_log.info("Report maccs file (REPT) found in: {} : {}".format(maccs_working_dir, maccs_report_file[0]), print_msg = True)
            new_maccs_report_file = "{}/MACCS_L2REPT_{}.EEF".format(demmaccs_context.output[:len(demmaccs_context.output) - 1] if demmaccs_context.output.endswith("/") else demmaccs_context.output, tile_id)
            if os.path.isdir(new_maccs_report_file):
                l2a_log.warning("The directory {} already exists. Trying to delete it in order to move the new created directory by MACCS/MAJA".format(new_maccs_report_file), print_msg = True)
                shutil.rmtree(new_maccs_report_file)
            elif os.path.isfile(new_maccs_report_file):
                l2a_log.warning("The file {} already exists. Trying to delete it in order to move the new created file by MACCS/MAJA".format(new_maccs_report_file), print_msg = True)
                os.remove(new_maccs_report_file)
            else: #the destination does not exist, so move the files
                pass
            l2a_log.info("Moving {} to {}".format(maccs_report_file[0], new_maccs_report_file))
            shutil.move(maccs_report_file[0], new_maccs_report_file)
        working_dir_content = glob.glob("{}/*".format(maccs_working_dir))
        l2a_log.info("Searching for valid products in working dir: {}. Following is the content of this dir: {}".format(maccs_working_dir, working_dir_content))
        #check for MACCS format
        output_format, maja_dir = get_l1c_processor_output_format(maccs_working_dir, tile_id)
        print("output_format = {}, maja_dir = {}".format(output_format, maja_dir))
        if output_format == MACCS_PROCESSOR_OUTPUT_FORMAT:
            l2a_log.info("MACCS output format found. Searching output for valid results")
            return_tile_id = "{}".format(tile_id)
            l2a_log.info("Found valid tile id {} in {}. Moving all the files to destination".format(tile_id, maccs_working_dir))
            for maccs_out in working_dir_content:
                new_file = os.path.join(demmaccs_context.output, os.path.basename(maccs_out))
                if os.path.isdir(new_file):
                    l2a_log.warning("The directory {} already exists. Trying to delete it in order to move the new created directory by MACCS".format(new_file), print_msg = True)
                    shutil.rmtree(new_file)
                elif os.path.isfile(new_file):
                    l2a_log.warning("The file {} already exists. Trying to delete it in order to move the new created file by MACCS".format(new_file), print_msg = True)
                    os.remove(new_file)
                else: #the dest does not exist, so it will be moved without problems
                    pass
                l2a_log.info("Moving {} to {}".format(maccs_out, new_file))
                shutil.move(maccs_out, new_file)
        elif output_format == THEIA_MUSCATE_OUTPUT_FORMAT and maja_dir is not None:
            #check for THEIA/MUSCATE format
            l2a_log.info( "Demmaccs_context.output,THEIA/MUSCATE ouput format found. Searching output for valid results")            
            new_file = os.path.join(demmaccs_context.output, os.path.basename(maja_dir))
            if os.path.isdir(new_file):
                l2a_log.warning("The directory {} already exists. Trying to delete it in order to move the new created directory by MAJA".format(new_file), print_msg = True)
                shutil.rmtree(new_file)
            elif os.path.isfile(new_file):
                l2a_log.warning("The file {} already exists. Trying to delete it in order to move the new created file by MAJA".format(new_file), print_msg = True)
                os.remove(new_file)
            else: #the dest does not exist, so it will be moved without problems
                pass
            
            is_valid_maja_jpi = get_maja_jpi_log_extract(maccs_working_dir, demmaccs_context)
            if is_valid_maja_jpi == True :
                l2a_log.info("Moving {} to {}".format(maja_dir, new_file))
                shutil.move(maja_dir, new_file)
                return_tile_id = "{}".format(tile_id)
            else :
                l2a_log.error("No valid products (JPI L2NOTV status) found in: {}.".format(maccs_working_dir), print_msg = True)
        else:
            l2a_log.error("No valid products (MACCS VALD status or THEIA/MUSCATE formats) found in: {}.".format(maccs_working_dir), print_msg = True)
        l2a_log.info("Erasing the MACCS/MAJA working directory: rmtree: {}".format(maccs_working_dir))
        shutil.rmtree(maccs_working_dir)
    except Exception as e:
        return_tile_id = ""
        l2a_log.error("Tile failure: Exception caught when moving maccs files for tile {} to the output directory {}: {}".format(tile_id, demmaccs_context.output, e), print_msg = True, trace = True)
 
    return return_tile_id

def postprocess(working_dir):
    #remove and compress sre/fre and transform to cog/compressed tiffs
    if args.removeSreFiles:
        sre_path = os.path.join(working_dir, "**/*SRE*")
        sre_files = glob.glob(sre_path)
        for sre in sre_files:
            try:
                os.remove(sre)
            except Exception as e:
                print("Can not remove SRE file {} due to {}".format(sre, e))
                return False

    if args.removeFreFiles:
        fre_path = os.path.join(working_dir, "**/*FRE*")
        fre_files = glob.glob(fre_path)
        for fre in fre_files:
            try:
                os.remove(fre)
            except Exception as e:
                print("Can not remove SRE file {} due to {}".format(fre, e))
                return False

    #translate if needed
    if args.compressTiffs or args.cogTiffs:
        tif_files = []
        tif_path = os.path.join(working_dir, "**/*.tif")
        tif_files.extend(glob.glob(tif_path))
        TIF_path = os.path.join(working_dir, "**/*.TIF")
        tif_files.extend(glob.glob(TIF_path))
        for tif in tif_files:
            tif_dir = os.path.dirname(tif)
            new_img_name = os.path.basename(tif)[:-4] + "_tmp" + os.path.basename(tif)[-4:]
            if args.cogTiffs:
                img_format = "COG"
            else:
                img_format = "GTiff"
            guid = get_guid(8)
            if args.product_id:
                container_name = "gdal_{}_{}".format(args.product_id, guid)
            else:
                container_name = "gdal_{}".format(guid)
            running_containers.add(container_name)
            ret_translate = translate(
                input_img = tif,
                output_dir = tif_dir,
                output_img_name = new_img_name,
                output_img_format = img_format,
                log = l2a_log,
                gdal_image = args.docker_image_gdal,
                name = args.product_id,
                compress = args.compressTiffs,
            )
            running_containers.remove(container_name)
            if ret_translate:
                try:
                    new_img_path = os.path.join(tif_dir, new_img_name)
                    os.remove(tif)
                    os.rename(new_img_path, tif)
                except Exception as e:
                    l2a_log.error(
                        "Can NOT rename the translated TIFF/COG translation for {} command due to: {}.".format(tif,e),
                        print_msg = True
                    )
                    return False
            else:
                l2a_log.error(
                    "Gdal can NOT convert {}.".format(tif),
                    print_msg = True
                )
                return False

    return True

def signal_handler(signum, frame):

    print("(Maja info) Signal caught: {}.".format(signum))
    l2a_log.error(
        "Signal caught: {}.".format(signum),
        print_msg = True
    )
    stop()

def stop():
    global running_containers, l2a_log 

    stop_containers(running_containers, l2a_log)
    os._exit(0)

parser = argparse.ArgumentParser(
    description="Launches DEM and MACCS/MAJA for L2A product creation")
parser.add_argument('input', help="input L1C directory")
parser.add_argument('-t', '--tiles-to-process', required=False, nargs='+', help="only this tiles shall be processed from the whole product", default=None)
parser.add_argument('-w', '--working-dir', required=True,
                    help="working directory")
parser.add_argument('--dem', required=True, help="DEM dataset path")
parser.add_argument('--swbd', required=True, help="SWBD dataset path")
parser.add_argument('--gipp-dir', required=True, help="directory where gip are to be found")
parser.add_argument('--processes-number-dem', required=False,
                        help="number of processes to run DEM in parallel", default="3")
parser.add_argument('--processes-number-maccs', required=False,
                        help="number of processes to run MACCS/MAJA in parallel", default="2")
parser.add_argument('--maccs-address', required=False, help="MACCS/MAJA has to be run from a remote host. This should be the ip address of the pc where MACCS/MAJA is to be found")
parser.add_argument('--prev-l2a-tiles', required=False,
                        help="Previous processed tiles from L2A product", default=[], nargs="+")
parser.add_argument('--prev-l2a-products-paths', required=False,
                        help="Path of the previous processed tiles from L2A product", default=[], nargs="+")
parser.add_argument('--delete-temp', required=False, action="store_true",
                        help="if set, it will delete all the temporary files and directories.")
parser.add_argument('--suffix-log-name', required=False,
                        help="if set, the string will be part of the log filename . Default: null", default=None)
parser.add_argument('output', help="output location")
parser.add_argument("--conf", required=True,
                        help = "Configuration file for maja")
parser.add_argument("--product-id", required=False,
                        help = "The id of the product been processed")
parser.add_argument("--removeSreFiles", required=False, action="store_true",
                        help = "Removes Sre files from the computed l2a product")
parser.add_argument("--removeFreFiles", required=False, action="store_true",
                        help = "Removes Fre files from the computed l2a product")
parser.add_argument("--compressTiffs", required=False, action="store_true",
                        help = "Compresses TIFF files")
parser.add_argument("--cogTiffs", required=False, action="store_true",
                        help = "Translastes TIFF to COF")
parser.add_argument(
    "--docker-image-l8align",
    required=True,
    help="Name of the l8 align docker image.",
)
parser.add_argument(
    "--docker-image-dem",
    required=True,
    help="Name of the dem docker image.",
)
parser.add_argument(
    "--docker-image-maja",
    required=True,
    help="Name of the maja docker image.",
)
parser.add_argument(
    "--docker-image-gdal",
    required=True,
    help="Name of the gdal docker image.",
)
parser.add_argument('-l', '--log-level', default = 'info',
                    choices = ['debug' , 'info', 'warning' , 'error', 'critical'], 
                    help = 'Minimum logging level')

args = parser.parse_args()
if args.product_id:
    l2a_log_filename = "l2a_{}.log".format(args.product_id)
else:
    l2a_log_filename = "l2a.log"
l2a_log_path = os.path.join(args.output, l2a_log_filename)    
l2a_log = LogHandler(l2a_log_path, "l2a_log", args.log_level, NO_ID)
running_containers = set()

if not create_recursive_dirs(args.output):
    l2a_log.critical("Could not create the output directory", print_msg = True)
    os._exit(1)

if len(args.prev_l2a_tiles) != len(args.prev_l2a_products_paths):
    l2a_log.critical("The number of previous l2a tiles is not the same as for paths for these tiles. Check args: --prev-l2-tiles and --prev-l2a-products-paths, the length should be equal", print_msg = True)
    os._exit(1)

if args.product_id:
    working_dir_name =  "tmp_" + args.product_id
    working_dir = os.path.join(args.working_dir, working_dir_name)
    if not create_recursive_dirs(working_dir):
        l2a_log.critical("Could not create the temporary directory", print_msg = True)
        os._exit(1)
    else:
        l2a_log.info("Created the working dir", print_msg = True)
else:
    working_dir = tempfile.mkdtemp(dir = args.working_dir)
if working_dir.endswith("/"):
    working_dir = working_dir[:-1]
remove_dir_content(working_dir)

dem_working_dir = "{}_DEM_TMP".format(working_dir)
dem_output_dir = "{}_DEM_OUT".format(working_dir)
if not create_recursive_dirs(dem_output_dir):
    l2a_log.critical("Could NOT create the output directory for DEM", print_msg = True)
    if not remove_dir(working_dir):
        l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
    os._exit(1)
remove_dir_content(dem_output_dir)

if not create_recursive_dirs(dem_working_dir):
    l2a_log.critical("Could not create the working directory for DEM", print_msg = True)
    if not remove_dir(working_dir):
        l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
    os._exit(1)
remove_dir_content(dem_working_dir)

l2a_log.info("working_dir = {}".format(working_dir), print_msg = True)
l2a_log.info("dem_working_dir = {}".format(dem_working_dir), print_msg = True)
l2a_log.info("dem_output_dir = {}".format(dem_output_dir), print_msg = True)


general_start = time.time()
start = time.time()
base_abs_path = os.path.dirname(os.path.abspath(__file__)) + "/"
product_name = os.path.basename(args.input[:len(args.input) - 1]) if args.input.endswith("/") else os.path.basename(args.input)
sat_id, acquistion_date = get_product_info(product_name)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# crop the LANDSAT products for the alignment
if sat_id == LANDSAT8_SATELLITE_ID:

    l8_align_wrk_dir = os.path.join(working_dir, "l8_align_tmp")
    if not create_recursive_dirs(l8_align_wrk_dir):
        l2a_log.error("Could not create the l8 align working directory.", print_msg = True)
        if not remove_dir(working_dir):
            l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
        os._exit(1)

    guid = get_guid(8)
    if args.product_id:
        container_name = "l8align_{}_{}".format(args.product_id, guid)
    else:
        container_name = "l8align_{}".format(guid)

    cmd_array = []
    #docker run 
    cmd_array.append("docker")
    cmd_array.append("run")
    cmd_array.append("--rm")
    cmd_array.append("-u")
    cmd_array.append("{}:{}".format(os.getuid(), os.getgid()))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(args.input,args.input))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(working_dir,working_dir))
    cmd_array.append("-v")
    cmd_array.append("{}:{}".format(l8_align_wrk_dir,l8_align_wrk_dir))
    cmd_array.append("--name")
    cmd_array.append(container_name)
    cmd_array.append(args.docker_image_l8align)

    #actual align command
    base_l8align_path = "/usr/share/sen2agri"
    l8_align_script_path = os.path.join(base_l8align_path, "l8_alignment/l8_align.py")
    cmd_array.append(l8_align_script_path)
    cmd_array.append("-i")
    cmd_array.append(args.input)
    cmd_array.append("-o")
    cmd_array.append(working_dir)
    cmd_array.append("-v")
    wrs_script_path = os.path.join(base_l8align_path, "wrs2_descending/wrs2_descending.shp")
    cmd_array.append(wrs_script_path)
    cmd_array.append("-w")
    cmd_array.append(l8_align_wrk_dir)
    cmd_array.append("-t")
    cmd_array.append(product_name)
    
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    l2a_log.info("Running command: " + cmd_str, print_msg = True)
    start_time = time.time()
    running_containers.add(container_name)
    ret = run_command(cmd_array, l2a_log)
    running_containers.remove(container_name)
    end_time = time.time()
    l2a_log.info(
        "Command finished with return code {} in {}".format(ret, datetime.timedelta(seconds=(end_time - start_time))),
        print_msg = True
    )

    if ret != 0:
        l2a_log.critical("The LANDSAT8 product could not be aligned {}".format(args.input), print_msg = True)
        if not remove_dir(working_dir):
            l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
        os._exit(1)
    #the l8.align.py outputs in the working_dir directory where creates a directory which has the product name
    args.input = working_dir + "/" + product_name
    l2a_log.info("The LANDSAT8 product was aligned here: {}".format(args.input), print_msg = True)

tiles_to_process = []
print("Creating DEMs for {}".format(args.input))
if args.tiles_to_process is not None :
    tiles_to_process = args.tiles_to_process
    args.processes_number_dem = str(len(tiles_to_process))
    args.processes_number_maccs = str(len(tiles_to_process))
print("tiles_to_process = {}".format(tiles_to_process))

guid = get_guid(8)
if args.product_id:
    container_name = "dem_{}_{}".format(args.product_id, guid)
else:
    container_name = "dem_{}".format(guid)

dem_command = []
#docker run
dem_command.append("docker")
dem_command.append("run")
dem_command.append("--rm")
dem_command.append("-u")
dem_command.append("{}:{}".format(os.getuid(), os.getgid()))
dem_command.append("-v")
dem_command.append("{}:{}".format(args.dem, args.dem))
dem_command.append("-v")
dem_command.append("{}:{}".format(args.swbd, args.swbd))
dem_command.append("-v")
dem_command.append("{}:{}".format(dem_working_dir, dem_working_dir))
dem_command.append("-v")
dem_command.append("{}:{}".format(args.input, args.input))
dem_command.append("-v")
dem_command.append("{}:{}".format(dem_output_dir, dem_output_dir))
dem_command.append("--name")
dem_command.append(container_name)
dem_command.append(args.docker_image_dem)
#dem_command.append("/bin/bash")
dem_command.append("/usr/bin/dem.py")
dem_command.append("--dem")
dem_command.append(args.dem)
dem_command.append("--swbd")
dem_command.append(args.swbd)
dem_command.append("-p")
dem_command.append(args.processes_number_dem)
dem_command.append("-w")
dem_command.append(dem_working_dir)
dem_command.append(args.input)
dem_command.append(dem_output_dir)
if len(tiles_to_process) > 0:
    dem_command.append("-l")
    dem_command += tiles_to_process

cmd_str = " ".join(map(pipes.quote, dem_command))
l2a_log.info("Running cmd: " + cmd_str)
start_time = time.time()
running_containers.add(container_name)
ret = run_command(dem_command, l2a_log)
running_containers.remove(container_name)
end_time = time.time()
l2a_log.info(
    "Command finished with return code {} in {}".format(ret, datetime.timedelta(seconds=(end_time - start_time))),
    print_msg = True
)

if ret != 0:
    l2a_log.critical("DEM failed", print_msg = True)
    if not remove_dir(working_dir):
        l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
    os._exit(1)

l2a_log.info("DEM finished in: {}".format(datetime.timedelta(seconds=(time.time() - start))), print_msg = True)

dem_hdr_path = os.path.join(dem_output_dir, "*.HDR")
dem_hdrs = glob.glob(dem_hdr_path)
l2a_log.info("DEM output directory {} has DEM hdrs = {}".format(dem_output_dir, dem_hdrs), print_msg = True)
if len(dem_hdrs) == 0:
    l2a_log.critical("There are no hdr DEM files in {}".format(dem_output_dir), print_msg = True)
    if args.delete_temp:
        if not remove_dir(dem_working_dir):
            l2a_log.warning("Couldn't remove the dem working dir {}".format(dem_working_dir), print_msg = True)
        if not remove_dir(dem_output_dir):
            l2a_log.warning("Couldn't remove the dem output dir {}".format(dem_output_dir), print_msg = True)
        if not remove_dir(working_dir):
            l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)
    os._exit(1)

if len(tiles_to_process) > 0 and len(tiles_to_process) != len(dem_hdrs):
    l2a_log.error("The number of hdr DEM files in {} is not equal with the number of the received tiles to process !!".format(dem_output_dir), print_msg = True)
demmaccs_contexts = []
print("Creating demmaccs contexts with: input: {} | output {}".format(args.input, args.output))
for dem_hdr in dem_hdrs:
    print("DEM_HDR: {}".format(dem_hdr))
    demmaccs_contexts.append(DEMMACCSContext(working_dir, dem_hdr, args.gipp_dir, args.prev_l2a_tiles, args.prev_l2a_products_paths, args.maccs_address, args.input, args.output))

processed_tiles = []
if len(demmaccs_contexts) == 1:
    print("One process will be started for this demmaccs")
    out = maccs_launcher(demmaccs_contexts[0], dem_output_dir)
    if len(out) >=5:
        processed_tiles.append(out)
else:
    #RELEASE mode, parallel launching
    # LE (august 2018): keeping parallel launching for compatibility. Now, demmaccs is launched for one tile only
    print("Parallel launching")
    pool = Pool(int(args.processes_number_maccs))
    pool_outputs = pool.map(maccs_launcher, demmaccs_contexts)
    pool.close()
    pool.join()

    #DEBUG mode only, sequentially launching
    #pool_outputs = map(maccs_launcher, demmaccs_contexts)

    for out in pool_outputs:
        if len(out) >=5:
            processed_tiles.append(out)

ret = 0
if len(processed_tiles) == 0:
    l2a_log.error("MACCS/MAJA did not process any tiles for L1C product {}".format(args.input), print_msg = True)
    ret = 1
else:
    l2a_log.info("MACCS/MAJA processed the following tiles for L1C product {} :".format(args.input), print_msg = True)
    l2a_log.info("{}".format(processed_tiles), print_msg = True)

if args.delete_temp:
    l2a_log.info("Remove all the temporary files and directory", print_msg = True)
    if not remove_dir(dem_working_dir):
        l2a_log.warning("Couldn't remove the dem working dir {}".format(dem_working_dir), print_msg = True)
    if not remove_dir(dem_output_dir):
        l2a_log.warning("Couldn't remove the dem output dir {}".format(dem_output_dir), print_msg = True)
    if not remove_dir(working_dir):
        l2a_log.warning("Couldn't remove the temp dir {}".format(working_dir), print_msg = True)

l2a_log.info("Total execution {}:".format(datetime.timedelta(seconds=(time.time() - general_start))), print_msg = True)
l2a_log.close()
os._exit(ret)

