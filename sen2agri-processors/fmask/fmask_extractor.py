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
import argparse
import glob
import os
import time
import datetime
import shutil
import tempfile
import signal
from l2a_commons import run_command, create_recursive_dirs, remove_dir, get_guid, stop_containers
from l2a_commons import Log, DL, IL, WL, EL, CL

DEFAULT_FMASK_IMAGE_NAME = "fmask"


def get_dir_name_from_path(dir_path):
    if dir_path.endswith("/"):
        basename = os.path.basename(dir_path[:len(dir_path) - 1])
    else:
        basename = os.path.basename(dir_path)    
    
    return basename
        
def create_sym_links(filenames, target_directory, log_path, log_filename):

    for file_to_sym_link in filenames:
        #target name
        if file_to_sym_link.endswith("/"):
            basename = os.path.basename(file_to_sym_link[:len(file_to_sym_link) - 1])
        else:
            basename = os.path.basename(file_to_sym_link)
        target = os.path.join(target_directory, basename)
        #does it already exist?
        if os.path.isfile(target) or os.path.isdir(target):
            fmask_log.write(IL, "The path {} does exist already".format(target), print_msg = True)
            #skip it
            continue
        #create it
        if run_command(["ln", "-s", file_to_sym_link, target_directory], log_path, log_filename) != 0:
            return False
    return True

def clone_product_dir(src_dir, target_directory, log_path, log_filename) :
    src_dir_name = get_dir_name_from_path(src_dir)

    for root, subdirs, files in os.walk(src_dir):
        src_rel_dir_path = os.path.relpath(root, src_dir)
        if src_rel_dir_path == "." :
            src_rel_dir_path = ""
        if not create_recursive_dirs(os.path.join(target_directory, src_dir_name, src_rel_dir_path)):
            return False
        for filename in files:
            file_path = os.path.join(root, filename)
            src_rel_file_path = os.path.relpath(file_path, src_dir)
            if not create_sym_links([file_path], os.path.join(target_directory, src_dir_name, src_rel_dir_path), log_path, log_filename):
                return False
    return True

class FMaskContext(object):
    def __init__(self, base_working_dir, l1c_input, l2a_output):
        self.base_working_dir = base_working_dir
        self.input = l1c_input
        self.output = l2a_output

def fmask_launcher(fmask_context):
    product_name = os.path.basename(fmask_context.input[:len(fmask_context.input) - 1]) if fmask_context.input.endswith("/") else os.path.basename(fmask_context.input)

    if not clone_product_dir(fmask_context.input, working_dir, fmask_context.output, log_filename) : 
        fmask_log.write(EL, "Product failure: Could not create sym links for {}".format(fmask_context.input), print_msg = True)
        return ""

    prd_name = get_dir_name_from_path(fmask_context.input)
    fmask_working_dir = os.path.join(working_dir, prd_name)

    start = time.time()
    
    docker_work_dir = ""
    # check for S2 product
    is_s2 = False
    fmask_file_out_location = ""
    granules_path = os.path.join(fmask_context.input,"GRANULE")
    if os.path.isdir(granules_path):
        for sub_dir_tupple in os.walk(granules_path):
            print ("SubdirT = {}".format(sub_dir_tupple))
            if len(sub_dir_tupple[1]) == 0 : 
                continue
            sub_dir = sub_dir_tupple[1][0]
            print ("Subdir = {}".format(sub_dir))
            if sub_dir.startswith("L1C_T") :
                docker_work_dir = os.path.join(prd_name, "GRANULE", sub_dir)
                is_s2 = True
                fmask_out_location = os.path.join(fmask_working_dir, "GRANULE", sub_dir, "FMASK_DATA")
                break
    else : 
        # assume an L8 product
        docker_work_dir = prd_name
        fmask_out_location = fmask_working_dir

    cmd_array = []
    debug = False
    if debug == True:
        cmd_array1 = []
        cmd_array1 += ["mkdir", "-p", fmask_out_location]
        run_command(cmd_array1, fmask_context.output, log_filename)
        cmd_array += ["cp", "-f", "/mnt/archive/test/fmask/TestFile/L1C_T33UVQ_A019710_20190401T100512_Fmask4.tif", fmask_out_location]
    else :
        guid = get_guid(8)
        if args.product_id:
            container_name = "fmask_{}_{}".format(args.product_id, guid)
        else:
            container_name = "fmask_{}".format(guid)

        cmd_array.append("docker")
        cmd_array.append("run")
        cmd_array.append("--rm")
        cmd_array.append("-u")
        cmd_array.append("{}:{}".format(os.getuid(), os.getgid()))
        cmd_array.append("-v")
        cmd_array.append("{}:{}".format(fmask_context.input, fmask_context.input))
        cmd_array.append("-v")
        cmd_array.append("{}:/work/{}".format(fmask_working_dir, prd_name))
        cmd_array.append("--workdir")
        cmd_array.append("/work/{}".format(docker_work_dir))
        cmd_array.append("--name")
        cmd_array.append(container_name)
        cmd_array.append(args.image_name)
        if args.cloud_dilation:
            cmd_array.append(args.cloud_dilation)
        else:
            cmd_array.append("3")
        if args.cloud_shadow_dilation:
            cmd_array.append(args.cloud_shadow_dilation)
        else:
            cmd_array.append("3")
        if args.snow_dilation:
            cmd_array.append(args.snow_dilation)
        else:
            cmd_array.append("0")
        if args.threshold: 
            cmd_array.append(args.threshold)
    
    fmask_log.write(IL, "Starting FMask in {}".format(fmask_context.input), print_msg = True)
    fmask_log.write(IL, "FMask: {}".format(cmd_array), print_msg = True)
    running_containers.add(container_name)
    if run_command(cmd_array, fmask_context.output, log_filename) != 0:
        fmask_log.write(EL, "FMask didn't work for {}. Location {}".format(fmask_context.input, fmask_context.output), print_msg = True)
    else:
        fmask_log.write(IL, "FMask for {} finished in: {}. Location: {}".format(fmask_context.input, datetime.timedelta(seconds=(time.time() - start)), fmask_context.output), print_msg = True)
    running_containers.remove(container_name)
    # move the fmask output to the output directory.
    # only the valid files should be moved
    fmask_out_file = glob.glob("{}/*_Fmask4.tif".format(fmask_out_location))
    new_fmask_out_file = ""
    try:
        # move the FMask file
        fmask_log.write(IL, "Searching for FMask file in: {}".format(fmask_out_location), print_msg = True)
        if len(fmask_out_file) >= 1:
            if len(fmask_out_file) > 1:
                fmask_log.write(WL, "More than one FMask files found in {}. Only the first one will be kept. FMask files list: {}.".format(fmask_working_dir, fmask_out_file), print_msg = True)
            fmask_log.write(IL, "FMask file found in: {} : {}".format(fmask_working_dir, fmask_out_file[0]), print_msg = True)
            basename = os.path.basename(fmask_out_file[0])
            new_fmask_out_file = "{}/{}".format(fmask_context.output[:len(fmask_context.output) - 1] if fmask_context.output.endswith("/") else fmask_context.output, basename)
            if os.path.isdir(new_fmask_out_file):
                fmask_log.write(IL, "The directory {} already exists. Trying to delete it in order to move the new created directory by FMask".format(new_fmask_out_file), print_msg = True)
                shutil.rmtree(new_fmask_out_file)
            elif os.path.isfile(new_fmask_out_file):
                fmask_log.write(IL, "The file {} already exists. Trying to delete it in order to move the new created file by FMask".format(new_fmask_out_file), print_msg = True)
                os.remove(new_fmask_out_file)
            else: #the destination does not exist, so move the files
                pass
            fmask_log.write(IL, "Moving {} to {}".format(fmask_out_file[0], new_fmask_out_file), print_msg = True)
            shutil.move(fmask_out_file[0], new_fmask_out_file)
        else:
            fmask_log.write(EL, "No FMask file found in: {}.".format(fmask_working_dir), print_msg = True)
        fmask_log.write(IL, "Erasing the FMask working directory: rmtree: {}".format(fmask_working_dir), print_msg = True)
        shutil.rmtree(fmask_working_dir)
    except Exception as e:
        new_fmask_out_file = ""
        fmask_log.write(EL, "FMask product failure: Exception caught when moving fmask files  to the output directory {}: {}".format(fmask_context.output, e), print_msg = True)
 
    return new_fmask_out_file

def signal_handler(signum, frame):
    global fmask_context, log_filename

    print("(FMask info) Signal caught: {}.".format(signum))
    fmask_log.write(
        IL,
        "(FMask info) Signal caught: {}.".format(signum),
        print_msg = True,
    )
    stop()

def stop():
    global running_containers, fmask_context, log_filename

    stop_containers(running_containers, fmask_context.output, log_filename)
    os._exit(0)

parser = argparse.ArgumentParser(
    description="Launches FMask for producing cloud/water/snow masks")
parser.add_argument('input', help="input L1C directory")
parser.add_argument('output', help="output location")
parser.add_argument('-w', '--working-dir', required=True,
                    help="working directory")
parser.add_argument('-t', '--threshold', required=False, default = "", 
                    help="FMask threshold")            
parser.add_argument('--delete-temp', required=False, action="store_true",
                        help="if set to True, it will delete all the temporary files and directories.")
parser.add_argument('--product-id', required=False,
                    help = "Downloader history id of the input product.")
parser.add_argument('--image-name', required=False, default = DEFAULT_FMASK_IMAGE_NAME,
                    help = "The name of the fmask docker image.")
parser.add_argument('--cloud-dilation', required = False, default = 3,
                    help = "Number of dilated pixels for cloud")
parser.add_argument('--cloud-shadow-dilation', required = False, default = 3,
                    help = "Number of dilated pixels for cloud shadow")
parser.add_argument('--snow-dilation', required = False, default = 0,
                    help = "Number of dilated pixels for snow")
parser.add_argument('--log-level', required= False, default = IL,
                    help = "Log level")
args = parser.parse_args()

running_containers = set()
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

general_log_path = args.output
if args.product_id:
    log_filename = "fmask_{}.log".format(args.product_id)
else:
    log_filename = "fmask.log"
fmask_log = Log("fmask_log", general_log_path, args.log_level)
if not create_recursive_dirs(args.output):
    fmask_log.write(CL, "Could not create the output directory", print_msg= True)
    os._exit(1)

working_dir = tempfile.mkdtemp(dir = args.working_dir)
fmask_log.write(IL, "working_dir = {}".format(working_dir), print_msg= True)
general_start = time.time()

if not os.path.isdir(working_dir):
    fmask_log.write(CL, "Could not create the temporary directory", print_msg = True)
    os._exit(1)

start = time.time()
base_abs_path = os.path.dirname(os.path.abspath(__file__)) + "/"
product_name = os.path.basename(args.input[:len(args.input) - 1]) if args.input.endswith("/") else os.path.basename(args.input)

print("Creating fmask contexts with: input: {} | output {}".format(args.input, args.output))
fmask_context = FMaskContext(working_dir, args.input, args.output)

processed_tiles = [] 
out = fmask_launcher(fmask_context)
if len(out) >=5:
    processed_tiles.append(out)

exit_code = 0
if len(processed_tiles) == 0:
    fmask_log.write(EL, "FMASK did NOT process the L1C product {}".format(args.input), print_msg = True)
    exit_code = 1
else:
    fmask_log.write(IL, "FMask processed the following tiles for L1C product {} :".format(args.input), print_msg = True)
    fmask_log.write(IL, "{}".format(processed_tiles), print_msg = True)

if args.delete_temp:
    fmask_log.write(IL, "Remove all the temporary files and directory", print_msg = True)
    if not remove_dir(working_dir):
        fmask_log.write(WL, "Couldn't remove the temp dir {}".format(working_dir), print_msg = True)

fmask_log.write(IL, "Total execution {}:".format(datetime.timedelta(seconds=(time.time() - general_start))), print_msg = True)

os._exit(exit_code)
