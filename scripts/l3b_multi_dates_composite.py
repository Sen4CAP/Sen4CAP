#!/usr/bin/env python
import argparse

import os
import time
import glob
import subprocess
import uuid
import shutil

def move_files_to_subdir(files, target_subdir):
    """
    Creates a subdirectory if it does not exist,
    and moves a list of files into it.
    
    :param files: list of file paths to move
    :param target_subdir: path to target subdirectory
    """
    # Create subdirectory if missing
    # os.makedirs(target_subdir, exist_ok=True)
    
    try:
        os.makedirs(target_subdir)
    except OSError as e:
        if os.path.isdir(target_subdir):
            # Directory already exists, ignore
            pass
        else:
            # Some other error (e.g., permission denied)
            raise

    for file_path in files:
        if os.path.isfile(file_path):                           # ensure it's a file
            filename = os.path.basename(file_path)
            destination = os.path.join(target_subdir, filename)
            shutil.move(file_path, destination)
            print("Moved: {} -> {}".format(file_path,destination))
        else:
            print("Skipped (not found): {}".format(file_path))

def run_composite_cmd(data_dir, data_files, msk_files, no_data_val, method, zero_is_valid, run_docker):
    """
    Run a docker container with a given command and list of files.
    """
    
    out_file_tmp = os.path.join(data_dir, str(uuid.uuid4()) + ".tif")
    
    cmd = []
    if run_docker : 
        cmd = ["docker", "run", "--rm", "-u", "1003:1003", "--group-add", "1033", "-v", "/var/run/docker.sock:/var/run/docker.sock", "-v", "/mnt/archive:/mnt/archive", "-v", "/etc/sen2agri:/etc/sen2agri", "-v", "/etc/sen2agri/sen2agri.conf:/etc/sen2agri/sen2agri.conf", "-v", "/mnt/data/archive:/mnt/data/archive",  "sen4x/sen4cap-processors:5.0.0"]
        
    cmd.append("otbcli")
    cmd.append("Sen4XGenericComposite")
    
    cmd.append("-bv")
    cmd.append(no_data_val)

    cmd.append("-mskvld")
    cmd.append("0")

    cmd.append("-method")
    cmd.append(method)

    cmd.append("-zv")
    cmd.append(zero_is_valid)

    cmd.append("-out")
    cmd.append(out_file_tmp)
    
    cmd.append("-il");
    cmd.extend(data_files)    # add all file paths as arguments

    if len(msk_files) > 0:
        cmd.append("-msks");
        cmd.extend(msk_files)   

    print("Running:", " ".join(cmd))
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print("Docker command failed with exit code {}".format(e.returncode))
        return 

    move_files_to_subdir(data_files, os.path.join(data_dir, "COMPOSITING_INPUTS"))
    out_file_path = data_files[0]
    if not os.path.exists(out_file_path):
        shutil.move(out_file_tmp, out_file_path)
    else :
        print("Cannot move file {} to {}. Destination still exists ... ".format(out_file_tmp, data_files[0]))

def handle_img_data(data_dir, data_files, msk_files, substring, tile_name, use_docker):
    """Called when multiple IMG_DATA files are found for a substring."""
    print("[{}] IMG_DATA duplicates found for '{}':".format(tile_name, substring))
    for f in data_files:
        print("   {}".format(f))
    
    run_composite_cmd(data_dir, data_files, msk_files, "-10000", "mean", "0", use_docker)

def handle_qi_data(data_dir, data_files, msk_files, substring, tile_name, use_docker):
    """Called when multiple QI_DATA files are found for a substring."""
    print("[{}] QI_DATA duplicates found for '{}':".format(tile_name, substring))
    for f in data_files:
        print("   {}".format(f))
    
    run_composite_cmd(data_dir, data_files, msk_files, "255", "min", "1", use_docker)

def get_files(data_dir, substr) :
    matches = []
    if os.path.isdir(data_dir):
        matches = [os.path.join(data_dir, f) for f in os.listdir(data_dir)
                   if substr in f]
    return matches

def process_tiles(root_dir, img_substrings, qi_substrings, use_docker):
    tiles_dir = os.path.join(root_dir, "TILES")

    if not os.path.isdir(tiles_dir):
        print("Error: {} not found".format(tiles_dir))
        return

    for tile_name in os.listdir(tiles_dir):
        tile_path = os.path.join(tiles_dir, tile_name)
        if not os.path.isdir(tile_path):
            continue  # skip non-directories

        # --- Check IMG_DATA ---
        img_data = os.path.join(tile_path, "IMG_DATA")
        qi_data = os.path.join(tile_path, "QI_DATA")
        monodate_masks = get_files(qi_data, "MMONODFLG")
        if os.path.isdir(img_data):
            for substr in img_substrings:
                data_files = get_files(img_data, substr)
                if len(data_files) > 1 and len(data_files) == len(monodate_masks):
                    handle_img_data(img_data, data_files, monodate_masks, substr, tile_name, use_docker)
                else:
                    print("No duplicated image files found for {} and tile name {} or number of data files ({}) not equal with the number of mask files ({})".format(substr, tile_name, len(data_files), len(monodate_masks)))
        # --- Check QI_DATA ---
        if os.path.isdir(qi_data):
            for substr in qi_substrings:
                qi_files = get_files(qi_data, substr)
                if len(qi_files) > 1:
                    handle_qi_data(qi_data, qi_files, [], substr, tile_name, use_docker)
                else:
                    print("No duplicated mask files found for {} and tile name {}".format(substr, tile_name))

# Get configurations from environment variables
def main():

    parser = argparse.ArgumentParser(
        description="Check the L3B product and perform composite of indices and masks from same day "
    )
    parser.add_argument("-i", "--input", help="Input L3B product path or text file containing the L3B product paths", required=True)
    parser.add_argument("--use-docker", help="Use docker to execute the command", action="store_true")

    args = parser.parse_args()

    img_substrings = ["SNDVI", "SLAIMONO", "SFAPARMONO", "SFCOVERMONO"]
    # img_substrings = ["SNDVI"]
    # qi_substrings = ["MMONODFLG", "MLAIDOMFLG", "MINDOMFLG"]
    qi_substrings = ["MMONODFLG"]

    if os.path.isdir(args.input):
        print ("Processing input product {}".format(args.input))
        process_tiles(args.input, img_substrings, qi_substrings, args.use_docker)
    else :
        with open(args.input, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                print ("Processing input product {}".format(line))
                process_tiles(line, img_substrings, qi_substrings, args.use_docker)

if __name__ == "__main__":
    main()