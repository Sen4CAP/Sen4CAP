#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
from datetime import datetime
import dateutil.parser
import errno
import os
import os.path
import pipes
import shutil
import subprocess
import sys


def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))

    print(cmd_line)
    result = subprocess.call(args, env=env)
    if result != 0:
        raise RuntimeError(
            "Command `{}` failed with exit code {}".format(cmd_line, result)
        )


def main():
    parser = argparse.ArgumentParser(description="Crop type processor wrapper")
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument("--working-path", default=".", help="working path")
    parser.add_argument(
        "--backscatter-compositing-months",
        default=2,
        type=int,
        help="backscatter compositing period in months",
    )
    parser.add_argument("--tile-footprints", required=False, help="tile footprints")
    parser.add_argument("--radar-products", required=False, help="radar products")
    parser.add_argument("--lpis-path", required=False, help="LPIS path file")
    args = parser.parse_args()

    current_path = os.getcwd()
    os.chdir(args.working_path)
    try:
        os.mkdir("sar")
    except OSError:
        pass
    try:
        os.mkdir("sar-merged")
    except OSError:
        pass
    try:
        os.mkdir("features")
    except OSError:
        pass
        
    tile_footprints = os.path.abspath(args.tile_footprints)
    radar_products = os.path.abspath(args.radar_products)
    lpis_path = os.path.abspath(args.lpis_path)

    with open(lpis_path, "rt") as f:
        lpis_path = f.readline().strip()
        
    os.chdir("sar")
    command = []
    command += ["crop-type-parcels.py"]
    command += ["-m", "sar"]
    command += ["-s", args.site_id]
    command += ["--lpis-path", lpis_path]
    command += ["--tile-footprints", tile_footprints]
    command += ["--radar-products", radar_products]
    command += [
        "--backscatter-compositing-months",
        args.backscatter_compositing_months,
    ]
    
    command += ["--disable-coherence-monthly-composites"]
    command += ["--disable-coherence-season-composites"]

    run_command(command)

    os.chdir("..")
    # command = []
    # command += ["merge-sar.py"]
    # command += ["sar", "sar-merged"]
    # 
    # run_command(command)
        
        
if __name__ == "__main__":
    main()
        