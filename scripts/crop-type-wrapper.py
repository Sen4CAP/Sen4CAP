#!/usr/bin/env python
from __future__ import print_function

import argparse
import os
import os.path
import pipes
import subprocess
import sys


def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))

    print(cmd_line)
    result = subprocess.call(args, env=env)
    if result != 0:
        raise RuntimeError("Command `{}` failed with exit code {}".format(cmd_line, result))


def check_file(p):
    if not os.path.exists(p):
        return False
    with open(p) as f:
        if f.readline() and f.readline():
            return True
    return False


def main():
    parser = argparse.ArgumentParser(description="Crop type processor wrapper")
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument(
        "-m",
        "--mode",
        help="processing mode",
        choices=["s1-only", "s2-only", "both"],
        default="both",
    )
    parser.add_argument("--season-start", help="season start date")
    parser.add_argument("--season-end", help="season end date")
    parser.add_argument("--out-path", default=".", help="output path")
    parser.add_argument("--working-path", default=".", help="working path")
    parser.add_argument("--tiles", nargs="+", help="tile filter")
    parser.add_argument("--products", nargs="+", help="product filter")
    parser.add_argument("--lc", help="LC classes to assess", default="1234")
    parser.add_argument(
        "--min-s2-pix", type=int, help="minimum number of S2 pixels", default=3
    )
    parser.add_argument(
        "--min-s1-pix", type=int, help="minimum number of S1 pixels", default=1
    )
    parser.add_argument(
        "--best-s2-pix",
        type=int,
        help="minimum number of S2 pixels for parcels to use in training",
        default=10,
    )
    parser.add_argument(
        "--pa-min", type=int, help="minimum parcels to assess a crop type", default=30
    )
    parser.add_argument(
        "--pa-train-h",
        type=int,
        help="upper threshold for parcel counts by crop type",
        default=4000,
    )
    parser.add_argument(
        "--pa-train-l",
        type=int,
        help="lower threshold for parcel counts by crop type",
        default=1333,
    )
    parser.add_argument(
        "--sample-ratio-h",
        type=float,
        help="training ratio for common crop types",
        default=0.25,
    )
    parser.add_argument(
        "--sample-ratio-l",
        type=float,
        help="training ratio for uncommon crop types",
        default=0.75,
    )
    parser.add_argument(
        "--smote-target", type=int, help="target sample count for SMOTE", default=1000
    )
    parser.add_argument(
        "--smote-k", type=int, help="number of SMOTE neighbours", default=5
    )
    parser.add_argument("--num-trees", type=int, help="number of RF trees", default=300)
    parser.add_argument(
        "--min-node-size", type=int, help="minimum node size", default=10
    )
    parser.add_argument(
        "--standalone", action="store_true", help="standalone mode"
    )
    parser.add_argument(
        "--parcels", required=False, help="parcels file"
    )
    parser.add_argument("--lut", required=False, help="LUT file")
    parser.add_argument("--tile-footprints", required=False, help="tile footprints")
    parser.add_argument("--optical-products", required=False, help="optical products")
    parser.add_argument("--radar-products", required=False, help="radar products")
    parser.add_argument("--lpis-path", required=False, help="LPIS path file")
    args = parser.parse_args()

    current_path = os.getcwd()
    os.chdir(args.working_path)
    if args.mode != "s1-only":
        try:
            os.mkdir("optical")
        except OSError:
            pass
    if args.mode != "s2-only":
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

    if args.standalone:
        parcels = os.path.join(args.working_path, "parcels.csv")
        lut = os.path.join(args.working_path, "lut.csv")
        tile_footprints = os.path.join(args.working_path, "tile-footprints.csv")
        optical_products = os.path.join(args.working_path, "optical-products.csv")
        radar_products = os.path.join(args.working_path, "radar-products.csv")
        lpis_path = os.path.join(args.working_path, "lpis.txt")

        command = []
        command += ["extract-parcels.py"]
        command += ["-s", args.site_id]
        command += ["--season-start", args.season_start]
        command += ["--season-end", args.season_end]
        command += [parcels, lut, tile_footprints, optical_products, radar_products, lpis_path]

        run_command(command)
    else:
        if not args.parcels or not args.lut or not args.tile_footprints or not args.optical_products or not args.radar_products or not args.lpis_path:
            print("The input files are required when not using `--standalone`")
            sys.exit(1)

        parcels = os.path.abspath(args.parcels)
        lut = os.path.abspath(args.lut)
        tile_footprints = os.path.abspath(args.tile_footprints)
        optical_products = os.path.abspath(args.optical_products)
        radar_products = os.path.abspath(args.radar_products)
        lpis_path = os.path.abspath(args.lpis_path)

    with open(lpis_path, "rt") as f:
        lpis_path = f.readline().strip()

    if args.mode != "s1-only":
        os.chdir("optical")

        command = []
        command += ["crop-type-parcels.py"]
        command += ["-m", "optical"]
        command += ["-s", args.site_id]
        command += ["--lpis-path", lpis_path]
        command += ["--optical-products", optical_products]
        if args.mode != "both":
            command += ["--no-re"]

        run_command(command)
        os.chdir("..")

    if args.mode != "s2-only":
        os.chdir("sar")
        command = []
        command += ["crop-type-parcels.py"]
        command += ["-m", "sar"]
        command += ["-s", args.site_id]
        command += ["--lpis-path", lpis_path]
        command += ["--tile-footprints", tile_footprints]
        command += ["--radar-products", radar_products]

        run_command(command)

        os.chdir("..")
        command = []
        command += ["merge-sar.py"]
        command += ["sar", "sar-merged"]

        run_command(command)

    if args.mode != "s1-only":
        os.rename("optical/optical-features.csv", "features/optical-features.csv")
    if args.mode == "both":
        os.rename("optical/optical-features-re.csv", "features/optical-features-re.csv")
    if args.mode != "s2-only":
        os.rename("sar-merged/sar-features.csv", "features/sar-features.csv")
        os.rename("sar-merged/sar-temporal.csv", "features/sar-temporal.csv")

    os.chdir(current_path)

    optical_features = os.path.join(args.working_path, "features/optical-features.csv")
    optical_re_features = os.path.join(
        args.working_path, "features/optical-features-re.csv"
    )
    sar_features = os.path.join(args.working_path, "features/sar-features.csv")
    sar_temporal = os.path.join(args.working_path, "features/sar-temporal.csv")

    if args.mode == "s1-only" or not check_file(optical_features):
        optical_features = "0"
    if args.mode != "both" or not check_file(optical_re_features):
        optical_re_features = "0"
    if args.mode == "s2-only" or not check_file(sar_features):
        sar_features = "0"
    if args.mode == "s2-only" or not check_file(sar_temporal):
        sar_temporal = "0"

    if args.mode == "s2-only":
        args.min_s1_pix = 0
    elif args.mode == "s1-only":
        args.min_s2_pix = 0

    command = []
    command += ["crop_type.R"]
    command += [args.out_path + "/"]
    command += [sar_features]
    command += [optical_features]
    command += [optical_re_features]
    command += [sar_temporal]
    command += [parcels]
    command += ["CTnumL4A"]
    command += [args.lc]
    command += ["Area_meters"]
    command += [args.min_s2_pix]
    command += [args.min_s1_pix]
    command += [args.pa_min]
    command += [args.best_s2_pix]
    command += [args.pa_train_h]
    command += [args.pa_train_l]
    command += [args.sample_ratio_h]
    command += [args.sample_ratio_l]
    command += ["Smote"]
    command += [args.smote_target]
    command += [args.smote_k]
    command += [args.num_trees]
    command += [args.min_node_size]
    command += [lut]

    run_command(command)


if __name__ == "__main__":
    main()
