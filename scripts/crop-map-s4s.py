#!/usr/bin/env python
from __future__ import print_function

import argparse
from datetime import datetime, timedelta
import glob
import json
import logging
from lxml import etree
from lxml.builder import E
import multiprocessing.dummy
import os
import os.path
from osgeo import gdal
import pipes
import psycopg2
from psycopg2.sql import SQL
import psycopg2.extras
import psycopg2.extensions
import subprocess
import docker
import shutil

from configparser import ConfigParser


OTB_IMAGE_NAME = "sen4x/otb:7.4.0"
INTERPOLATION_IMAGE_NAME = "sen4x/interpolation:0.1.0"
GDAL_IMAGE_NAME = "osgeo/gdal:ubuntu-full-3.4.1"
MISC_IMAGE_NAME = "sen4x/s4s-interim-ct:latest"


class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")

        # work around Docker networking scheme
        if self.host == "127.0.0.1" or self.host == "::1" or self.host == "localhost":
            self.host = "172.17.0.1"

        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        self.site_id = args.site_id


def get_connection(config):
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


def run_command(args, env=None, retry=False):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)

    retries = 5 if retry else 1
    while retries > 0:
        retries -= 1
        print(cmd_line)
        if env:
            result = subprocess.call(args, env=env)
        else:
            result = subprocess.call(args)
        if result != 0:
            print("Exit code: {}".format(result))
        else:
            break


def parse_date(str):
    return datetime.strptime(str, "%Y-%m-%d").date()


def get_season_dates(start_date, end_date):
    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=10)
    return dates


class FeatureSet(object):
    def __init__(self):
        self.s2_reflectance_10m = True
        self.s2_reflectance_20m = True
        self.vegetation_indices = True
        self.vegetation_indices_statistics = True

        self.s1_features = True

    def want_s2_reflectance_10m(self):
        return self.s2_reflectance_10m

    def want_s2_reflectance_20m(self):
        return self.s2_reflectance_20m

    def want_vegetation_indices(self):
        return self.vegetation_indices

    def want_vegetation_indices_statistics(self):
        return self.vegetation_indices_statistics

    def want_s1_features(self):
        return self.s1_features

    def need_s2_reflectance_10m(self):
        return self.s2_reflectance_10m or self.need_vegetation_indices()

    def need_s2_reflectance_20m(self):
        return self.s2_reflectance_20m or self.need_vegetation_indices()

    def need_vegetation_indices(self):
        return self.vegetation_indices or self.vegetation_indices_statistics

    def need_vegetation_indices_statistics(self):
        return self.vegetation_indices_statistics

    def need_s1_features(self):
        return self.s1_features

    @staticmethod
    def parse(features):
        feature_set = FeatureSet()
        if features:
            feature_set.s2_reflectance_10m = "sr10" in features
            feature_set.s2_reflectance_10m = "sr20" in features
            feature_set.vegetation_indices = "vi" in features
            feature_set.vegetation_indices_statistics = "vis" in features
            feature_set.s1_features = "sar" in features
        return feature_set


class L2AProduct(object):
    def __init__(
        self, date, b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12, mask_10m, mask_20m
    ):
        self.date = date
        self.b2 = b2
        self.b3 = b3
        self.b4 = b4
        self.b5 = b5
        self.b6 = b6
        self.b7 = b7
        self.b8 = b8
        self.b8a = b8a
        self.b11 = b11
        self.b12 = b12
        self.mask_10m = mask_10m
        self.mask_20m = mask_20m


class ProcessorConfig:
    def __init__(self, additional_mounts, max_depth, min_samples, num_trees):
        self.additional_mounts = additional_mounts
        self.max_depth = max_depth
        self.min_samples = min_samples
        self.num_trees = num_trees


def load_processor_config(conn, site_id):
    query = SQL(
        """
with site_config as (
    select key,
           value
    from v_site_config
    where site_id = %s
),
     config as (
         select (
                    select value as additional_mounts
                    from site_config
                    where key = 'general.orchestrator.docker_add_mounts'
                ),
                (
                    select value :: int as rf_max_depth
                    from site_config
                    where key = 'processor.s4s_crop_mapping.rf.max-depth'
                ),
                (
                    select value :: int as rf_min_samples
                    from site_config
                    where key = 'processor.s4s_crop_mapping.rf.min-samples'
                ),
                (
                    select value :: int as rf_num_trees
                    from site_config
                    where key = 'processor.s4s_crop_mapping.rf.num-trees'
                )
     )
select *
from config;
"""
    )
    logging.debug(query.as_string(conn))
    with conn.cursor() as cursor:
        cursor.execute(query, (site_id,))
        (additional_mounts, max_depth, min_samples, num_trees) = cursor.fetchone()
        if additional_mounts:
            additional_mounts = list(
                map(
                    lambda p: (p[0], p[1]),
                    map(lambda x: x.split(":"), additional_mounts.split(",")),
                )
            )
        else:
            additional_mounts = []

        print(additional_mounts)

        return ProcessorConfig(additional_mounts, max_depth, min_samples, num_trees)


def load_tiles(conn, site_id, tile_filter):
    query = SQL(
        "select unnest(tiles) from site_tiles where site_id = %s and satellite_id = 1"
    )
    logging.debug(query.as_string(conn))
    tiles = []
    with conn.cursor() as cursor:
        cursor.execute(query, (site_id,))

        for (tile_id,) in cursor:
            if not tile_filter or tile_id in tile_filter:
                tiles.append(tile_id)
    return tiles


def get_maja_band_files(path):
    files = glob.glob(os.path.join(path, "*_FRE_*.tif"))
    b2 = next((p for p in files if p.endswith("_FRE_B2.tif")), None)
    b3 = next((p for p in files if p.endswith("_FRE_B3.tif")), None)
    b4 = next((p for p in files if p.endswith("_FRE_B4.tif")), None)
    b5 = next((p for p in files if p.endswith("_FRE_B5.tif")), None)
    b6 = next((p for p in files if p.endswith("_FRE_B6.tif")), None)
    b7 = next((p for p in files if p.endswith("_FRE_B7.tif")), None)
    b8 = next((p for p in files if p.endswith("_FRE_B8.tif")), None)
    b8a = next((p for p in files if p.endswith("_FRE_B8A.tif")), None)
    b11 = next((p for p in files if p.endswith("_FRE_B11.tif")), None)
    b12 = next((p for p in files if p.endswith("_FRE_B12.tif")), None)
    return (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12)


def get_sen2cor_band_files(path):
    files_10m = glob.glob(os.path.join(path, "R10m/*_B*_10m.jp2"))
    files_20m = glob.glob(os.path.join(path, "R20m/*_B*_20m.jp2"))
    b2 = next((p for p in files_10m if p.endswith("_B02_10m.jp2")), None)
    b3 = next((p for p in files_10m if p.endswith("_B03_10m.jp2")), None)
    b4 = next((p for p in files_10m if p.endswith("_B04_10m.jp2")), None)
    b8 = next((p for p in files_10m if p.endswith("_B08_10m.jp2")), None)
    b5 = next((p for p in files_20m if p.endswith("_B05_20m.jp2")), None)
    b6 = next((p for p in files_20m if p.endswith("_B06_20m.jp2")), None)
    b7 = next((p for p in files_20m if p.endswith("_B07_20m.jp2")), None)
    b8a = next((p for p in files_20m if p.endswith("_B8A_20m.jp2")), None)
    b11 = next((p for p in files_20m if p.endswith("_B11_20m.jp2")), None)
    b12 = next((p for p in files_20m if p.endswith("_B12_20m.jp2")), None)
    return (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12)


def get_band_files(l2a_path):
    product_dir = glob.glob(os.path.join(l2a_path, "GRANULE/L2A*/IMG_DATA"))
    if product_dir:
        return get_sen2cor_band_files(product_dir[0])
    product_dir = glob.glob(os.path.join(l2a_path, "SENTINEL2*"))
    if product_dir:
        return get_maja_band_files(product_dir[0])
    return None


def get_product(name, l2a_path, created_timestamp, mask_path):
    mask_name = os.path.basename(mask_path)
    mask_name_10m = mask_name.replace(".SAFE", "_10M.tif")
    mask_name_20m = mask_name.replace(".SAFE", "_20M.tif")
    mask_10m = os.path.join(
        mask_path,
        mask_name_10m,
    )
    mask_20m = os.path.join(
        mask_path,
        mask_name_20m,
    )

    (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12) = get_band_files(l2a_path)

    if (
        not b2
        or not b3
        or not b4
        or not b5
        or not b6
        or not b7
        or not b8
        or not b8a
        or not b11
        or not b12
    ):
        return None

    product = L2AProduct(
        created_timestamp,
        b2,
        b3,
        b4,
        b5,
        b6,
        b7,
        b8,
        b8a,
        b11,
        b12,
        mask_10m,
        mask_20m,
    )
    return product


def load_products(conn, pool, site_id, season_start, season_end, tiles):
    products_by_tile = {}
    for tile in tiles:
        query = SQL(
            """
select product_l2a.name,
       product_l2a.full_path as l2a_path,
       product_l2a.created_timestamp :: date,
       product_validity_mask.full_path as mask_path
from product product_validity_mask
         inner join product_provenance on product_provenance.product_id = product_validity_mask.id
         inner join product product_l2a
                    on product_l2a.id = product_provenance.parent_product_id and product_l2a.product_type_id = 1
where product_validity_mask.site_id = %s
  and product_validity_mask.product_type_id = 26
  and product_validity_mask.created_timestamp >= %s
  and product_validity_mask.created_timestamp < %s + interval '1 day'
  and %s :: character varying = any(product_l2a.tiles)
order by product_l2a.created_timestamp;
"""
        )
        logging.debug(query.as_string(conn))

        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (
                    site_id,
                    season_start,
                    season_end,
                    tile,
                ),
            )
            result = cursor.fetchall()

            products = [p for p in pool.map(lambda r: get_product(*r), result) if p]
            products = sorted(products, key=lambda p: p.date)
            products_by_tile[tile] = products

    return products_by_tile


def get_band_names(feature_set, output_dates, s1_features):
    band_names = []
    if feature_set.want_s2_reflectance_10m():
        for b in [
            "S2_B03",
            "S2_B04",
            "S2_B08",
        ]:
            for d in output_dates:
                band_names.append(f"{b}_{d}")
    if feature_set.want_s2_reflectance_20m():
        for b in [
            "S2_B05",
            "S2_B06",
            "S2_B07",
            "S2_B11",
            "S2_B12",
        ]:
            for d in output_dates:
                band_names.append(f"{b}_{d}")
    if feature_set.want_vegetation_indices():
        for b in [
            "NDVI",
            "NDWI",
            "BRIGHTNESS",
        ]:
            for d in output_dates:
                band_names.append(f"{b}_{d}")
    if feature_set.want_vegetation_indices_statistics():
        for indicator in ["NDVI", "NDWI", "BRIGHTNESS"]:
            for statistic in ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"]:
                band_name = f"{indicator}_{statistic}"
                band_names.append(band_name)
    band_names += s1_features
    return band_names


def main():
    parser = argparse.ArgumentParser(
        description="Run S4S/Crop Type feature extraction and classification"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )

    required_args = parser.add_argument_group("required named arguments")
    required_args.add_argument(
        "-s", "--site-id", type=int, required=True, help="site ID to filter by"
    )
    parser.add_argument("--season-start", help="season start date")
    parser.add_argument("--season-end", help="season end date")
    parser.add_argument("--remapping-set-id", help="remapping set id", type=int)
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    parser.add_argument(
        "--keep-polygons",
        help="keep training and validation polygons",
        action="store_true",
    )
    parser.add_argument("--working-path", help="working path")
    parser.add_argument("--output-path", help="output path")
    parser.add_argument("--mounts", help="paths to mount in containers", nargs="*")
    parser.add_argument("--tiles", help="tile filter", nargs="*")
    parser.add_argument("--features", help="feature filter", nargs="*")

    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level)

    season_start = parse_date(args.season_start)
    season_end = parse_date(args.season_end)

    training_files = []
    validation_files = []

    if args.working_path:
        os.chdir(args.working_path)
    output_dir = os.path.abspath(".")

    client = docker.from_env()
    pool = multiprocessing.dummy.Pool()

    config = Config(args)

    with get_connection(config) as conn:
        processor_config = load_processor_config(conn, config.site_id)

    volumes = {
        "/mnt/archive": {"bind": "/mnt/archive", "mode": "rw"},
        "/etc/sen2agri/sen2agri.conf": {
            "bind": "/etc/sen2agri/sen2agri.conf",
            "mode": "ro",
        },
        output_dir: {"bind": output_dir, "mode": "rw"},
    }
    for (s, t) in processor_config.additional_mounts:
        volumes[s] = {"bind": t, "mode": "ro"}
    if args.mounts:
        for mount in args.mounts:
            volumes[mount] = {"bind": mount, "mode": "ro"}

    extra_mounts = ["/mnt/archive"]
    for (s, t) in processor_config.additional_mounts:
        extra_mounts.append(s)
    if args.mounts:
        extra_mounts += args.mounts

    if not args.keep_polygons:
        command = [
            "sample-selection.py",
            "-s",
            str(args.site_id),
            "--year",
            str(season_start.year),  # TODO
        ]
        if args.tiles:
            command += ["--tiles"] + args.tiles
        if args.remapping_set_id:
            command += ["--remapping-set-id", str(args.remapping_set_id)]
        if args.debug:
            command += ["--debug"]
        if extra_mounts:
            command += ["--mounts"] + extra_mounts
        run_command(command)

    command = [
        "extract-parcels2.py",
        "-s",
        str(config.site_id),
        "--season-start",
        args.season_start,
        "--season-end",
        args.season_end,
    ]

    command += [
        "parcels.csv",
        "lut.csv",
        "tiles.csv",
        "optical.csv",
        "radar.csv",
        "lpis.txt",
    ]

    container = client.containers.run(
        image=MISC_IMAGE_NAME,
        detach=True,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        working_dir=output_dir,
        command=command,
    )
    res = container.wait()
    if res["StatusCode"] != 0:
        print(container.logs())
    container.remove()

    feature_set = FeatureSet.parse(args.features)

    if feature_set.need_s1_features():
        command = [
            "crop-type-parcels2.py",
            "-s",
            str(config.site_id),
            "-m",
            "sar",
            "--radar-compositing-weeks",
            "2",
            "--tile-footprints",
            "tiles.csv",
            "--radar-products",
            "radar.csv",
        ]
        if extra_mounts:
            command += ["--mounts"] + extra_mounts

        run_command(command)

    with get_connection(config) as conn:
        tiles = load_tiles(conn, config.site_id, args.tiles)
        products_by_tile = load_products(
            conn, pool, config.site_id, season_start, season_end, tiles
        )

        first_date = season_end
        last_date = season_start
        for (tile, products) in products_by_tile.items():
            for p in products:
                if p.date < first_date:
                    first_date = p.date
                if p.date > last_date:
                    last_date = p.date

        step = 10
        output_dates = []
        d = first_date
        last_output_date = d
        while d <= last_date:
            last_output_date = d
            output_dates.append(d.strftime("%Y_%m_%d"))
            d += timedelta(days=step)
        last_date = last_output_date

        if feature_set.need_s1_features():
            s1_features_by_tile = {}
            for tile in products_by_tile.keys():
                s1_vrt = f"S1_{tile}.vrt"
                features = []
                if os.path.exists(s1_vrt):
                    ds = gdal.Open(s1_vrt, gdal.gdalconst.GA_ReadOnly)
                    for b in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(b)
                        band_name = band.GetDescription()
                        features.append(band_name)
                s1_features_by_tile[tile] = features
            s1_feature_list = list(map(set, s1_features_by_tile.values()))
            if s1_feature_list:
                s1_features = set.intersection(*s1_feature_list)
            else:
                s1_features = set()
        else:
            s1_features = set()

        band_names = get_band_names(feature_set, output_dates, s1_features)
        for (tile, products) in products_by_tile.items():
            b3s = [p.b3 for p in products]
            b4s = [p.b4 for p in products]
            b8s = [p.b8 for p in products]
            b5s = [p.b5 for p in products]
            b6s = [p.b6 for p in products]
            b7s = [p.b7 for p in products]
            b11s = [p.b11 for p in products]
            b12s = [p.b12 for p in products]

            days = [(p.date - season_start).days for p in products]
            days_string = "#".join(map(str, days))
            begin = first_date - season_start
            end = last_date - season_start
            interval_string = "{}#{}#{}".format(begin.days, step, end.days)

            masks_10m = [p.mask_10m for p in products]
            mask_10m_vrt = f"mask_10m_{tile}.vrt"
            command_mask_10m_vrt = [
                "gdalbuildvrt",
                "-separate",
                mask_10m_vrt,
            ] + masks_10m

            masks_20m = [p.mask_20m for p in products]
            mask_20m_vrt = f"mask_20m_{tile}.vrt"
            command_mask_20m_vrt = [
                "gdalbuildvrt",
                "-separate",
                mask_20m_vrt,
            ] + masks_20m

            b3_vrt = f"S2_B03_{tile}.vrt"
            b4_vrt = f"S2_B04_{tile}.vrt"
            b8_vrt = f"S2_B08_{tile}.vrt"
            b5_vrt = f"S2_B05_{tile}.vrt"
            b6_vrt = f"S2_B06_{tile}.vrt"
            b7_vrt = f"S2_B07_{tile}.vrt"
            b11_vrt = f"S2_B11_{tile}.vrt"
            b12_vrt = f"S2_B12_{tile}.vrt"

            command_b3_vrt = ["gdalbuildvrt", "-separate", b3_vrt] + b3s
            command_b4_vrt = ["gdalbuildvrt", "-separate", b4_vrt] + b4s
            command_b8_vrt = ["gdalbuildvrt", "-separate", b8_vrt] + b8s

            command_b5_vrt = ["gdalbuildvrt", "-separate", b5_vrt] + b5s
            command_b6_vrt = ["gdalbuildvrt", "-separate", b6_vrt] + b6s
            command_b7_vrt = ["gdalbuildvrt", "-separate", b7_vrt] + b7s
            command_b11_vrt = ["gdalbuildvrt", "-separate", b11_vrt] + b11s
            command_b12_vrt = ["gdalbuildvrt", "-separate", b12_vrt] + b12s

            if feature_set.need_s2_reflectance_10m():
                commands_10m = []
                if not os.path.exists(mask_10m_vrt):
                    commands_10m.append(command_mask_10m_vrt)
                if not os.path.exists(b3_vrt):
                    commands_10m.append(command_b3_vrt)
                if not os.path.exists(b4_vrt):
                    commands_10m.append(command_b4_vrt)
                if not os.path.exists(b8_vrt):
                    commands_10m.append(command_b8_vrt)
                pool.map(run_command, commands_10m)

            if feature_set.need_s2_reflectance_20m():
                commands_20m = []
                if not os.path.exists(mask_20m_vrt):
                    commands_20m.append(command_mask_20m_vrt)
                if not os.path.exists(b5_vrt):
                    commands_20m.append(command_b5_vrt)
                if not os.path.exists(b6_vrt):
                    commands_20m.append(command_b6_vrt)
                if not os.path.exists(b7_vrt):
                    commands_20m.append(command_b7_vrt)
                if not os.path.exists(b11_vrt):
                    commands_20m.append(command_b11_vrt)
                if not os.path.exists(b12_vrt):
                    commands_20m.append(command_b12_vrt)
                pool.map(run_command, commands_20m)

            b3_tif = f"S2_B03_{tile}.tif"
            b4_tif = f"S2_B04_{tile}.tif"
            b8_tif = f"S2_B08_{tile}.tif"
            b5_tif = f"S2_B05_{tile}.tif"
            b6_tif = f"S2_B06_{tile}.tif"
            b7_tif = f"S2_B07_{tile}.tif"
            b11_tif = f"S2_B11_{tile}.tif"
            b12_tif = f"S2_B12_{tile}.tif"

            interpolation_no_data = -10000
            interpolation_max_distance = 30
            interpolation_window_radius = 15

            tiling_suffix = "?&gdal:co:TILED=YES&gdal:co:BLOCKXSIZE=1024&gdal:co:BLOCKYSIZE=1024&streaming:type=tiled&streaming:sizemode=height&streaming:sizevalue=1024"
            commands = []
            if feature_set.need_s2_reflectance_10m() and not os.path.exists(b3_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b3_vrt,
                    mask_10m_vrt,
                    b3_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_10m() and not os.path.exists(b4_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b4_vrt,
                    mask_10m_vrt,
                    b4_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_10m() and not os.path.exists(b8_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b8_vrt,
                    mask_10m_vrt,
                    b8_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_20m() and not os.path.exists(b5_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b5_vrt,
                    mask_20m_vrt,
                    b5_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_20m() and not os.path.exists(b6_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b6_vrt,
                    mask_20m_vrt,
                    b6_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_20m() and not os.path.exists(b7_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b7_vrt,
                    mask_20m_vrt,
                    b7_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_20m() and not os.path.exists(b11_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b11_vrt,
                    mask_20m_vrt,
                    b11_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)
            if feature_set.need_s2_reflectance_20m() and not os.path.exists(b12_tif):
                command = [
                    "constant_step_interpolation_masked",
                    b12_vrt,
                    mask_20m_vrt,
                    b12_tif + tiling_suffix,
                    days_string,
                    interval_string,
                    "0",
                    str(interpolation_no_data),
                    str(interpolation_max_distance),
                    str(interpolation_window_radius),
                ]
                commands.append(command)

            containers = []
            for command in commands:
                container = client.containers.run(
                    image=INTERPOLATION_IMAGE_NAME,
                    detach=True,
                    user=f"{os.getuid()}:{os.getgid()}",
                    volumes=volumes,
                    working_dir=output_dir,
                    command=command,
                )
                containers.append(container)
            for container in containers:
                res = container.wait()
                if res["StatusCode"] != 0:
                    print(container.logs())
                container.remove()

            b5_10m_vrt = f"b5_10m_{tile}.vrt"
            b6_10m_vrt = f"b6_10m_{tile}.vrt"
            b7_10m_vrt = f"b7_10m_{tile}.vrt"
            b11_10m_vrt = f"b11_10m_{tile}.vrt"
            b12_10m_vrt = f"b12_10m_{tile}.vrt"

            if feature_set.need_s2_reflectance_20m():
                command_b5_10m_vrt = [
                    "gdal_translate",
                    "-tr",
                    "10",
                    "10",
                    "-r",
                    "cubic",
                    b5_tif,
                    b5_10m_vrt,
                ]
                command_b6_10m_vrt = [
                    "gdal_translate",
                    "-tr",
                    "10",
                    "10",
                    "-r",
                    "cubic",
                    b6_tif,
                    b6_10m_vrt,
                ]
                command_b7_10m_vrt = [
                    "gdal_translate",
                    "-tr",
                    "10",
                    "10",
                    "-r",
                    "cubic",
                    b7_tif,
                    b7_10m_vrt,
                ]
                command_b11_10m_vrt = [
                    "gdal_translate",
                    "-tr",
                    "10",
                    "10",
                    "-r",
                    "cubic",
                    b11_tif,
                    b11_10m_vrt,
                ]
                command_b12_10m_vrt = [
                    "gdal_translate",
                    "-tr",
                    "10",
                    "10",
                    "-r",
                    "cubic",
                    b12_tif,
                    b12_10m_vrt,
                ]

                commands = [
                    command_b5_10m_vrt,
                    command_b6_10m_vrt,
                    command_b7_10m_vrt,
                    command_b11_10m_vrt,
                    command_b12_10m_vrt,
                ]

                containers = []
                for command in commands:
                    container = client.containers.run(
                        image=GDAL_IMAGE_NAME,
                        detach=True,
                        user=f"{os.getuid()}:{os.getgid()}",
                        volumes=volumes,
                        working_dir=output_dir,
                        command=command,
                    )
                    print(command)
                    containers.append(container)
                for container in containers:
                    res = container.wait()
                    if res["StatusCode"] != 0:
                        print(container.logs())
                    container.remove()

            ndvi = f"S2_NDVI_{tile}.tif"
            ndwi = f"S2_NDWI_{tile}.tif"
            brightness = f"S2_BRIGHTNESS_{tile}.tif"

            if feature_set.need_vegetation_indices() and (
                not os.path.exists(ndvi)
                or not os.path.exists(ndwi)
                or not os.path.exists(brightness)
            ):
                command = [
                    "otbcli",
                    "S4SCMSpectralIndices",
                    "-bv",
                    "-10000",
                    "-b3",
                    b3_tif,
                    "-b4",
                    b4_tif,
                    "-b8",
                    b8_tif,
                    "-b11",
                    b11_10m_vrt,
                    "-outndvi",
                    ndvi,
                    "-outndwi",
                    ndwi,
                    "-outbrightness",
                    brightness,
                ]

                container = client.containers.run(
                    image=INTERPOLATION_IMAGE_NAME,
                    detach=True,
                    user=f"{os.getuid()}:{os.getgid()}",
                    volumes=volumes,
                    working_dir=output_dir,
                    command=command,
                )
                print(command)
                res = container.wait()
                if res["StatusCode"] != 0:
                    print(container.logs())
                container.remove()

            if feature_set.need_vegetation_indices_statistics():
                ndvi_statistics = f"S2_NDVI_STATISTICS_{tile}.tif"
                ndwi_statistics = f"S2_NDWI_STATISTICS_{tile}.tif"
                brightness_statistics = f"S2_BRIGHTNESS_STATISTICS_{tile}.tif"

                commands = []
                if not os.path.exists(ndvi_statistics):
                    command = [
                        "otbcli",
                        "S4SCMSpectralIndicesStatistics",
                        "-in",
                        ndvi,
                        "-out",
                        ndvi_statistics,
                    ]
                    commands.append(command)
                if not os.path.exists(ndwi_statistics):
                    command = [
                        "otbcli",
                        "S4SCMSpectralIndicesStatistics",
                        "-in",
                        ndwi,
                        "-out",
                        ndwi_statistics,
                    ]
                    commands.append(command)
                if not os.path.exists(brightness_statistics):
                    command = [
                        "otbcli",
                        "S4SCMSpectralIndicesStatistics",
                        "-in",
                        brightness,
                        "-out",
                        brightness_statistics,
                    ]
                    commands.append(command)

                containers = []
                for command in commands:
                    container = client.containers.run(
                        image=INTERPOLATION_IMAGE_NAME,
                        detach=True,
                        user=f"{os.getuid()}:{os.getgid()}",
                        volumes=volumes,
                        working_dir=output_dir,
                        command=command,
                    )
                    print(command)
                    containers.append(container)
                for container in containers:
                    res = container.wait()
                    if res["StatusCode"] != 0:
                        print(container.logs())
                    container.remove()

            ds = gdal.Open(b3_tif, gdal.gdalconst.GA_ReadOnly)
            gt = ds.GetGeoTransform()
            band = ds.GetRasterBand(1)
            block_size = band.GetBlockSize()
            vrt_dataset = E.VRTDataset(
                {
                    "rasterXSize": str(ds.RasterXSize),
                    "rasterYSize": str(ds.RasterYSize),
                },
                E.SRS({"dataAxisToSRSAxisMapping": "1,2"}, ds.GetProjectionRef()),
                E.GeoTransform(
                    "{}, {}, {}, {}, {}, {}".format(
                        gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]
                    )
                ),
                E.BlockXSize(str(block_size[0])),
                E.BlockYSize(str(block_size[1])),
            )
            out_band = 1
            if feature_set.want_s2_reflectance_10m():
                for (p, name) in zip(
                    [b3_tif, b4_tif, b8_tif],
                    ["S2_B03", "S2_B04", "S2_B08"],
                ):
                    ds = gdal.Open(p, gdal.gdalconst.GA_ReadOnly)
                    for b in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(b)
                        block_size = band.GetBlockSize()
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": "Int16",
                                "band": str(out_band),
                                "blockXSize": str(block_size[0]),
                                "blockYSize": str(block_size[1]),
                            },
                            E.Description(name),
                            E.SimpleSource(
                                E.SourceFileName({"relativeToVRT": "1"}, p),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(ds.RasterXSize),
                                        "RasterYSize": str(ds.RasterYSize),
                                        "DataType": "Int16",
                                        "BlockXSize": str(block_size[0]),
                                        "BlockYSize": str(block_size[1]),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            if feature_set.want_s2_reflectance_20m():
                for (p, name) in zip(
                    [b5_10m_vrt, b6_10m_vrt, b7_10m_vrt, b11_10m_vrt, b12_10m_vrt],
                    ["S2_B05", "S2_B06", "S2_B07", "S2_B11", "S2_B12"],
                ):
                    ds = gdal.Open(p, gdal.gdalconst.GA_ReadOnly)
                    for b in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(b)
                        block_size = band.GetBlockSize()
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": "Int16",
                                "band": str(out_band),
                                "blockXSize": str(block_size[0] * 2),
                                "blockYSize": str(block_size[1]),
                            },
                            E.Description(name),
                            E.SimpleSource(
                                {"resampling": "cubic"},
                                E.SourceFileName({"relativeToVRT": "1"}, p),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(ds.RasterXSize),
                                        "RasterYSize": str(ds.RasterYSize),
                                        "DataType": "Int16",
                                        "BlockXSize": str(block_size[0]),
                                        "BlockYSize": str(block_size[1]),
                                    }
                                ),
                                E.SrcRect(
                                    {
                                        "xOff": "0",
                                        "yOff": "0",
                                        "xSize": str(ds.RasterXSize),
                                        "ySize": str(ds.RasterYSize),
                                    }
                                ),
                                E.DstRect(
                                    {
                                        "xOff": "0",
                                        "yOff": "0",
                                        "xSize": str(ds.RasterXSize * 2),
                                        "ySize": str(ds.RasterYSize * 2),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            if feature_set.want_vegetation_indices():
                for (p, name) in zip(
                    [ndvi, ndwi, brightness],
                    ["NDVI", "NDWI", "BRIGHTNESS"],
                ):
                    ds = gdal.Open(p, gdal.gdalconst.GA_ReadOnly)
                    for b in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(b)
                        block_size = band.GetBlockSize()
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": "Int16",
                                "band": str(out_band),
                                "blockXSize": str(block_size[0]),
                                "blockYSize": str(block_size[1]),
                            },
                            E.Description(name),
                            E.SimpleSource(
                                E.SourceFileName({"relativeToVRT": "1"}, p),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(ds.RasterXSize),
                                        "RasterYSize": str(ds.RasterYSize),
                                        "DataType": "Int16",
                                        "BlockXSize": str(block_size[0]),
                                        "BlockYSize": str(block_size[1]),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            if feature_set.want_vegetation_indices_statistics():
                for (p, fname) in zip(
                    [ndvi_statistics, ndwi_statistics, brightness_statistics],
                    ["NDVI", "NDWI", "BRIGHTNESS"],
                ):
                    ds = gdal.Open(p, gdal.gdalconst.GA_ReadOnly)
                    for (b, name) in enumerate(
                        ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"], start=1
                    ):
                        band = ds.GetRasterBand(b)
                        band_name = f"{fname}_{name}"
                        block_size = band.GetBlockSize()
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": "Int16",
                                "band": str(out_band),
                                "blockXSize": str(block_size[0]),
                                "blockYSize": str(block_size[1]),
                            },
                            E.Description(band_name),
                            E.SimpleSource(
                                E.SourceFileName({"relativeToVRT": "1"}, p),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(ds.RasterXSize),
                                        "RasterYSize": str(ds.RasterYSize),
                                        "DataType": "Int16",
                                        "BlockXSize": str(block_size[0]),
                                        "BlockYSize": str(block_size[1]),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            if feature_set.want_s1_features():
                s1_vrt = f"S1_{tile}.vrt"
                ds = gdal.Open(s1_vrt, gdal.gdalconst.GA_ReadOnly)
                for b in range(1, ds.RasterCount + 1):
                    band = ds.GetRasterBand(b)
                    data_type = gdal.GetDataTypeName(band.DataType)
                    block_size = band.GetBlockSize()
                    band_name = band.GetDescription()
                    if band_name not in s1_features:
                        print(f"Dropping feature {band_name}")
                        continue
                    vrt_raster_band = E.VRTRasterBand(
                        {
                            "dataType": data_type,
                            "band": str(out_band),
                            "blockXSize": str(block_size[0]),
                            "blockYSize": str(block_size[1]),
                        },
                        E.Description(band_name),
                        E.SimpleSource(
                            E.SourceFileName({"relativeToVRT": "1"}, s1_vrt),
                            E.SourceBand(str(b)),
                            E.SourceProperties(
                                {
                                    "RasterXSize": str(ds.RasterXSize),
                                    "RasterYSize": str(ds.RasterYSize),
                                    "DataType": data_type,
                                    "BlockXSize": str(block_size[0]),
                                    "BlockYSize": str(block_size[1]),
                                }
                            ),
                        ),
                    )
                    vrt_dataset.append(vrt_raster_band)
                    out_band += 1

            root = etree.ElementTree(vrt_dataset)
            bands_vrt = f"bands_{tile}.vrt"
            root.write(bands_vrt, pretty_print=True, encoding="utf-8")

        band_names_lower = list(map(lambda x: x.lower(), band_names))
        for (tile, products) in products_by_tile.items():
            training_points = f"training_points_{tile}.shp"
            validation_points = f"validation_points_{tile}.shp"
            training_samples = f"training_samples_{tile}.sqlite"
            validation_samples = f"validation_samples_{tile}.sqlite"

            bands_vrt = f"bands_{tile}.vrt"

            commands = []
            if not os.path.exists(training_samples) and os.path.exists(training_points):
                command = [
                    "otbcli_SampleExtraction",
                    "-in",
                    bands_vrt,
                    "-vec",
                    training_points,
                    "-out",
                    training_samples,
                    "-field",
                    "crop_code",
                    "-outfield",
                    "list",
                    "-outfield.list.names",
                ] + band_names_lower
                commands.append(command)

            if not os.path.exists(validation_samples) and os.path.exists(
                validation_points
            ):
                command = [
                    "otbcli_SampleExtraction",
                    "-in",
                    bands_vrt,
                    "-vec",
                    validation_points,
                    "-out",
                    validation_samples,
                    "-field",
                    "crop_code",
                    "-outfield",
                    "list",
                    "-outfield.list.names",
                ] + band_names_lower
                commands.append(command)

            containers = []
            for command in commands:
                container = client.containers.run(
                    image=OTB_IMAGE_NAME,
                    detach=True,
                    user=f"{os.getuid()}:{os.getgid()}",
                    volumes=volumes,
                    working_dir=output_dir,
                    command=command,
                )
                containers.append(container)
            for container in containers:
                res = container.wait()
                if res["StatusCode"] != 0:
                    print(container.logs())
                container.remove()

            if os.path.exists(training_samples):
                training_files.append(training_samples)
            if os.path.exists(validation_samples):
                validation_files.append(validation_samples)

    training_samples = "training_samples.vrt"
    validation_samples = "validation_samples.vrt"
    command_merge_training_samples = [
        "ogrmerge.py",
        "-overwrite_ds",
        "-single",
        "-o",
        training_samples,
    ] + training_files
    command_merge_validation_samples = [
        "ogrmerge.py",
        "-overwrite_ds",
        "-single",
        "-o",
        validation_samples,
    ] + validation_files
    pool.map(
        run_command,
        [command_merge_training_samples, command_merge_validation_samples],
    )

    commands = []
    with open("smote-targets.json", "rt", encoding="utf-8") as file:
        smote_targets = json.load(file)

    smote_outputs = []
    for (crop_code, target) in smote_targets.items():
        output = f"smote_{crop_code}.sqlite"
        command = [
            "otbcli_SampleAugmentation",
            "-in",
            training_samples,
            "-out",
            output,
            "-label",
            crop_code,
            "-samples",
            str(target),
            "-field",
            "crop_code",
            "-exclude",
            "id",
            "originfid",
            "-strategy",
            "smote",
        ]
        commands.append(command)
        smote_outputs.append(output)

    containers = []
    for command in commands:
        container = client.containers.run(
            image=OTB_IMAGE_NAME,
            detach=True,
            user=f"{os.getuid()}:{os.getgid()}",
            volumes=volumes,
            working_dir=output_dir,
            command=command,
        )
        containers.append(container)
    for container in containers:
        res = container.wait()
        if res["StatusCode"] != 0:
            print(container.logs())
        container.remove()

    training_samples_agumented = "training_samples_agumented.vrt"
    command = [
        "ogrmerge.py",
        "-overwrite_ds",
        "-single",
        "-o",
        training_samples_agumented,
        training_samples,
    ] + smote_outputs
    run_command(command)

    model = "model.yaml"
    confusion_matrix = "confusion_matrix.txt"
    command = [
        "otbcli_TrainVectorClassifier",
        "-io.vd",
        training_samples_agumented,
        "-valid.vd",
        validation_samples,
        "-io.out",
        model,
        "-io.confmatout",
        confusion_matrix,
        "-cfield",
        "crop_code",
        "-classifier",
        "rf",
        "-classifier.rf.max",
        str(processor_config.max_depth),
        "-classifier.rf.min",
        str(processor_config.min_samples),
        "-classifier.rf.nbtrees",
        str(processor_config.num_trees),
        # "-classifier.rf.ra",
        # "0",
        # "-classifier.rf.cat",
        # "10",
        # "-classifier.rf.var",
        # "0",
        # "-classifier.rf.acc",
        # "0.01",
        "-feat",
    ] + band_names_lower
    print(" ".join(command))

    client.containers.run(
        image=OTB_IMAGE_NAME,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        working_dir=output_dir,
        command=command,
    )

    with open(confusion_matrix, "rt", encoding="utf-8") as file:
        line = file.readline()
        num_classes = line.count(",") + 1

    remapping_table_name = "remapping-table.csv"
    if os.path.exists(remapping_table_name):
        remapping_table = remapping_table_name
    else:
        remapping_table = None

    commands = []
    for tile in tiles:
        bands_vrt = f"bands_{tile}.vrt"
        if remapping_table:
            classified_pre_tif = f"classified_pre_{tile}.tif"
        else:
            classified_pre_tif = f"classified_{tile}.tif"

        confidence_map_tif = f"confidence_map_{tile}.tif"
        probability_map_tif = f"probability_map_{tile}.tif"
        command = [
            "otbcli_ImageClassifier",
            "-in",
            bands_vrt,
            "-out",
            classified_pre_tif,
            "int16",
            "-model",
            model,
            "-confmap",
            confidence_map_tif,
            "-probamap",
            probability_map_tif,
            "-nbclasses",
            str(num_classes),
        ]
        commands.append(command)
    containers = []
    for command in commands:
        container = client.containers.run(
            image=OTB_IMAGE_NAME,
            detach=True,
            user=f"{os.getuid()}:{os.getgid()}",
            volumes=volumes,
            working_dir=output_dir,
            command=command,
        )
        containers.append(container)
    for container in containers:
        res = container.wait()
        if res["StatusCode"] != 0:
            print(container.logs())
        container.remove()

    if remapping_table:
        commands = []
        for tile in tiles:
            classified_pre_tif = f"classified_pre_{tile}.tif"
            classified_tif = f"classified_{tile}.tif"
            command = [
                "otbcli",
                "ClassRemapping",
                "-table",
                remapping_table,
                "-in",
                classified_pre_tif,
                "-out",
                classified_tif,
            ]
            commands.append(command)
        containers = []
        for command in commands:
            container = client.containers.run(
                image=INTERPOLATION_IMAGE_NAME,
                detach=True,
                user=f"{os.getuid()}:{os.getgid()}",
                volumes=volumes,
                working_dir=output_dir,
                command=command,
            )
            containers.append(container)
        for container in containers:
            res = container.wait()
            if res["StatusCode"] != 0:
                print(container.logs())
            container.remove()

    if args.output_path:
        for tile in tiles:
            classified_tif = f"classified_{tile}.tif"
            confidence_map_tif = f"confidence_map_{tile}.tif"
            probability_map_tif = f"probability_map_{tile}.tif"
            shutil.copy2(classified_tif, args.output_path)
            shutil.copy2(confidence_map_tif, args.output_path)
            # shutil.copy2(probability_map_tif, args.output_path)

            if remapping_table:
                classified_pre_tif = f"classified_pre_{tile}.tif"
                shutil.copy2(classified_pre_tif, args.output_path)

        shutil.copy2(confusion_matrix, args.output_path)


if __name__ == "__main__":
    main()
