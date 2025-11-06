#!/usr/bin/env python
from __future__ import print_function

import argparse
import dataclasses
import glob
import json
import logging
import os
import os.path
import pickle
import shlex
import shutil
import subprocess
from collections import defaultdict
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import IntEnum
from multiprocessing.dummy import Pool
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from lxml import etree
from lxml.builder import E
from osgeo import gdal, ogr, osr
from psycopg2.extensions import connection
from psycopg2.sql import SQL

import docker

OTB_NEW_IMAGE_NAME = "docker.io/orfeotoolbox/otb:8.1.1"
OTB_OLD_IMAGE_NAME = "docker.io/sen4x/otb:6.6.1"
PROCESSORS_NEW_IMAGE_NAME = "sen4x/processors-new:0.2.0"
MISC_IMAGE_NAME = "sen4x/s4s-interim-ct:latest"
ERDY_IMAGE_NAME = "docker.io/lnicola/erdy:0.2.4"


def parse_date(str):
    return datetime.strptime(str, "%Y-%m-%d").date()


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

        if args.date_filter:
            self.date_filter = list(map(parse_date, args.date_filter))
        else:
            self.date_filter = None
        self.stratum_filter = args.stratum_filter
        if args.stratum_start_dates:
            self.stratum_start_dates = list(map(parse_date, args.stratum_start_dates))
        else:
            self.stratum_start_dates = None
        if args.stratum_end_dates:
            self.stratum_end_dates = list(map(parse_date, args.stratum_end_dates))
        else:
            self.stratum_end_dates = None


def get_connection(config) -> connection:
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


def run_command(args, env=None, retry=False):
    args = list(map(str, args))
    cmd_line = " ".join(map(shlex.quote, args))
    print(cmd_line)

    retries = 5 if retry else 1
    while retries > 0:
        retries -= 1
        if env:
            result = subprocess.call(args, env=env)
        else:
            result = subprocess.call(args)
        if result != 0:
            print("Exit code: {}".format(result))
        else:
            break


def get_season_dates(start_date, end_date):
    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=10)
    return dates


class FeatureSet(object):
    def __init__(self):
        self.s2_b2 = False
        self.s2_b3 = True
        self.s2_b4 = True
        self.s2_b8 = True
        self.s2_b5 = True
        self.s2_b6 = True
        self.s2_b7 = True
        self.s2_b11 = True
        self.s2_b12 = True

        self.vegetation_indices = True
        self.vegetation_indices_statistics = True
        self.red_edge_features = False

        self.s1_features = True

    def want_s2_b2(self):
        return self.s2_b2

    def want_s2_b3(self):
        return self.s2_b3

    def want_s2_b4(self):
        return self.s2_b4

    def want_s2_b8(self):
        return self.s2_b8

    def want_s2_b5(self):
        return self.s2_b5

    def want_s2_b6(self):
        return self.s2_b6

    def want_s2_b7(self):
        return self.s2_b7

    def want_s2_b11(self):
        return self.s2_b11

    def want_s2_b12(self):
        return self.s2_b12

    def need_s2_b2(self):
        return self.s2_b2 or self.need_red_edge_features()

    def need_s2_b3(self):
        return self.s2_b3 or self.need_vegetation_indices()

    def need_s2_b4(self):
        return (
            self.s2_b4
            or self.need_vegetation_indices()
            or self.need_red_edge_features()
        )

    def need_s2_b8(self):
        return (
            self.s2_b8
            or self.need_vegetation_indices()
            or self.need_red_edge_features()
        )

    def need_s2_b5(self):
        return self.s2_b5 or self.need_red_edge_features()

    def need_s2_b6(self):
        return self.s2_b6 or self.need_red_edge_features()

    def need_s2_b7(self):
        return self.s2_b7 or self.need_red_edge_features()

    def need_s2_b11(self):
        return self.s2_b11 or self.need_vegetation_indices()

    def need_s2_b12(self):
        return self.s2_b12

    def want_vegetation_indices(self):
        return self.vegetation_indices

    def want_vegetation_indices_statistics(self):
        return self.vegetation_indices_statistics

    def want_red_edge_features(self):
        return self.red_edge_features

    def want_s1_features(self):
        return self.s1_features

    def need_vegetation_indices(self):
        return self.vegetation_indices or self.vegetation_indices_statistics

    def need_vegetation_indices_statistics(self):
        return self.vegetation_indices_statistics

    def need_red_edge_features(self):
        return self.red_edge_features

    def need_s1_features(self):
        return self.s1_features

    @staticmethod
    def empty():
        feature_set = FeatureSet()
        feature_set.s2_b2 = False
        feature_set.s2_b3 = False
        feature_set.s2_b4 = False
        feature_set.s2_b8 = False
        feature_set.s2_b5 = False
        feature_set.s2_b6 = False
        feature_set.s2_b7 = False
        feature_set.s2_b11 = False
        feature_set.s2_b12 = False

        feature_set.vegetation_indices = False
        feature_set.vegetation_indices_statistics = False
        feature_set.red_edge_features = False

        feature_set.s1_features = False

        return feature_set

    @staticmethod
    def parse(features):
        if not features:
            return FeatureSet()

        feature_set = FeatureSet.empty()

        for feature in features:
            if feature.startswith("-"):
                feature = feature[1:]
                value = False
            else:
                value = True

            if feature == "s2_b2":
                feature_set.s2_b2 = value
            elif feature == "s2_b3":
                feature_set.s2_b3 = value
            elif feature == "s2_b4":
                feature_set.s2_b4 = value
            elif feature == "s2_b8":
                feature_set.s2_b8 = value
            elif feature == "s2_b5":
                feature_set.s2_b5 = value
            elif feature == "s2_b6":
                feature_set.s2_b6 = value
            elif feature == "s2_b7":
                feature_set.s2_b7 = value
            elif feature == "s2_b11":
                feature_set.s2_b11 = value
            elif feature == "s2_b12":
                feature_set.s2_b12 = value
            elif feature == "sr10":
                feature_set.s2_b2 = value
                feature_set.s2_b3 = value
                feature_set.s2_b4 = value
                feature_set.s2_b8 = value
            elif feature == "sr20":
                feature_set.s2_b5 = value
                feature_set.s2_b6 = value
                feature_set.s2_b7 = value
                feature_set.s2_b11 = value
                feature_set.s2_b12 = value
            elif feature == "vi":
                feature_set.vegetation_indices = value
            elif feature == "vis":
                feature_set.vegetation_indices_statistics = value
            elif feature == "re":
                feature_set.red_edge_features = value
            elif feature == "sar":
                feature_set.s1_features = value

        return feature_set


class ContainerInfo:
    def __init__(
        self, image, command, working_dir, volumes, outputs=None, environment=None
    ):
        self.image = image
        self.command = command
        self.working_dir = working_dir
        self.volumes = volumes
        self.environment = environment
        self.outputs = outputs

    def run(self, client):
        try:
            container = client.containers.run(
                image=self.image,
                command=self.command,
                working_dir=self.working_dir,
                volumes=self.volumes,
                environment=self.environment,
                user=f"{os.getuid()}:{os.getgid()}",
                auto_remove=True,
                stderr=True,
                detach=True,
                tty=True,
            )
            status = container.wait()

            status_code = status["StatusCode"]
            command_string = " ".join(map(shlex.quote, self.command))
            if status_code != 0:
                print(f"Command {command_string} failed with status code {status_code}")
                if self.outputs:
                    for output in self.outputs:
                        try:
                            os.remove(output)
                        except FileNotFoundError:
                            pass
                        except Exception as exc:
                            print(f"Cannot remove {output}: {exc}")

            return status
        except Exception as exc:
            command_string = " ".join(map(shlex.quote, self.command))
            print(f"Cannot run {command_string}: {exc}")

            if self.outputs:
                for output in self.outputs:
                    os.remove(output)
            return None


def run_containers_concurrently(client, pool, containers):
    results = pool.map(lambda c: c.run(client), containers, chunksize=1)
    for res in results:
        if res is not None:
            if res["StatusCode"] != 0:
                print(res)


class L2AProduct(object):
    def __init__(
        self,
        date,
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
        band_offsets,
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
        self.band_offsets = band_offsets


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

        return ProcessorConfig(additional_mounts, max_depth, min_samples, num_trees)


def geotransform_for_tile(geom, epsg_code, pixel_size=10):
    xmin, xmax, ymin, ymax = geom.GetEnvelope()
    return [round(xmin), pixel_size, 0.0, round(ymax), 0.0, -pixel_size]


class Tile(object):
    def __init__(self, tile_id: str, epsg_code: int, geom: ogr.Geometry):
        self.tile_id = tile_id
        self.epsg_code = epsg_code
        self.geom = geom
        self.geo_transform = geotransform_for_tile(geom, epsg_code)


def load_tiles(
    conn: connection, site_id: int, tile_filter: Optional[List[str]]
) -> List[Tile]:
    query = SQL(
        """
with site_tiles as (
    select unnest(tiles) as tile_id
    from site_tiles
    where site_id = %s
      and satellite_id = 1
)
select shape_tiles_s2.tile_id,
       shape_tiles_s2.epsg_code,
       ST_AsBinary(ST_SnapToGrid(ST_Transform(geom, epsg_code), 1)) as geom
from site_tiles
     inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id
"""
    )
    logging.debug(query.as_string(conn))

    tiles = []
    srs_cache = {}
    with conn.cursor() as cursor:
        cursor.execute(query, (site_id,))

        for tile_id, epsg_code, geom in cursor:
            if not tile_filter or tile_id in tile_filter:
                srs = srs_cache.get(epsg_code)
                if not srs:
                    srs = osr.SpatialReference()
                    srs.ImportFromEPSG(epsg_code)
                    srs_cache[epsg_code] = srs

                geom = ogr.CreateGeometryFromWkb(geom)
                geom.AssignSpatialReference(srs)

                tile = Tile(tile_id, epsg_code, geom)
                tiles.append(tile)
    return tiles


class Sentinel2Band(IntEnum):
    B1 = 0
    B2 = 1
    B3 = 2
    B4 = 3
    B5 = 4
    B6 = 5
    B7 = 6
    B8 = 7
    B8A = 8
    B9 = 9
    B10 = 10
    B11 = 11
    B12 = 12


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
    band_offsets = {}
    return (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12, band_offsets)


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

    metadata = os.path.join(path, "../../../MTD_MSIL2A.xml")
    parser = etree.XMLPullParser(["start"])
    with open(metadata, "r") as file:
        parser.feed(file.read())
    band_offsets = {}
    for event, elem in parser.read_events():
        if elem.tag == "BOA_ADD_OFFSET":
            band_id = int(elem.attrib["band_id"])
            offset = int(elem.text)
            band_offsets[band_id] = offset
    return (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12, band_offsets)


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
    mask_name_10m = mask_name.replace(".SAFE", "_10M_BIN.tif")
    mask_name_20m = mask_name.replace(".SAFE", "_20M_BIN.tif")
    mask_10m = os.path.join(
        mask_path,
        mask_name_10m,
    )
    mask_20m = os.path.join(
        mask_path,
        mask_name_20m,
    )

    res = get_band_files(l2a_path)
    if not res:
        print("Missing files for", l2a_path)
        return None
    (b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12, band_offsets) = res

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
        band_offsets,
    )
    return product


def load_products(
    conn: connection, pool, site_id: int, season_start, season_end, tiles: List[Tile]
):
    products_by_tile: dict[str, List[L2AProduct]] = {}
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
order by product_l2a.created_timestamp, name;
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
                    tile.tile_id,
                ),
            )
            result = cursor.fetchall()

            products: List[L2AProduct] = [
                p for p in pool.map(lambda r: get_product(*r), result, chunksize=1) if p
            ]
            products = sorted(products, key=lambda p: p.date)
            products_by_tile[tile.tile_id] = products

    return products_by_tile


class Stratum(object):
    def __init__(
        self,
        stratum_id: Optional[int],
        geom,
        epsg_code: Optional[int],
        tiles: List[Tile],
    ) -> None:
        self.stratum_id = stratum_id
        self.geom = geom
        self.epsg_code = epsg_code
        self.tiles = tiles


def get_site_strata(
    conn: connection, config: Config, tiles: List[Tile]
) -> List[Stratum]:
    if config.stratum_filter:
        filter = set(config.stratum_filter)
    else:
        filter = None

    query = SQL("select * from sp_get_site_strata(%s)")
    logging.debug(query.as_string(conn))

    srs_cache = {}
    strata = []

    tile_dict = dict([(t.tile_id, t) for t in tiles])
    with conn.cursor() as cursor:
        cursor.execute(query, (config.site_id,))
        for stratum_id, geom, epsg_code, stratum_tiles in cursor:
            if filter and stratum_id not in filter:
                continue

            srs = srs_cache.get(epsg_code)
            if not srs:
                srs = osr.SpatialReference()
                srs.ImportFromEPSG(epsg_code)
                srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
                srs_cache[epsg_code] = srs

            geom = ogr.CreateGeometryFromWkb(geom)
            geom.AssignSpatialReference(srs)

            tiles_for_stratum = [tile_dict[t] for t in stratum_tiles]
            stratum = Stratum(stratum_id, geom, epsg_code, tiles_for_stratum)
            strata.append(stratum)

    return strata


def write_tile_vrts(
    strata: List[Stratum],
    feature_set: FeatureSet,
    s1_features: List[str],
    output_dates: List[date],
    stratum_date_filters: Optional[List[Tuple[date, date]]],
) -> List[str]:
    raster_size = 10980
    block_size = 256

    band_types = []

    if feature_set.want_s2_b2():
        band_types.append("S2_B02")
    if feature_set.want_s2_b3():
        band_types.append("S2_B03")
    if feature_set.want_s2_b4():
        band_types.append("S2_B04")
    if feature_set.want_s2_b8():
        band_types.append("S2_B08")

    if feature_set.want_s2_b5():
        band_types.append("S2_B05")
    if feature_set.want_s2_b6():
        band_types.append("S2_B06")
    if feature_set.want_s2_b7():
        band_types.append("S2_B07")
    if feature_set.want_s2_b11():
        band_types.append("S2_B11")
    if feature_set.want_s2_b12():
        band_types.append("S2_B12")

    if feature_set.want_vegetation_indices():
        band_types.append("NDVI")
        band_types.append("NDWI")
        band_types.append("BRIGHTNESS")

    if feature_set.want_red_edge_features():
        band_types.append("NDRE")
        band_types.append("REPI")
        band_types.append("PSRI")
        band_types.append("CIRE")

    if output_dates:
        season_start = output_dates[0]
        season_end = output_dates[-1]
        stratum_date_filters_opt = stratum_date_filters or [
            (season_start, season_end) for _ in strata
        ]
    else:
        stratum_date_filters_opt = stratum_date_filters or [None for _ in strata]

    stratum_band_names = []

    for stratum, stratum_date_filter in zip(strata, stratum_date_filters_opt):
        stratum_start_date = stratum_date_filter[0] if stratum_date_filter else None
        stratum_end_date = stratum_date_filter[1] if stratum_date_filter else None

        stratum_start_date_idx = None
        stratum_end_date_idx = None

        if stratum_start_date:
            for idx, d in enumerate(output_dates):
                if d >= stratum_start_date:
                    stratum_start_date_idx = idx
                    break
        if stratum_end_date:
            for idx, d in enumerate(reversed(output_dates)):
                if d <= stratum_end_date:
                    stratum_end_date_idx = len(output_dates) - idx
                    break

        print(
            f"Stratum {stratum.stratum_id}: start date {stratum_start_date}, end date {stratum_end_date}, start {stratum_start_date_idx}, end {stratum_end_date_idx}"
        )
        band_names = []

        if stratum_start_date_idx is not None and stratum_end_date_idx is not None:
            for name in band_types:
                for b, d in enumerate(
                    output_dates[stratum_start_date_idx:stratum_end_date_idx],
                    start=stratum_start_date_idx + 1,
                ):
                    dstr = d.strftime("%Y_%m_%d")
                    band_names.append(f"{name}_{dstr}")

            if feature_set.want_vegetation_indices_statistics():
                for fname in ["NDVI", "NDWI", "BRIGHTNESS"]:
                    for name in ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"]:
                        band_names.append(f"{fname}_{name}")

        if feature_set.want_s1_features():
            for name in s1_features:
                band_names.append(name)

        wkt_cache: Dict[int, str] = {}
        for tile in stratum.tiles:
            tile_id = tile.tile_id
            gt = tile.geo_transform

            b2_tif = f"S2_B02_{tile_id}.tif"
            b3_tif = f"S2_B03_{tile_id}.tif"
            b4_tif = f"S2_B04_{tile_id}.tif"
            b8_tif = f"S2_B08_{tile_id}.tif"

            b5_10m_vrt = f"S2_B05_10m_{tile_id}.vrt"
            b6_10m_vrt = f"S2_B06_10m_{tile_id}.vrt"
            b7_10m_vrt = f"S2_B07_10m_{tile_id}.vrt"
            b11_10m_vrt = f"S2_B11_10m_{tile_id}.vrt"
            b12_10m_vrt = f"S2_B12_10m_{tile_id}.vrt"

            ndvi = f"S2_NDVI_{tile_id}.tif"
            ndwi = f"S2_NDWI_{tile_id}.tif"
            brightness = f"S2_BRIGHTNESS_{tile_id}.tif"

            ndvi_statistics = f"S2_NDVI_STATISTICS_{tile_id}.tif"
            ndwi_statistics = f"S2_NDWI_STATISTICS_{tile_id}.tif"
            brightness_statistics = f"S2_BRIGHTNESS_STATISTICS_{tile_id}.tif"

            ndre = f"S2_NDRE_{tile_id}.tif"
            repi = f"S2_REPI_{tile_id}.tif"
            psri = f"S2_PSRI_{tile_id}.tif"
            cire = f"S2_CIRE_{tile_id}.tif"

            if tile.epsg_code not in wkt_cache:
                spatial_ref = osr.SpatialReference()
                spatial_ref.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
                spatial_ref.ImportFromEPSG(tile.epsg_code)
                wkt_cache[tile.epsg_code] = spatial_ref.ExportToWkt()
            wkt = wkt_cache[tile.epsg_code]
            vrt_dataset = E.VRTDataset(
                {
                    "rasterXSize": str(raster_size),
                    "rasterYSize": str(raster_size),
                },
                E.SRS({"dataAxisToSRSAxisMapping": "1,2"}, wkt),
                E.GeoTransform(
                    "{}, {}, {}, {}, {}, {}".format(
                        gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]
                    )
                ),
                E.BlockXSize(str(block_size)),
                E.BlockYSize(str(block_size)),
            )

            out_band = 1

            band_files = []

            if feature_set.want_s2_b2():
                band_files.append(b2_tif)
            if feature_set.want_s2_b3():
                band_files.append(b3_tif)
            if feature_set.want_s2_b4():
                band_files.append(b4_tif)
            if feature_set.want_s2_b8():
                band_files.append(b8_tif)

            if feature_set.want_s2_b5():
                band_files.append(b5_10m_vrt)
            if feature_set.want_s2_b6():
                band_files.append(b6_10m_vrt)
            if feature_set.want_s2_b7():
                band_files.append(b7_10m_vrt)
            if feature_set.want_s2_b11():
                band_files.append(b11_10m_vrt)
            if feature_set.want_s2_b12():
                band_files.append(b12_10m_vrt)

            if feature_set.want_vegetation_indices():
                band_files.append(ndvi)
                band_files.append(ndwi)
                band_files.append(brightness)

            if feature_set.want_red_edge_features():
                band_files.append(ndre)
                band_files.append(repi)
                band_files.append(psri)
                band_files.append(cire)

            for p, name in zip(
                band_files,
                band_types,
            ):
                if stratum_start_date_idx is None or stratum_end_date_idx is None:
                    continue

                for b, d in enumerate(
                    output_dates[stratum_start_date_idx:stratum_end_date_idx],
                    start=stratum_start_date_idx + 1,
                ):
                    dstr = d.strftime("%Y_%m_%d")
                    description = f"{name}_{dstr}"
                    assert band_names[out_band - 1] == description
                    vrt_raster_band = E.VRTRasterBand(
                        {
                            "dataType": "Int16",
                            "band": str(out_band),
                            "blockXSize": str(block_size),
                            "blockYSize": str(block_size),
                        },
                        E.Description(description),
                        E.SimpleSource(
                            E.SourceFilename({"relativeToVRT": "1"}, p),
                            E.SourceBand(str(b)),
                            E.SourceProperties(
                                {
                                    "RasterXSize": str(raster_size),
                                    "RasterYSize": str(raster_size),
                                    "DataType": "Int16",
                                    "BlockXSize": str(block_size),
                                    "BlockYSize": str(block_size),
                                }
                            ),
                        ),
                    )
                    vrt_dataset.append(vrt_raster_band)
                    out_band += 1

            if (
                stratum_start_date_idx is not None
                and stratum_end_date_idx is not None
                and feature_set.want_vegetation_indices_statistics()
            ):
                for p, fname in zip(
                    [ndvi_statistics, ndwi_statistics, brightness_statistics],
                    ["NDVI", "NDWI", "BRIGHTNESS"],
                ):
                    for b, name in enumerate(
                        ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"], start=1
                    ):
                        description = f"{fname}_{name}"
                        assert band_names[out_band - 1] == description
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": "Int16",
                                "band": str(out_band),
                                "blockXSize": str(block_size),
                                "blockYSize": str(block_size),
                            },
                            E.Description(description),
                            E.SimpleSource(
                                E.SourceFilename({"relativeToVRT": "1"}, p),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(raster_size),
                                        "RasterYSize": str(raster_size),
                                        "DataType": "Int16",
                                        "BlockXSize": str(block_size),
                                        "BlockYSize": str(block_size),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            if feature_set.want_s1_features():
                s1_vrt = f"S1_{tile_id}.vrt"
                ds = gdal.Open(s1_vrt, gdal.gdalconst.GA_ReadOnly)
                if ds:
                    for b in range(1, ds.RasterCount + 1):
                        band = ds.GetRasterBand(b)
                        data_type = gdal.GetDataTypeName(band.DataType)
                        band_name = band.GetDescription()
                        if band_name not in s1_features:
                            print(f"Dropping feature {band_name}")
                            continue
                        vrt_raster_band = E.VRTRasterBand(
                            {
                                "dataType": data_type,
                                "band": str(out_band),
                                "blockXSize": str(block_size),
                                "blockYSize": str(block_size),
                            },
                            E.Description(band_name),
                            E.SimpleSource(
                                E.SourceFilename({"relativeToVRT": "1"}, s1_vrt),
                                E.SourceBand(str(b)),
                                E.SourceProperties(
                                    {
                                        "RasterXSize": str(raster_size),
                                        "RasterYSize": str(raster_size),
                                        "DataType": data_type,
                                        "BlockXSize": str(block_size),
                                        "BlockYSize": str(block_size),
                                    }
                                ),
                            ),
                        )
                        vrt_dataset.append(vrt_raster_band)
                        out_band += 1

            root = etree.ElementTree(vrt_dataset)
            if stratum.stratum_id:
                bands_vrt = f"bands_{stratum.stratum_id}_{tile_id}.vrt"
            else:
                bands_vrt = f"bands_{tile_id}.vrt"
            root.write(bands_vrt, pretty_print=True, encoding="utf-8")
        stratum_band_names.append(band_names)

    return stratum_band_names


def run_sample_extraction(
    client,
    pool,
    output_dir: str,
    volumes: Dict[str, Dict[str, str]],
    env: Dict[str, str],
    tiles: List[Tile],
    strata: List[Stratum],
    stratum_band_names: List[str],
):
    commands = []
    for stratum, band_names in zip(strata, stratum_band_names):
        band_names_lower = list(map(lambda x: x.lower(), band_names))
        print(f"Stratum {stratum.stratum_id}, fields: {band_names_lower}")

        for tile in tiles:
            tile_id = tile.tile_id
            if stratum.stratum_id:
                bands_vrt = f"bands_{stratum.stratum_id}_{tile_id}.vrt"
                training_points = f"training_points_{stratum.stratum_id}_{tile_id}.gpkg"
                validation_points = (
                    f"validation_points_{stratum.stratum_id}_{tile_id}.gpkg"
                )

                training_samples = (
                    f"training_samples_{stratum.stratum_id}_{tile_id}.sqlite"
                )
                validation_samples = (
                    f"validation_samples_{stratum.stratum_id}_{tile_id}.sqlite"
                )
            else:
                bands_vrt = f"bands_{tile_id}.vrt"
                training_points = f"training_points_{tile_id}.gpkg"
                validation_points = f"validation_points_{tile_id}.gpkg"

                training_samples = f"training_samples_{tile_id}.sqlite"
                validation_samples = f"validation_samples_{tile_id}.sqlite"

            points = []
            outputs = []

            if os.path.exists(training_points) and not os.path.exists(training_samples):
                points.append(training_points)
                outputs.append(training_samples)

            if os.path.exists(validation_points) and not os.path.exists(
                validation_samples
            ):
                points.append(validation_points)
                outputs.append(validation_samples)

            if points:
                command = (
                    [
                        "erdy",
                        "sample-extraction",
                        "--num-threads",
                        "2",
                        bands_vrt,
                        "--points",
                    ]
                    + points
                    + ["--outputs"]
                    + outputs
                    + [
                        "--fields",
                    ]
                    + band_names_lower
                )
                commands.append(command)

    containers = []
    for command in commands:
        container = ContainerInfo(
            image=ERDY_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
            environment=env,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)

    training_map: dict[Optional[int], list[str]] = {}
    validation_map: dict[Optional[int], list[str]] = {}
    for stratum in strata:
        training_files = []
        validation_files = []

        for tile in tiles:
            tile_id = tile.tile_id
            if stratum.stratum_id:
                training_samples = (
                    f"training_samples_{stratum.stratum_id}_{tile_id}.sqlite"
                )
                validation_samples = (
                    f"validation_samples_{stratum.stratum_id}_{tile_id}.sqlite"
                )
            else:
                training_samples = f"training_samples_{tile_id}.sqlite"
                validation_samples = f"validation_samples_{tile_id}.sqlite"

            if os.path.exists(training_samples):
                training_files.append(training_samples)
            if os.path.exists(validation_samples):
                validation_files.append(validation_samples)

        training_map[stratum.stratum_id] = training_files
        validation_map[stratum.stratum_id] = validation_files

    return (training_map, validation_map)


def run_sample_augmentation(
    client,
    pool,
    output_dir: str,
    volumes: Dict[str, Dict[str, str]],
    env: Dict[str, str],
    strata: List[Stratum],
    training_map: Dict[Optional[int], List[str]],
):
    commands = []
    stratum_smote_outputs: list[list[str]] = []
    training_map_augmented: dict[Optional[int], list[str]] = {}
    for stratum in strata:
        if stratum.stratum_id:
            smote_targets_json = f"smote_targets_{stratum.stratum_id}.json"
        else:
            smote_targets_json = "smote_targets.json"

        with open(smote_targets_json, "rt", encoding="utf-8") as file:
            smote_targets: Dict[str, int] = json.load(file)

        training_files = training_map[stratum.stratum_id]
        training_map_augmented[stratum.stratum_id] = training_files.copy()

        smote_outputs = []
        for crop_code, target in smote_targets.items():
            if stratum.stratum_id:
                output = f"smote_{stratum.stratum_id}_{crop_code}.sqlite"
            else:
                output = f"smote_{crop_code}.sqlite"

            if not os.path.exists(output):
                command = [
                    "erdy",
                    "sample-augmentation",
                    output,
                    "--label",
                    crop_code,
                    "--samples",
                    str(target),
                    "--field",
                    "crop_code",
                    "--exclude",
                    "id",
                    "code_n1",
                    "code_n2",
                    "code_n3",
                    "code_n4",
                    "code_lc",
                    "pix_10m",
                    "strategy",
                    "originfid",
                    "--inputs",
                ] + training_files
                commands.append(command)
            smote_outputs.append(output)
            training_map_augmented[stratum.stratum_id].append(output)

        stratum_smote_outputs.append(smote_outputs)

    containers = []
    for command in commands:
        container = ContainerInfo(
            image=ERDY_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)

    return training_map_augmented


def run_training(
    client,
    pool,
    output_dir: str,
    volumes: Dict[str, Dict[str, str]],
    env: Dict[str, str],
    processor_config: ProcessorConfig,
    strata: List[Stratum],
    stratum_band_names: List[str],
    training_map_augmented: Dict[Optional[int], List[str]],
    validation_map: Dict[Optional[int], List[str]],
    use_old_otb: bool,
) -> List[str]:
    remapping_table = "remapping-table.csv"
    if not os.path.exists(remapping_table):
        remapping_table = None

    confusion_matrices = []
    for stratum, band_names in zip(strata, stratum_band_names):
        band_names_lower = list(map(lambda x: x.lower(), band_names))

        if stratum.stratum_id:
            model = f"model_{stratum.stratum_id}.yaml"

            if remapping_table:
                confusion_matrix_pre_json = (
                    f"confusion_matrix_pre_{stratum.stratum_id}.json"
                )
                confusion_matrix_json = f"confusion_matrix_{stratum.stratum_id}.json"
            else:
                confusion_matrix_pre_json = (
                    f"confusion_matrix_{stratum.stratum_id}.json"
                )
                confusion_matrix_json = None
        else:
            model = "model.yaml"

            if remapping_table:
                confusion_matrix_pre_json = "confusion_matrix_pre.json"
                confusion_matrix_json = "confusion_matrix.json"
            else:
                confusion_matrix_pre_json = "confusion_matrix.json"
                confusion_matrix_json = None

        confusion_matrices.append(confusion_matrix_pre_json)

        training_samples_augmented = training_map_augmented[stratum.stratum_id]
        validation_samples = validation_map[stratum.stratum_id]
        command = (
            [
                "otbcli_TrainVectorClassifier",
                "-io.out",
                model,
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
            ]
            + band_names_lower
            + ["-io.vd"]
        )
        for file in training_samples_augmented:
            if os.path.exists(file):
                command.append(file)

        if not os.path.exists(model):
            print(" ".join(command))
            container = ContainerInfo(
                image=OTB_OLD_IMAGE_NAME if use_old_otb else OTB_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                environment=env,
            )
            res = container.run(client)
            if res and res["StatusCode"] != 0:
                print(res)

        commands = []
        for validation_file in validation_samples:
            predictions = validation_file.replace("validation_samples_", "predictions_")

            if os.path.exists(predictions):
                continue

            command = [
                "otbcli_VectorClassifier",
                "-model",
                model,
                "-out",
                predictions,
                "-in",
                validation_file,
                "-feat",
            ] + band_names_lower
            commands.append(command)

        containers = []
        for command in commands:
            container = ContainerInfo(
                image=OTB_OLD_IMAGE_NAME if use_old_otb else OTB_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                environment=env,
            )
            containers.append(container)
        run_containers_concurrently(client, pool, containers)

        command = [
            "erdy",
            "compute-confusion-matrix",
            "--output",
            confusion_matrix_pre_json,
            "--reference",
            "crop_code",
            "--prediction",
            "predicted",
            "--inputs",
        ]
        for validation_file in validation_samples:
            predictions = validation_file.replace("validation_samples_", "predictions_")
            if os.path.exists(predictions):
                command.append(predictions)

        container = ContainerInfo(
            image=ERDY_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
            environment=env,
        )
        res = container.run(client)
        if res and res["StatusCode"] != 0:
            print(res)

        if confusion_matrix_json:
            command = [
                "erdy",
                "remap-confusion-matrix",
                "--remapping-table",
                remapping_table,
                "--output",
                confusion_matrix_json,
                "--input",
                confusion_matrix_pre_json,
            ]
            container = ContainerInfo(
                image=ERDY_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                environment=env,
            )
            res = container.run(client)
            if res and res["StatusCode"] != 0:
                print(res)

            confusion_matrices.append(confusion_matrix_json)

    return confusion_matrices


def run_classification(
    client,
    pool,
    output_dir: str,
    volumes: Dict[str, Dict[str, str]],
    env: Dict[str, str],
    strata: List[Stratum],
    use_old_otb: bool,
):
    tiling_suffix = "?&gdal:co:TILED=YES&gdal:co:COMPRESS=DEFLATE&streaming:type=tiled&streaming:sizemode=height&streaming:sizevalue=256"

    commands = []
    for stratum in strata:
        if stratum.stratum_id:
            model = f"model_{stratum.stratum_id}.yaml"
        else:
            model = "model.yaml"

        if not os.path.exists(model):
            continue

        remapping_table_name = "remapping-table.csv"
        if os.path.exists(remapping_table_name):
            remapping_table = remapping_table_name
        else:
            remapping_table = None

        for tile in stratum.tiles:
            tile_id = tile.tile_id
            if stratum.stratum_id:
                bands_vrt = f"bands_{stratum.stratum_id}_{tile_id}.vrt"
                confidence_map_tif = (
                    f"confidence_map_{stratum.stratum_id}_{tile_id}.tif"
                )
                probability_map_tif = (
                    f"probability_map_{stratum.stratum_id}_{tile_id}.tif"
                )
                if remapping_table:
                    classified_pre_tif = (
                        f"classified_pre_{stratum.stratum_id}_{tile_id}.tif"
                    )
                else:
                    classified_pre_tif = (
                        f"classified_{stratum.stratum_id}_{tile_id}.tif"
                    )
            else:
                bands_vrt = f"bands_{tile_id}.vrt"
                confidence_map_tif = f"confidence_map_{tile_id}.tif"
                probability_map_tif = f"probability_map_{tile_id}.tif"
                if remapping_table:
                    classified_pre_tif = f"classified_pre_{tile_id}.tif"
                else:
                    classified_pre_tif = f"classified_{tile_id}.tif"

            if os.path.exists(bands_vrt) and (
                not os.path.exists(classified_pre_tif)
                or (
                    not os.path.exists(confidence_map_tif)
                    and not os.path.exists(probability_map_tif)
                )
            ):
                command = [
                    "otbcli_ImageClassifier",
                    "-in",
                    bands_vrt,
                    "-out",
                    classified_pre_tif + tiling_suffix,
                    "int16",
                    "-model",
                    model,
                    "-confmap",
                    confidence_map_tif + tiling_suffix,
                ]
                commands.append(command)

    containers = []
    for command in commands:
        container = ContainerInfo(
            image=OTB_OLD_IMAGE_NAME if use_old_otb else OTB_NEW_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
            environment=env,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)


def rasterize_stratum_masks(
    tiles: List[Tile], strata: List[Stratum], strata_for_tile: Dict[str, List[int]]
):
    target_epsg_codes = set()
    for tile in tiles:
        if len(strata_for_tile[tile.tile_id]) > 1:
            target_epsg_codes.add(tile.epsg_code)

    driver = ogr.GetDriverByName("GPKG")
    strata_ds = {}
    for epsg_code in target_epsg_codes:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg_code)
        srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        ds = driver.CreateDataSource(f"strata_{epsg_code}.gpkg")
        strata_layer = ds.CreateLayer(
            "strata",
            srs,
            ogr.wkbMultiPolygon,
        )
        id_field = ogr.FieldDefn("id", ogr.OFTInteger)
        strata_layer.CreateField(id_field)
        for stratum in strata:
            transform = osr.CoordinateTransformation(
                stratum.geom.GetSpatialReference(), srs
            )
            geom = stratum.geom.Clone()
            geom.Transform(transform)

            feature = ogr.Feature(strata_layer.GetLayerDefn())
            feature.SetField(0, stratum.stratum_id)
            feature.SetGeometry(geom)
            strata_layer.CreateFeature(feature)

        ds.SyncToDisk()
        ds = gdal.OpenEx(f"strata_{epsg_code}.gpkg", gdal.OF_VECTOR)
        strata_ds[epsg_code] = ds

    for tile in tiles:
        if len(strata_for_tile[tile.tile_id]) > 1:
            mask_filename = f"mask_{tile.tile_id}.tif"
            creation_options = [
                "COMPRESS=DEFLATE",
                "TILED=YES",
                "NUM_THREADS=ALL_CPUS",
            ]
            (min_x, max_x, min_y, max_y) = tile.geom.GetEnvelope()
            bounds = [min_x, min_y, max_x, max_y]
            srs = f"EPSG:{tile.epsg_code}"
            rasterize_options = gdal.RasterizeOptions(
                format="GTiff",
                outputType=gdal.GDT_Byte,
                creationOptions=creation_options,
                outputBounds=bounds,
                outputSRS=srs,
                width=10980,
                height=10980,
                xRes=10,
                yRes=10,
                noData=0,
                attribute="id",
            )
            gdal.Rasterize(
                mask_filename, strata_ds[tile.epsg_code], options=rasterize_options
            )


def merge_strata(
    client,
    pool,
    output_dir: str,
    volumes: Dict[str, Dict[str, str]],
    env: Dict[str, str],
    tiles: List[Tile],
    strata_for_tile: Dict[str, List[int]],
    remapping_enabled: bool,
):
    commands = []
    for tile in tiles:
        tile_id = tile.tile_id
        tile_strata = strata_for_tile[tile.tile_id]
        print(f"{tile_id}: {len(tile_strata)} strata")

        if len(tile_strata) > 1:
            mask_filename = f"mask_{tile.tile_id}.tif"
            ok = os.path.exists(mask_filename)

            inputs = []
            for stratum_id in tile_strata:
                if remapping_enabled:
                    classified_pre_tif = f"classified_pre_{stratum_id}_{tile_id}.tif"
                    output = f"classified_pre_{tile_id}.tif"
                else:
                    classified_pre_tif = f"classified_{stratum_id}_{tile_id}.tif"
                    output = f"classified_{tile_id}.tif"
                inputs.append(classified_pre_tif)
                ok = ok and os.path.exists(classified_pre_tif)

            if remapping_enabled:
                output = f"classified_pre_{tile_id}.tif"
            else:
                output = f"classified_{tile_id}.tif"

            if ok:
                command = (
                    [
                        "erdy",
                        "band-select",
                        "--format",
                        "GTiff",
                        mask_filename,
                        output,
                        "--inputs",
                    ]
                    + inputs
                    + ["--input-labels"]
                    + [str(id) for id in tile_strata]
                )
                commands.append(command)
        elif len(tile_strata) == 1:
            stratum_id = tile_strata[0]

            if remapping_enabled:
                classified_pre_tif = f"classified_pre_{stratum_id}_{tile_id}.tif"
                output = f"classified_pre_{tile_id}.tif"
            else:
                classified_pre_tif = f"classified_{stratum_id}_{tile_id}.tif"
                output = f"classified_{tile_id}.tif"

            if os.path.exists(classified_pre_tif) and not os.path.exists(output):
                shutil.copy2(classified_pre_tif, output)
        else:
            print(f"No stratum for tile {tile_id}")

    containers = []
    for command in commands:
        print(command)
        container = ContainerInfo(
            image=ERDY_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
            environment=env,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)


def dominant_year(start: date, end: date) -> int:
    midpoint = start + (end - start) / 2
    return midpoint.year


@dataclass
class TileInfo:
    raster_size: Tuple[int, int]
    geo_transform: Tuple[float, float, float, float, float, float]
    projection: str
    block_size: List[int]
    data_type: int

    @staticmethod
    def from_dataset(path):
        ds = gdal.Open(path)
        raster_size = (ds.RasterXSize, ds.RasterYSize)
        geo_transform = ds.GetGeoTransform()
        projection = ds.GetProjectionRef()
        band = ds.GetRasterBand(1)
        block_size = band.GetBlockSize()
        data_type = band.DataType
        return TileInfo(raster_size, geo_transform, projection, block_size, data_type)


def write_stack_vrt_fast(tile_info: TileInfo, files: List[str], destination: str):
    if not files:
        return

    data_type_name = gdal.GetDataTypeName(tile_info.data_type)

    vrt_dataset = E.VRTDataset(
        {
            "rasterXSize": str(tile_info.raster_size[0]),
            "rasterYSize": str(tile_info.raster_size[1]),
        },
        E.SRS({"dataAxisToSRSAxisMapping": "1,2"}, tile_info.projection),
        E.GeoTransform(
            "{}, {}, {}, {}, {}, {}".format(
                tile_info.geo_transform[0],
                tile_info.geo_transform[1],
                tile_info.geo_transform[2],
                tile_info.geo_transform[3],
                tile_info.geo_transform[4],
                tile_info.geo_transform[5],
            )
        ),
        E.BlockXSize(str(tile_info.block_size[0])),
        E.BlockYSize(str(tile_info.block_size[1])),
    )

    out_band = 1
    for file in files:
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": data_type_name,
                "band": str(out_band),
                "blockXSize": str(tile_info.block_size[0]),
                "blockYSize": str(tile_info.block_size[1]),
            },
            E.SimpleSource(
                E.SourceFilename({"relativeToVRT": "0"}, file),
                E.SourceBand("1"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(tile_info.raster_size[0]),
                        "RasterYSize": str(tile_info.raster_size[1]),
                        "DataType": data_type_name,
                        "BlockXSize": str(tile_info.block_size[0]),
                        "BlockYSize": str(tile_info.block_size[1]),
                    }
                ),
            ),
        )
        vrt_dataset.append(vrt_raster_band)
        out_band += 1

    root = etree.ElementTree(vrt_dataset)
    root.write(destination, pretty_print=True, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="Run S4S/Crop Type feature extraction and classification",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
    parser.add_argument("--year", help="in-situ data or classification year", type=int)
    parser.add_argument(
        "--pix-min", type=int, default=1, help="Minimum number of pixels of polygons"
    )
    parser.add_argument(
        "--pix-best",
        type=int,
        default=1,
        help="Minimum number of pixels of polygons used for training",
    )
    parser.add_argument(
        "--pix-ratio-min",
        type=float,
        default=0.0002,
        help="Minimum crop to total pixel ratio",
    )
    parser.add_argument(
        "--poly-min", type=int, default=1, help="Minimum number of polygons for crops"
    )
    parser.add_argument(
        "--pix-ratio-hi",
        type=float,
        default=0.05,
        help="Minimum crop to total pixel ratio for strategy 1",
    )
    parser.add_argument(
        "--pix-ratio-lo",
        type=float,
        default=0.01,
        help="Minimum crop to total pixel ratio for strategy 2",
    )
    parser.add_argument(
        "--smote-ratio", type=float, default=0.0075, help="Synthetic sample ratio"
    )
    parser.add_argument(
        "--sample-ratio-hi",
        type=float,
        default=0.25,
        help="Training pixel ratio for strategy 1",
    )
    parser.add_argument(
        "--sample-ratio-lo",
        type=float,
        default=0.75,
        help="Training pixel ratio for strategies 2 and 3",
    )
    parser.add_argument(
        "--monitored-land-covers",
        type=int,
        nargs="+",
        default=None,
        help="Land cover class filter",
    )
    parser.add_argument(
        "--monitored-crops", type=int, nargs="+", default=None, help="Crop class filter"
    )
    parser.add_argument(
        "--monitored-crops-remapped-pre",
        type=int,
        nargs="+",
        default=None,
        help="Pre-remapped monitored crops",
    )
    parser.add_argument(
        "--excluded-crops-remapped-pre",
        type=int,
        nargs="+",
        default=None,
        help="Pre-remapped excluded crops",
    )
    parser.add_argument("--remapping-set-id", help="remapping set id", type=int)
    parser.add_argument(
        "--broceliande", help="Broceliande mode", default=False, action="store_true"
    )
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
    parser.add_argument("--date-filter", help="date filter", nargs="*")
    parser.add_argument("--stratum-filter", help="stratum filter", type=int, nargs="*")
    parser.add_argument("--stratum-start-dates", help="stratum start dates", nargs="*")
    parser.add_argument("--stratum-end-dates", help="stratum end dates", nargs="*")
    parser.add_argument(
        "--use-old-otb",
        help="use old OTB, for > 2 GB models",
        default=False,
        action="store_true",
    )

    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level)

    season_start = parse_date(args.season_start)
    season_end = parse_date(args.season_end)
    classification_year = dominant_year(season_start, season_end)

    if args.working_path:
        os.chdir(args.working_path)
    output_dir = os.path.abspath(".")

    env = {
        "GDAL_MAX_DATASET_POOL_SIZE": "1000",
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        "GDAL_PAM_ENABLED": "NO",
    }
    for key, value in env.items():
        os.environ[key] = value

    client = docker.from_env(timeout=600)
    pool_hi_conc = Pool()
    pool_med_conc = Pool(min(os.cpu_count() or 1, 4))
    pool_no_conc = Pool(1)

    config = Config(args)
    if config.stratum_start_dates and config.stratum_end_dates:
        assert len(config.stratum_start_dates) == len(config.stratum_end_dates)

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
    for s, t in processor_config.additional_mounts:
        volumes[s] = {"bind": t, "mode": "ro"}
    if args.mounts:
        for mount in args.mounts:
            volumes[mount] = {"bind": mount, "mode": "ro"}

    extra_mounts = ["/mnt/archive"]
    for s, t in processor_config.additional_mounts:
        extra_mounts.append(s)
    if args.mounts:
        extra_mounts += args.mounts

    if not args.keep_polygons:
        command = [
            "sample-selection.py",
            "-s",
            str(args.site_id),
            "--year",
            str(classification_year),
            "--pix-min",
            str(args.pix_min),
            "--pix-best",
            str(args.pix_best),
            "--pix-ratio-min",
            str(args.pix_ratio_min),
            "--poly-min",
            str(args.poly_min),
            "--pix-ratio-hi",
            str(args.pix_ratio_hi),
            "--pix-ratio-lo",
            str(args.pix_ratio_lo),
            "--smote-ratio",
            str(args.smote_ratio),
            "--sample-ratio-hi",
            str(args.sample_ratio_hi),
            "--sample-ratio-lo",
            str(args.sample_ratio_lo),
        ]
        if args.monitored_land_covers:
            command += ["--monitored-land-covers"] + list(
                map(str, args.monitored_land_covers)
            )
        if args.monitored_crops:
            command += ["--monitored-crops"] + list(map(str, args.monitored_crops))
        if args.monitored_crops_remapped_pre:
            command += ["--monitored-crops-remapped-pre"] + list(
                map(str, args.monitored_crops_remapped_pre)
            )
        if args.excluded_crops_remapped_pre:
            command += ["--excluded-crops-remapped-pre"] + list(
                map(str, args.excluded_crops_remapped_pre)
            )
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

    container = ContainerInfo(
        image=MISC_IMAGE_NAME,
        command=command,
        working_dir=output_dir,
        volumes=volumes,
    )
    res = container.run(client)
    if res and res["StatusCode"] != 0:
        print(res)

    feature_set = FeatureSet.parse(args.features)
    if args.broceliande:
        feature_set = FeatureSet.parse(["s2_b4", "s2_b8"])

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

        strata = get_site_strata(conn, config, tiles)
        if not strata:
            stratum = Stratum(None, None, None, tiles)
            strata.append(stratum)

        if strata:
            tiles = list(set([t for s in strata for t in s.tiles]))

        tile_ids = set([t.tile_id for t in tiles])
        if os.path.exists("s2-products.pickle"):
            with open("s2-products.pickle", "rb") as file:
                products_by_tile: dict[str, List[L2AProduct]] = pickle.load(file)
                products_by_tile = dict(
                    [(t, p) for (t, p) in products_by_tile.items() if t in tile_ids]
                )
        else:
            products_by_tile = load_products(
                conn, pool_med_conc, config.site_id, season_start, season_end, tiles
            )
            with open("s2-products.pickle", "wb") as file:
                pickle.dump(products_by_tile, file, protocol=pickle.HIGHEST_PROTOCOL)

    if config.stratum_start_dates:
        assert len(strata) == len(config.stratum_start_dates)
    if config.stratum_end_dates:
        assert len(strata) == len(config.stratum_end_dates)

    first_date = season_end
    last_date = season_start
    for tile, products in products_by_tile.items():
        for p in products:
            if p.date < first_date:
                first_date = p.date
            if p.date > last_date:
                last_date = p.date

    step = 10

    if config.date_filter:
        first_date = config.date_filter[0]
        last_date = config.date_filter[0]
        # TODO support multiple dates
        output_dates = [first_date]
    else:
        output_dates = []
        d = first_date
        last_output_date = d
        while d <= last_date:
            last_output_date = d
            output_dates.append(d)
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

    s1_features = list(s1_features)

    output_dates_param = []
    d = first_date
    while d <= last_date:
        output_dates_param.append(str((d - season_start).days))
        d += timedelta(days=step)

    input_dates = {}
    for tile, products in products_by_tile.items():
        days = [(p.date - season_start).days for p in products]
        input_dates[tile] = list(map(str, days))

    l2a_block_size_10m = None
    l2a_block_size_20m = None
    l2a_data_type = None
    commands = []
    for tile, products in products_by_tile.items():
        if len(products) == 0:
            print("No S2 products for tile", tile)
            continue

        b2s = [p.b2 for p in products]
        b3s = [p.b3 for p in products]
        b4s = [p.b4 for p in products]
        b8s = [p.b8 for p in products]
        b5s = [p.b5 for p in products]
        b6s = [p.b6 for p in products]
        b7s = [p.b7 for p in products]
        b11s = [p.b11 for p in products]
        b12s = [p.b12 for p in products]

        mask_10m_vrt = f"mask_10m_{tile}.vrt"
        mask_20m_vrt = f"mask_20m_{tile}.vrt"

        masks_10m = [p.mask_10m for p in products]
        masks_20m = [p.mask_20m for p in products]

        tile_info_masks_10m = None
        if (
            feature_set.need_s2_b2()
            or feature_set.need_s2_b3()
            or feature_set.need_s2_b4()
            or feature_set.need_s2_b8()
        ):
            tile_info_masks_10m = TileInfo.from_dataset(masks_10m[0])
            write_stack_vrt_fast(tile_info_masks_10m, masks_10m, mask_10m_vrt)
        tile_info_masks_20m = None
        if (
            feature_set.need_s2_b5()
            or feature_set.need_s2_b6()
            or feature_set.need_s2_b7()
            or feature_set.need_s2_b11()
            or feature_set.need_s2_b12()
        ):
            tile_info_masks_20m = TileInfo.from_dataset(masks_20m[0])
            write_stack_vrt_fast(tile_info_masks_20m, masks_20m, mask_20m_vrt)

        b2_vrt = f"S2_B02_{tile}.vrt"
        b3_vrt = f"S2_B03_{tile}.vrt"
        b4_vrt = f"S2_B04_{tile}.vrt"
        b8_vrt = f"S2_B08_{tile}.vrt"
        b5_vrt = f"S2_B05_{tile}.vrt"
        b6_vrt = f"S2_B06_{tile}.vrt"
        b7_vrt = f"S2_B07_{tile}.vrt"
        b11_vrt = f"S2_B11_{tile}.vrt"
        b12_vrt = f"S2_B12_{tile}.vrt"

        if l2a_block_size_10m is None:
            path = None
            if feature_set.need_s2_b2():
                path = b2s[0]
            elif feature_set.need_s2_b3():
                path = b3s[0]
            elif feature_set.need_s2_b4():
                path = b4s[0]
            elif feature_set.need_s2_b8():
                path = b8s[0]
            if path:
                ds = gdal.Open(path)
                band = ds.GetRasterBand(1)
                l2a_block_size_10m = band.GetBlockSize()
                l2a_data_type = band.DataType
                del ds

        if l2a_block_size_20m is None:
            path = None
            if feature_set.need_s2_b5():
                path = b5s[0]
            elif feature_set.need_s2_b6():
                path = b6s[0]
            elif feature_set.need_s2_b7():
                path = b7s[0]
            elif feature_set.need_s2_b11():
                path = b11s[0]
            elif feature_set.need_s2_b12():
                path = b12s[0]
            if path:
                ds = gdal.Open(path)
                band = ds.GetRasterBand(1)
                l2a_block_size_20m = band.GetBlockSize()
                del ds

        if tile_info_masks_10m is not None:
            tile_info_10m = dataclasses.replace(
                tile_info_masks_10m,
                block_size=l2a_block_size_10m,
                data_type=l2a_data_type,
            )
        if tile_info_masks_20m is not None:
            tile_info_20m = dataclasses.replace(
                tile_info_masks_20m,
                block_size=l2a_block_size_20m,
                data_type=l2a_data_type,
            )

        if feature_set.need_s2_b2():
            if not tile_info_10m:
                tile_info_10m = TileInfo.from_dataset(b2s[0])
            write_stack_vrt_fast(tile_info_10m, b2s, b2_vrt)
        if feature_set.need_s2_b3():
            if not tile_info_10m:
                tile_info_10m = TileInfo.from_dataset(b3s[0])
            write_stack_vrt_fast(tile_info_10m, b3s, b3_vrt)
        if feature_set.need_s2_b4():
            if not tile_info_10m:
                tile_info_10m = TileInfo.from_dataset(b4s[0])
            write_stack_vrt_fast(tile_info_10m, b4s, b4_vrt)
        if feature_set.need_s2_b8():
            if not tile_info_10m:
                tile_info_10m = TileInfo.from_dataset(b8s[0])
            write_stack_vrt_fast(tile_info_10m, b8s, b8_vrt)

        if feature_set.need_s2_b5():
            if not tile_info_20m:
                tile_info_20m = TileInfo.from_dataset(b5s[0])
            write_stack_vrt_fast(tile_info_20m, b5s, b5_vrt)
        if feature_set.need_s2_b6():
            if not tile_info_20m:
                tile_info_20m = TileInfo.from_dataset(b6s[0])
            write_stack_vrt_fast(tile_info_20m, b6s, b6_vrt)
        if feature_set.need_s2_b7():
            if not tile_info_20m:
                tile_info_20m = TileInfo.from_dataset(b7s[0])
            write_stack_vrt_fast(tile_info_20m, b7s, b7_vrt)
        if feature_set.need_s2_b11():
            if not tile_info_20m:
                tile_info_20m = TileInfo.from_dataset(b11s[0])
            write_stack_vrt_fast(tile_info_20m, b11s, b11_vrt)
        if feature_set.need_s2_b12():
            if not tile_info_20m:
                tile_info_20m = TileInfo.from_dataset(b12s[0])
            write_stack_vrt_fast(tile_info_20m, b12s, b12_vrt)

    containers = []
    tiling_suffix = "?&gdal:co:TILED=YES&streaming:type=tiled&streaming:sizemode=height&streaming:sizevalue=256"
    for tile, products in products_by_tile.items():
        b2_vrt = f"S2_B02_{tile}.vrt"
        b3_vrt = f"S2_B03_{tile}.vrt"
        b4_vrt = f"S2_B04_{tile}.vrt"
        b8_vrt = f"S2_B08_{tile}.vrt"
        b5_vrt = f"S2_B05_{tile}.vrt"
        b6_vrt = f"S2_B06_{tile}.vrt"
        b7_vrt = f"S2_B07_{tile}.vrt"
        b11_vrt = f"S2_B11_{tile}.vrt"
        b12_vrt = f"S2_B12_{tile}.vrt"

        b2_tif = f"S2_B02_{tile}.tif"
        b3_tif = f"S2_B03_{tile}.tif"
        b4_tif = f"S2_B04_{tile}.tif"
        b8_tif = f"S2_B08_{tile}.tif"
        b5_tif = f"S2_B05_{tile}.tif"
        b6_tif = f"S2_B06_{tile}.tif"
        b7_tif = f"S2_B07_{tile}.tif"
        b11_tif = f"S2_B11_{tile}.tif"
        b12_tif = f"S2_B12_{tile}.tif"

        mask_10m_vrt = f"mask_10m_{tile}.vrt"
        mask_20m_vrt = f"mask_20m_{tile}.vrt"

        band_offsets = [
            (b, [-(p.band_offsets.get(b) or 0) for p in products]) for b in range(13)
        ]
        band_offsets_str = dict(
            [(b, [str(o) for o in offsets]) for (b, offsets) in band_offsets]
        )

        interpolation_no_data = -10000
        # interpolation_max_distance = 30
        # interpolation_window_radius = 15
        interpolation_max_distance = 0
        interpolation_window_radius = 0

        common_temporal_resampling_args = (
            ["-indates"]
            + input_dates[tile]
            + [
                "-outdates",
            ]
            + output_dates_param
            + [
                "-bv",
                str(0),
                "-nan",
                str(interpolation_no_data),
            ]
        )
        if interpolation_max_distance:
            common_temporal_resampling_args += [
                "-maxdist",
                str(interpolation_max_distance),
            ]
        if interpolation_window_radius:
            common_temporal_resampling_args += [
                "-winradius",
                str(interpolation_window_radius),
            ]

        if feature_set.need_s2_b2() and not os.path.exists(b2_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b2_vrt,
                    "-mask",
                    mask_10m_vrt,
                    "-out",
                    b2_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B2]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b2_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b3() and not os.path.exists(b3_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b3_vrt,
                    "-mask",
                    mask_10m_vrt,
                    "-out",
                    b3_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B3]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b3_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b4() and not os.path.exists(b4_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b4_vrt,
                    "-mask",
                    mask_10m_vrt,
                    "-out",
                    b4_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B4]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b4_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b8() and not os.path.exists(b8_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b8_vrt,
                    "-mask",
                    mask_10m_vrt,
                    "-out",
                    b8_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B8]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b8_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b5() and not os.path.exists(b5_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b5_vrt,
                    "-mask",
                    mask_20m_vrt,
                    "-out",
                    b5_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B5]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b5_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b6() and not os.path.exists(b6_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b6_vrt,
                    "-mask",
                    mask_20m_vrt,
                    "-out",
                    b6_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B6]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b6_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b7() and not os.path.exists(b7_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b7_vrt,
                    "-mask",
                    mask_20m_vrt,
                    "-out",
                    b7_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B7]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b7_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b11() and not os.path.exists(b11_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b11_vrt,
                    "-mask",
                    mask_20m_vrt,
                    "-out",
                    b11_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B11]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b11_tif],
                environment=env,
            )
            containers.append(container)
        if feature_set.need_s2_b12() and not os.path.exists(b12_tif):
            command = (
                [
                    "otbcli_TemporalResampling",
                    "-in",
                    b12_vrt,
                    "-mask",
                    mask_20m_vrt,
                    "-out",
                    b12_tif + tiling_suffix,
                    "-inoffsets",
                ]
                + band_offsets_str[Sentinel2Band.B12]
                + common_temporal_resampling_args
            )
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                volumes=volumes,
                outputs=[b12_tif],
                environment=env,
            )
            containers.append(container)

    run_containers_concurrently(client, pool_med_conc, containers)

    commands = []
    for tile in products_by_tile.keys():
        b5_tif = f"S2_B05_{tile}.tif"
        b6_tif = f"S2_B06_{tile}.tif"
        b7_tif = f"S2_B07_{tile}.tif"
        b11_tif = f"S2_B11_{tile}.tif"
        b12_tif = f"S2_B12_{tile}.tif"

        b5_nodata_vrt = f"S2_B05_{tile}_nodata.vrt"
        b6_nodata_vrt = f"S2_B06_{tile}_nodata.vrt"
        b7_nodata_vrt = f"S2_B07_{tile}_nodata.vrt"
        b11_nodata_vrt = f"S2_B11_{tile}_nodata.vrt"
        b12_nodata_vrt = f"S2_B12_{tile}_nodata.vrt"

        if feature_set.need_s2_b5():
            command = [
                "gdal_translate",
                "-q",
                "-a_nodata",
                "-10000",
                b5_tif,
                b5_nodata_vrt,
            ]
            commands.append(command)
        if feature_set.need_s2_b6():
            command = [
                "gdal_translate",
                "-q",
                "-a_nodata",
                "-10000",
                b6_tif,
                b6_nodata_vrt,
            ]
            commands.append(command)
        if feature_set.need_s2_b7():
            command = [
                "gdal_translate",
                "-q",
                "-a_nodata",
                "-10000",
                b7_tif,
                b7_nodata_vrt,
            ]
            commands.append(command)
        if feature_set.need_s2_b11():
            command = [
                "gdal_translate",
                "-q",
                "-a_nodata",
                "-10000",
                b11_tif,
                b11_nodata_vrt,
            ]
            commands.append(command)
        if feature_set.need_s2_b12():
            command = [
                "gdal_translate",
                "-q",
                "-a_nodata",
                "-10000",
                b12_tif,
                b12_nodata_vrt,
            ]
            commands.append(command)

    pool_med_conc.map(run_command, commands, chunksize=1)

    commands = []
    for tile in products_by_tile.keys():
        b5_nodata_vrt = f"S2_B05_{tile}_nodata.vrt"
        b6_nodata_vrt = f"S2_B06_{tile}_nodata.vrt"
        b7_nodata_vrt = f"S2_B07_{tile}_nodata.vrt"
        b11_nodata_vrt = f"S2_B11_{tile}_nodata.vrt"
        b12_nodata_vrt = f"S2_B12_{tile}_nodata.vrt"

        b5_10m_vrt = f"S2_B05_10m_{tile}.vrt"
        b6_10m_vrt = f"S2_B06_10m_{tile}.vrt"
        b7_10m_vrt = f"S2_B07_10m_{tile}.vrt"
        b11_10m_vrt = f"S2_B11_10m_{tile}.vrt"
        b12_10m_vrt = f"S2_B12_10m_{tile}.vrt"

        if feature_set.need_s2_b5():
            command = [
                "gdal_translate",
                "-q",
                "-tr",
                "10",
                "10",
                "-r",
                "cubic",
                b5_nodata_vrt,
                b5_10m_vrt,
            ]
            commands.append(command)

        if feature_set.need_s2_b6():
            command = [
                "gdal_translate",
                "-q",
                "-tr",
                "10",
                "10",
                "-r",
                "cubic",
                b6_nodata_vrt,
                b6_10m_vrt,
            ]
            commands.append(command)

        if feature_set.need_s2_b7():
            command = [
                "gdal_translate",
                "-q",
                "-tr",
                "10",
                "10",
                "-r",
                "cubic",
                b7_nodata_vrt,
                b7_10m_vrt,
            ]
            commands.append(command)

        if feature_set.need_s2_b11():
            command = [
                "gdal_translate",
                "-q",
                "-tr",
                "10",
                "10",
                "-r",
                "cubic",
                b11_nodata_vrt,
                b11_10m_vrt,
            ]
            commands.append(command)

        if feature_set.need_s2_b12():
            command = [
                "gdal_translate",
                "-q",
                "-tr",
                "10",
                "10",
                "-r",
                "cubic",
                b12_nodata_vrt,
                b12_10m_vrt,
            ]
            commands.append(command)

    pool_med_conc.map(run_command, commands, chunksize=1)

    containers = []
    for tile in products_by_tile.keys():
        b2_tif = f"S2_B02_{tile}.tif"
        b3_tif = f"S2_B03_{tile}.tif"
        b4_tif = f"S2_B04_{tile}.tif"
        b8_tif = f"S2_B08_{tile}.tif"

        b5_tif = f"S2_B05_{tile}.tif"
        b6_tif = f"S2_B06_{tile}.tif"
        b7_tif = f"S2_B07_{tile}.tif"
        b11_tif = f"S2_B11_{tile}.tif"
        b12_tif = f"S2_B12_{tile}.tif"

        b5_10m_vrt = f"S2_B05_10m_{tile}.vrt"
        b6_10m_vrt = f"S2_B06_10m_{tile}.vrt"
        b7_10m_vrt = f"S2_B07_10m_{tile}.vrt"
        b11_10m_vrt = f"S2_B11_10m_{tile}.vrt"
        b12_10m_vrt = f"S2_B12_10m_{tile}.vrt"

        ndvi = f"S2_NDVI_{tile}.tif"
        ndwi = f"S2_NDWI_{tile}.tif"
        brightness = f"S2_BRIGHTNESS_{tile}.tif"

        ndvi_statistics = f"S2_NDVI_STATISTICS_{tile}.tif"
        ndwi_statistics = f"S2_NDWI_STATISTICS_{tile}.tif"
        brightness_statistics = f"S2_BRIGHTNESS_STATISTICS_{tile}.tif"

        ndre = f"S2_NDRE_{tile}.tif"
        repi = f"S2_REPI_{tile}.tif"
        psri = f"S2_PSRI_{tile}.tif"
        cire = f"S2_CIRE_{tile}.tif"

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
                ndvi + tiling_suffix,
                "-outndwi",
                ndwi + tiling_suffix,
                "-outbrightness",
                brightness + tiling_suffix,
            ]

            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                outputs=[ndvi, ndwi, brightness],
                volumes=volumes,
            )
            containers.append(container)

        if feature_set.need_red_edge_features() and (
            not os.path.exists(ndre)
            or not os.path.exists(repi)
            or not os.path.exists(psri)
            or not os.path.exists(cire)
        ):
            command = [
                "otbcli",
                "S4SCMRedEdgeFeatures",
                "-bv",
                "-10000",
                "-b2",
                b2_tif,
                "-b4",
                b4_tif,
                "-b5",
                b5_10m_vrt,
                "-b6",
                b6_10m_vrt,
                "-b7",
                b7_10m_vrt,
                "-b8",
                b8_tif,
                "-outndre",
                ndre + tiling_suffix,
                "-outrepi",
                repi + tiling_suffix,
                "-outpsri",
                psri + tiling_suffix,
                "-outcire",
                cire + tiling_suffix,
            ]

            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                outputs=[ndre, repi, psri, cire],
                volumes=volumes,
            )
            containers.append(container)
    run_containers_concurrently(client, pool_med_conc, containers)

    containers = []
    for tile in products_by_tile.keys():
        ndvi = f"S2_NDVI_{tile}.tif"
        ndwi = f"S2_NDWI_{tile}.tif"
        brightness = f"S2_BRIGHTNESS_{tile}.tif"

        ndvi_statistics = f"S2_NDVI_STATISTICS_{tile}.tif"
        ndwi_statistics = f"S2_NDWI_STATISTICS_{tile}.tif"
        brightness_statistics = f"S2_BRIGHTNESS_STATISTICS_{tile}.tif"

        commands = []
        if feature_set.need_vegetation_indices_statistics():
            ndvi_statistics = f"S2_NDVI_STATISTICS_{tile}.tif"
            ndwi_statistics = f"S2_NDWI_STATISTICS_{tile}.tif"
            brightness_statistics = f"S2_BRIGHTNESS_STATISTICS_{tile}.tif"

            if not os.path.exists(ndvi_statistics):
                command = [
                    "otbcli",
                    "S4SCMSpectralIndicesStatistics",
                    "-in",
                    ndvi,
                    "-out",
                    ndvi_statistics + tiling_suffix,
                ]
                commands.append(command)
            if not os.path.exists(ndwi_statistics):
                command = [
                    "otbcli",
                    "S4SCMSpectralIndicesStatistics",
                    "-in",
                    ndwi,
                    "-out",
                    ndwi_statistics + tiling_suffix,
                ]
                commands.append(command)
            if not os.path.exists(brightness_statistics):
                command = [
                    "otbcli",
                    "S4SCMSpectralIndicesStatistics",
                    "-in",
                    brightness,
                    "-out",
                    brightness_statistics + tiling_suffix,
                ]
                commands.append(command)

        for command in commands:
            container = ContainerInfo(
                image=PROCESSORS_NEW_IMAGE_NAME,
                command=command,
                working_dir=output_dir,
                outputs=[ndvi_statistics, ndwi_statistics, brightness_statistics],
                volumes=volumes,
            )
            containers.append(container)
    run_containers_concurrently(client, pool_med_conc, containers)

    if config.stratum_start_dates and config.stratum_end_dates:
        stratum_date_filters = list(
            zip(config.stratum_start_dates, config.stratum_end_dates)
        )
    else:
        stratum_date_filters = None

    strata_for_tile: defaultdict[str, List[Optional[int]]] = defaultdict(lambda: [])
    for stratum in strata:
        for tile in stratum.tiles:
            tile_id = tile.tile_id
            strata_for_tile[tile_id].append(stratum.stratum_id)

    if args.broceliande:
        tile_inputs_training: dict[str, str] = {}
        tile_inputs_validation: dict[str, str] = {}
        commands = []
        for tile_id, strata in strata_for_tile.items():
            if not strata:
                pass

            if not strata[0]:
                training_polygons = f"training_polygons_{tile_id}.gpkg"
                if os.path.exists(training_polygons):
                    tile_inputs_training[tile_id] = training_polygons
                validation_polygons = f"validation_polygons_{tile_id}.gpkg"
                if os.path.exists(validation_polygons):
                    tile_inputs_validation[tile_id] = validation_polygons
            else:
                has_training_polygons = False
                has_validation_polygons = False

                broceliande_training_vrt = f"training_polygons_{tile_id}.vrt"
                broceliande_validation_vrt = f"validation_polygons_{tile_id}.vrt"
                training_command = [
                    "ogrmerge.py",
                    "-overwrite_ds",
                    "-single",
                    "-o",
                    broceliande_training_vrt,
                ]
                validation_command = [
                    "ogrmerge.py",
                    "-overwrite_ds",
                    "-single",
                    "-o",
                    broceliande_validation_vrt,
                ]
                for stratum_id in strata:
                    training_polygons = f"training_polygons_{stratum_id}_{tile_id}.gpkg"
                    validation_polygons = (
                        f"validation_polygons_{stratum_id}_{tile_id}.gpkg"
                    )
                    if os.path.exists(training_polygons):
                        training_command.append(training_polygons)
                        has_training_polygons = True
                    if os.path.exists(validation_polygons):
                        validation_command.append(validation_polygons)
                        has_validation_polygons = True

                if has_training_polygons:
                    tile_inputs_training[tile_id] = broceliande_training_vrt
                    commands.append(training_command)
                if has_validation_polygons:
                    tile_inputs_validation[tile_id] = broceliande_validation_vrt
                    commands.append(validation_command)
        pool_hi_conc.map(run_command, commands, chunksize=1)

        for tile in tiles:
            training_polygons = tile_inputs_training[tile.tile_id]
            validation_polygons = tile_inputs_validation[tile.tile_id]
            if not training_polygons or not validation_polygons:
                continue

            creation_options = [
                "COMPRESS=DEFLATE",
                "TILED=YES",
                "NUM_THREADS=ALL_CPUS",
            ]
            (min_x, max_x, min_y, max_y) = tile.geom.GetEnvelope()
            bounds = [min_x, min_y, max_x, max_y]
            srs = f"EPSG:{tile.epsg_code}"
            rasterize_options = gdal.RasterizeOptions(
                format="GTiff",
                outputType=gdal.GDT_Byte,
                creationOptions=creation_options,
                outputBounds=bounds,
                outputSRS=srs,
                width=10980,
                height=10980,
                xRes=10,
                yRes=10,
                noData=0,
                attribute="code_lc",
            )
            training_raster = f"broceliande_training_{tile.tile_id}.tif"
            if not os.path.exists(training_raster):
                gdal.Rasterize(
                    training_raster,
                    training_polygons,
                    options=rasterize_options,
                )
            validation_raster = f"broceliande_validation_{tile.tile_id}.tif"
            if not os.path.exists(validation_raster):
                gdal.Rasterize(
                    validation_raster,
                    validation_polygons,
                    options=rasterize_options,
                )
    else:
        stratum_band_names = write_tile_vrts(
            strata, feature_set, s1_features, output_dates, stratum_date_filters
        )
        (training_map, validation_map) = run_sample_extraction(
            client,
            pool_med_conc,
            output_dir,
            volumes,
            env,
            tiles,
            strata,
            stratum_band_names,
        )
        training_map_augmented = run_sample_augmentation(
            client,
            pool_no_conc,
            output_dir,
            volumes,
            env,
            strata,
            training_map,
        )
        confusion_matrices = run_training(
            client,
            pool_med_conc,
            output_dir,
            volumes,
            env,
            processor_config,
            strata,
            stratum_band_names,
            training_map_augmented,
            validation_map,
            args.use_old_otb,
        )

        remapping_table_name = "remapping-table.csv"
        if os.path.exists(remapping_table_name):
            remapping_table = remapping_table_name
            remapping_enabled = True
        else:
            remapping_table = None
            remapping_enabled = False

        run_classification(
            client, pool_med_conc, output_dir, volumes, env, strata, args.use_old_otb
        )

        if strata[0].stratum_id:
            rasterize_stratum_masks(tiles, strata, strata_for_tile)
            merge_strata(
                client,
                pool_hi_conc,
                output_dir,
                volumes,
                env,
                tiles,
                strata_for_tile,
                remapping_enabled,
            )

        if remapping_table:
            tiling_suffix = "?&gdal:co:TILED=YES&gdal:co:COMPRESS=DEFLATE&streaming:type=tiled&streaming:sizemode=height&streaming:sizevalue=256"
            commands = []
            for tile in tiles:
                tile_id = tile.tile_id
                classified_pre_tif = f"classified_pre_{tile_id}.tif"
                classified_tif = f"classified_{tile_id}.tif"
                command = [
                    "otbcli",
                    "ClassRemapping",
                    "-table",
                    remapping_table,
                    "-in",
                    classified_pre_tif,
                    "-out",
                    classified_tif + tiling_suffix,
                ]
                commands.append(command)

            containers = []
            for command in commands:
                container = ContainerInfo(
                    image=PROCESSORS_NEW_IMAGE_NAME,
                    command=command,
                    working_dir=output_dir,
                    volumes=volumes,
                )
                containers.append(container)
            run_containers_concurrently(client, pool_med_conc, containers)

        run_command(["product-report.py"])

        if args.output_path:
            for tile in tiles:
                tile_id = tile.tile_id
                classified_tif = f"classified_{tile_id}.tif"
                confidence_map_tif = f"confidence_map_{tile_id}.tif"
                probability_map_tif = f"probability_map_{tile_id}.tif"
                shutil.copy2(classified_tif, args.output_path)
                shutil.copy2(confidence_map_tif, args.output_path)
                # shutil.copy2(probability_map_tif, args.output_path)

                if remapping_table:
                    classified_pre_tif = f"classified_pre_{tile_id}.tif"
                    shutil.copy2(classified_pre_tif, args.output_path)

            for confusion_matrix in confusion_matrices:
                shutil.copy2(confusion_matrix, args.output_path)

            polygons = "polygons.gpkg"
            if os.path.exists(polygons):
                shutil.copy2(polygons, args.output_path)
            polygon_statistics = "polygon-statistics.json"
            if os.path.exists(polygon_statistics):
                shutil.copy2(polygon_statistics, args.output_path)
            classification_report = "classification_report.xlsx"
            if os.path.exists(classification_report):
                shutil.copy2(classification_report, args.output_path)


if __name__ == "__main__":
    main()
