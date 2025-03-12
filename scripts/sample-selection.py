#!/usr/bin/env python
from __future__ import print_function

import argparse
from collections import defaultdict
import csv
from datetime import date
from multiprocessing.dummy import Pool
from typing import Dict, List, Optional
import docker
import glob
import json
import logging
import math
import os
import os.path
from osgeo import gdal, ogr, osr
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
from psycopg2.extensions import connection
from configparser import ConfigParser


OTB_IMAGE_NAME = "docker.io/orfeotoolbox/otb:8.1.1"


class ContainerInfo:
    def __init__(self, image, command, working_dir, volumes, environment=None):
        self.image = image
        self.command = command
        self.working_dir = working_dir
        self.volumes = volumes
        self.environment = environment

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
            return container.wait()
        except Exception as exc:
            print(exc)
            return None


def run_containers_concurrently(client, pool, containers):
    results = pool.map(lambda c: c.run(client), containers, chunksize=1)
    for res in results:
        if res is not None:
            if res["StatusCode"] != 0:
                print(res)


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


def get_site_name(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL("select short_name from site where id = %s")
        cursor.execute(query, (site_id,))
        row = cursor.fetchone()
        conn.commit()
        return row[0]


def get_connection(config):
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )


class Stratum(object):
    def __init__(self, stratum_id: Optional[int], tiles: List[str]) -> None:
        self.stratum_id = stratum_id
        self.tiles = tiles


def get_site_strata(conn: connection, site_id: int) -> List[Stratum]:
    query = SQL("select * from sp_get_site_strata(%s)")
    logging.debug(query.as_string(conn))

    strata = []
    with conn.cursor() as cursor:
        cursor.execute(query, (site_id,))
        for stratum_id, _, _, tiles in cursor:
            stratum = Stratum(stratum_id, tiles)
            strata.append(stratum)

    return strata


class Tile(object):
    def __init__(
        self,
        id,
        raster,
        spatial_ref,
    ):
        self.id = id
        self.raster = raster
        self.spatial_ref = spatial_ref


class TileOutput(object):
    def __init__(
        self,
        tile_id,
        training_polygons,
        validation_polygons,
        training_dataset,
        validation_dataset,
        training_layer,
        validation_layer,
    ):
        self.tile_id = tile_id
        self.training_polygons = training_polygons
        self.validation_polygons = validation_polygons
        self.training_dataset = training_dataset
        self.validation_dataset = validation_dataset
        self.training_layer = training_layer
        self.validation_layer = validation_layer

        self.training_points: Optional[str] = None
        self.validation_points: Optional[str] = None


class CropStatistics(object):
    def __init__(self):
        self.training_polygons = 0
        self.validation_polygons = 0
        self.estimated_training_pixels = 0
        self.estimated_validation_pixels = 0
        self.training_samples = 0
        self.validation_samples = 0
        self.smote_samples = 0


def create_tile_outputs(
    driver: ogr.Driver,
    stratum_id: Optional[int],
    tile: Tile,
    fields: List[ogr.FieldDefn],
) -> TileOutput:
    tile_id = tile.id
    if stratum_id:
        training_polygons = f"training_polygons_{stratum_id}_{tile_id}.gpkg"
        validation_polygons = f"validation_polygons_{stratum_id}_{tile_id}.gpkg"
    else:
        training_polygons = f"training_polygons_{tile_id}.gpkg"
        validation_polygons = f"validation_polygons_{tile_id}.gpkg"

    if os.path.exists(training_polygons):
        driver.DeleteDataSource(training_polygons)
    if os.path.exists(validation_polygons):
        driver.DeleteDataSource(validation_polygons)
    training_dataset = driver.CreateDataSource(training_polygons)
    validation_dataset = driver.CreateDataSource(validation_polygons)

    training_layer = training_dataset.CreateLayer(
        "polygons",
        tile.spatial_ref,
        ogr.wkbUnknown,
    )
    validation_layer = validation_dataset.CreateLayer(
        "polygons",
        tile.spatial_ref,
        ogr.wkbUnknown,
    )

    for field in fields:
        training_layer.CreateField(field)
        validation_layer.CreateField(field)

    tile_output = TileOutput(
        tile_id,
        training_polygons,
        validation_polygons,
        training_dataset,
        validation_dataset,
        training_layer,
        validation_layer,
    )
    return tile_output


def main():
    parser = argparse.ArgumentParser(
        description="Select polygons for S4S/Crop Type",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("--year", help="year", type=int, default=date.today().year)
    parser.add_argument("--mounts", help="paths to mount in containers", nargs="*")

    required_args = parser.add_argument_group("required named arguments")
    required_args.add_argument(
        "-s", "--site-id", type=int, required=True, help="site ID to filter by"
    )
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
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    parser.add_argument("--working-path", help="working path")

    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level)

    config = Config(args)

    driver = ogr.GetDriverByName("GPKG")

    parcel_id_field = ogr.FieldDefn("id", ogr.OFTInteger)
    code_n1_field = ogr.FieldDefn("code_n1", ogr.OFTInteger)
    code_n2_field = ogr.FieldDefn("code_n2", ogr.OFTInteger)
    code_n3_field = ogr.FieldDefn("code_n3", ogr.OFTInteger)
    code_n4_field = ogr.FieldDefn("code_n4", ogr.OFTInteger)
    code_lc_field = ogr.FieldDefn("code_lc", ogr.OFTInteger)
    crop_code_field = ogr.FieldDefn("crop_code", ogr.OFTInteger)
    pix_10m_field = ogr.FieldDefn("pix_10m", ogr.OFTInteger)
    strategy_field = ogr.FieldDefn("strategy", ogr.OFTInteger)
    crop_pixels_field = ogr.FieldDefn("crop_pixels", ogr.OFTInteger)
    total_pixels_field = ogr.FieldDefn("total_pixels", ogr.OFTInteger)
    polygon_num_field = ogr.FieldDefn("polygon_num", ogr.OFTInteger)
    pixel_ratio_field = ogr.FieldDefn("pixel_ratio", ogr.OFTReal)

    fields = [
        parcel_id_field,
        code_n1_field,
        code_n2_field,
        code_n3_field,
        code_n4_field,
        code_lc_field,
        crop_code_field,
        pix_10m_field,
        strategy_field,
        crop_pixels_field,
        total_pixels_field,
        polygon_num_field,
        pixel_ratio_field,
    ]

    polygon_class_statistics_commands = []
    sample_selection_commands = []

    with get_connection(config) as conn:
        site_name = get_site_name(conn, config.site_id)

        parcels_table = "in_situ_polygons_{}_{}".format(site_name, args.year)
        attributes_table = "polygon_attributes_{}_{}".format(site_name, args.year)
        statistical_data_table = "in_situ_data_{}_{}".format(site_name, args.year)

        parcels_table_id = Identifier(parcels_table)
        attributes_table_id = Identifier(attributes_table)
        statistical_data_id = Identifier(statistical_data_table)

        query = SQL("select Find_SRID('public', {}, 'wkb_geometry')").format(
            Literal(parcels_table)
        )
        logging.debug(query.as_string(conn))
        with conn.cursor() as cursor:
            cursor.execute(query)
            site_srid = cursor.fetchone()[0]
            site_srs = osr.SpatialReference()
            site_srs.ImportFromEPSG(site_srid)
            site_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

            query = SQL(
                """
select value
from sp_get_parameters('processor.insitu.path')
where site_id is null or site_id = %s
order by site_id;"""
            )
            logging.debug(query.as_string(conn))
            cursor.execute(query, (config.site_id,))
            insitu_path = cursor.fetchone()[0]

            query = SQL("select short_name from site where id = %s")
            logging.debug(query.as_string(conn))
            cursor.execute(query, (config.site_id,))
            site_short_name = cursor.fetchone()[0]

            insitu_path = insitu_path.replace("{site}", site_short_name)
            insitu_path = insitu_path.replace("{year}", str(args.year))

        tile_rasters = glob.glob(os.path.join(insitu_path, "*_10m.tif"))
        tiles: Dict[str, Tile] = {}
        transforms = {}
        output_dir = os.path.abspath(".")  # TODO

        volumes = {
            output_dir: {"bind": output_dir, "mode": "rw"},
            insitu_path: {"bind": insitu_path, "mode": "ro"},
        }

        if args.mounts:
            for mount in args.mounts:
                volumes[mount] = {"bind": mount, "mode": "ro"}

        for path in tile_rasters:
            name = os.path.splitext(os.path.basename(path))[0]
            parts = name.split("_")
            tile_id = parts[len(parts) - 2]

            ds = gdal.Open(path, gdal.gdalconst.GA_ReadOnly)
            projection = ds.GetSpatialRef()

            if tile_id not in transforms:
                transform = osr.CoordinateTransformation(site_srs, projection)
                transforms[tile_id] = transform

            tile = Tile(
                tile_id,
                path,
                projection,
            )
            tiles[tile_id] = tile

        if args.remapping_set_id:
            crop_code_column = SQL("crop_remapping_set_detail.remapped_code_pre")
            remapping_set_join = SQL(
                "inner join crop_remapping_set_detail on (crop_remapping_set_detail.crop_remapping_set_id, crop_remapping_set_detail.original_code) = ({}, statistical_data.crop_code)"
            ).format(Literal(args.remapping_set_id))
            remapped_code_filter = SQL(
                """
                and (%(monitored_crops_remapped_pre)s is null
                  or crop_remapping_set_detail.remapped_code_pre = any (%(monitored_crops_remapped_pre)s))
                and (%(excluded_crops_remapped_pre)s is null
                  or crop_remapping_set_detail.remapped_code_pre <> all (%(excluded_crops_remapped_pre)s))
                """
            )

            query = SQL(
                """
                select distinct
                       remapped_code_pre,
                       remapped_code_post
                from crop_remapping_set_detail
                where crop_remapping_set_id = %s;
                """
            )
            logging.debug(query.as_string(conn))

            with conn.cursor() as cursor, open(
                "remapping-table.csv", "wt", encoding="utf-8"
            ) as csvfile:
                cursor.execute(query, (args.remapping_set_id,))
                writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

                for row in cursor:
                    writer.writerow(row)
        else:
            crop_code_column = SQL("statistical_data.crop_code")
            remapping_set_join = SQL("")
            remapped_code_filter = SQL("")

            try:
                os.remove("remapping-table.csv")
            except OSError:
                pass

        strata = get_site_strata(conn, config.site_id)
        if not strata:
            stratum = Stratum(None, list(tiles.keys()))
            strata.append(stratum)

        query = SQL(
            """
with eligible_polygons as (
    select polygons.parcel_id
            , ST_Multi(ST_Buffer(polygons.wkb_geometry, -10)) as wkb_geometry
            , attributes.pix_10m
            , {} as crop_code
            , crop_list_n4.code_n4
            , crop_list_n3.code_n3
            , crop_list_n2.code_n2
            , crop_list_n2.code_n1
            , case crop_list_n2.code_n1
                when 1 then 1
                when 2 then 2
                else 3
            end as land_cover_class
            , sum(pix_10m) over (partition by {}) as crop_pixels
            , sum(pix_10m) over () as total_pixels
            , count(*) over (partition by {}) as polygon_num
            , attributes.tile_id as tile_id
    from {} polygons
        inner join {} attributes using (parcel_id)
        inner join {} statistical_data using (parcel_id)
        inner join crop_list_n4 on crop_list_n4.code_n4 = statistical_data.crop_code
        inner join crop_list_n3 using (code_n3)
        inner join crop_list_n2 using (code_n2)
        {}
    where geom_valid
      and not multipart
      and not overlap
      --  and quality_control
      and attributes.tile_id is not null
      and pix_10m >= %(pix_min)s
      and (%(monitored_land_covers)s is null
      or code_n1 = any (%(monitored_land_covers)s))
      and (%(monitored_crops)s is null
      or crop_code = any (%(monitored_crops)s))
      and stratum_crop_id = %(stratum_id)s
      {}
),
eligible_polygons_with_attr as (
    select *,
            crop_pixels :: float / total_pixels as pixel_ratio
    from eligible_polygons
),
selected_polygons as (
    select eligible_polygons_with_attr.*,
            case
                when pix_10m < %(pix_best)s then 4
                when pixel_ratio >= %(pix_ratio_hi)s then 1
                when pixel_ratio >= %(pix_ratio_lo)s then 2
                else 3
            end as strategy
    from eligible_polygons_with_attr
    where pixel_ratio >= %(pix_ratio_min)s
      and polygon_num >= %(poly_min)s
)
select selected_polygons.parcel_id,
       ST_AsBinary(selected_polygons.wkb_geometry),
       selected_polygons.pix_10m,
       selected_polygons.crop_code,
       selected_polygons.code_n4,
       selected_polygons.code_n3,
       selected_polygons.code_n2,
       selected_polygons.code_n1,
       selected_polygons.land_cover_class,
       selected_polygons.crop_pixels,
       selected_polygons.total_pixels,
       selected_polygons.polygon_num,
       selected_polygons.pixel_ratio,
       selected_polygons.strategy,
       selected_polygons.tile_id
from selected_polygons
order by random();
            """
        ).format(
            crop_code_column,
            crop_code_column,
            crop_code_column,
            parcels_table_id,
            attributes_table_id,
            statistical_data_id,
            remapping_set_join,
            remapped_code_filter,
        )
        logging.debug(query.as_string(conn))

        statistics = defaultdict(lambda: defaultdict(lambda: CropStatistics()))
        stratum_tile_outputs = defaultdict(lambda: {})
        for stratum in strata:
            stratum_id = stratum.stratum_id or 0
            tile_outputs: Dict[str, TileOutput] = stratum_tile_outputs[stratum_id]
            print(f"Stratum {stratum_id}: ", stratum.tiles)

            training_pixels = defaultdict(lambda: 0)
            training_target = {}

            PURPOSE_TRAINING = 0
            PURPOSE_VALIDATION = 1

            stratum_statistics = statistics[stratum_id]
            smote_targets = {}
            with conn.cursor() as cursor:
                query_args = {
                    "site_id": config.site_id,
                    "stratum_id": stratum_id,
                    "pix_min": args.pix_min,
                    "pix_best": args.pix_best,
                    "pix_ratio_min": args.pix_ratio_min,
                    "poly_min": args.poly_min,
                    "pix_ratio_hi": args.pix_ratio_hi,
                    "pix_ratio_lo": args.pix_ratio_lo,
                    "monitored_land_covers": args.monitored_land_covers,
                    "monitored_crops": args.monitored_crops,
                    "monitored_crops_remapped_pre": args.monitored_crops_remapped_pre,
                    "excluded_crops_remapped_pre": args.excluded_crops_remapped_pre,
                }

                cursor.execute(query, query_args)
                for (
                    parcel_id,
                    geometry,
                    pix_10m,
                    crop_code,
                    code_n4,
                    code_n3,
                    code_n2,
                    code_n1,
                    code_lc,
                    crop_pixels,
                    total_pixels,
                    polygon_num,
                    pixel_ratio,
                    strategy,
                    tile_id,
                ) in cursor:
                    geom = ogr.CreateGeometryFromWkb(bytes(geometry))
                    geom.AssignSpatialReference(site_srs)
                    transform = transforms[tile_id]
                    geom.Transform(transform)

                    crop_statistics = stratum_statistics[crop_code]
                    if strategy != 4:
                        crop_target = None
                        if strategy == 1:
                            crop_target = args.sample_ratio_hi * crop_pixels
                        elif strategy == 2 or strategy == 3:
                            crop_target = args.sample_ratio_lo * crop_pixels
                        else:
                            raise RuntimeError(
                                f"Invalid strategy for crop {crop_code}: {strategy}"
                            )

                        crop_target = math.ceil(crop_target)
                        if crop_code not in training_target:
                            print(
                                "Target pixels for crop {}: {}".format(
                                    crop_code, crop_target
                                )
                            )
                            training_target[crop_code] = crop_target

                        if strategy == 3:
                            smote_target = int(
                                round(
                                    args.smote_ratio * total_pixels
                                    - args.sample_ratio_lo * crop_pixels
                                )
                            )
                            if crop_code not in smote_targets:
                                smote_targets[crop_code] = smote_target
                                crop_statistics.smote_samples = smote_target

                        pixels = training_pixels[crop_code]
                        if pixels < crop_target:
                            training_pixels[crop_code] = pixels + pix_10m
                            purpose = PURPOSE_TRAINING
                        else:
                            purpose = PURPOSE_VALIDATION
                    else:
                        purpose = PURPOSE_VALIDATION

                    tile_output = tile_outputs.get(tile_id)
                    if not tile_output:
                        tile = tiles[tile_id]
                        tile_output = create_tile_outputs(
                            driver,
                            stratum.stratum_id,
                            tile,
                            fields,
                        )
                        tile_outputs[tile_id] = tile_output

                    if purpose == PURPOSE_TRAINING:
                        feature = ogr.Feature(tile_output.training_layer.GetLayerDefn())
                    else:
                        feature = ogr.Feature(
                            tile_output.validation_layer.GetLayerDefn()
                        )

                    feature.SetFID(parcel_id)
                    feature.SetField("id", parcel_id)
                    feature.SetField("code_n1", code_n1)
                    feature.SetField("code_n2", code_n2)
                    feature.SetField("code_n3", code_n3)
                    feature.SetField("code_n4", code_n4)
                    feature.SetField("code_lc", code_lc)
                    feature.SetField("crop_code", crop_code)
                    feature.SetField("pix_10m", pix_10m)
                    feature.SetField("strategy", strategy)
                    feature.SetField("crop_pixels", crop_pixels)
                    feature.SetField("total_pixels", total_pixels)
                    feature.SetField("polygon_num", polygon_num)
                    feature.SetField("pixel_ratio", pixel_ratio)
                    feature.SetGeometry(geom)

                    if purpose == PURPOSE_TRAINING:
                        tile_output.training_layer.CreateFeature(feature)
                        crop_statistics.training_polygons += 1
                        crop_statistics.estimated_training_pixels += pix_10m
                    else:
                        tile_output.validation_layer.CreateFeature(feature)
                        crop_statistics.validation_polygons += 1
                        crop_statistics.estimated_validation_pixels += pix_10m

            if stratum.stratum_id:
                smote_targets_json = f"smote_targets_{stratum.stratum_id}.json"
            else:
                smote_targets_json = "smote_targets.json"

            with open(smote_targets_json, "wt") as file:
                json.dump(smote_targets, file)

            for tile_id, tile_output in tile_outputs.items():
                tile = tiles[tile_id]

                # HACK
                tile_output.training_layer.SyncToDisk()
                tile_output.training_dataset.SyncToDisk()
                tile_output.validation_layer.SyncToDisk()
                tile_output.validation_dataset.SyncToDisk()

                tile_output.training_layer = None
                tile_output.validation_layer = None
                tile_output.training_dataset = None
                tile_output.validation_dataset = None

                if stratum.stratum_id:
                    training_stats = (
                        f"training_statistics_{stratum.stratum_id}_{tile_id}.xml"
                    )
                    validation_stats = (
                        f"validation_statistics_{stratum.stratum_id}_{tile_id}.xml"
                    )

                    tile_output.training_points = (
                        f"training_points_{stratum.stratum_id}_{tile_id}.gpkg"
                    )
                    tile_output.validation_points = (
                        f"validation_points_{stratum.stratum_id}_{tile_id}.gpkg"
                    )
                else:
                    training_stats = f"training_statistics_{tile_id}.xml"
                    validation_stats = f"validation_statistics_{tile_id}.xml"

                    tile_output.training_points = f"training_points_{tile_id}.gpkg"
                    tile_output.validation_points = f"validation_points_{tile_id}.gpkg"

                command = [
                    "otbcli_PolygonClassStatistics",
                    "-field",
                    "crop_code",
                    "-in",
                    tile.raster,
                    "-vec",
                    tile_output.training_polygons,
                    "-out",
                    training_stats,
                ]
                polygon_class_statistics_commands.append(command)

                command = [
                    "otbcli_PolygonClassStatistics",
                    "-field",
                    "crop_code",
                    "-in",
                    tile.raster,
                    "-vec",
                    tile_output.validation_polygons,
                    "-out",
                    validation_stats,
                ]
                polygon_class_statistics_commands.append(command)

                command = [
                    "otbcli_SampleSelection",
                    "-field",
                    "crop_code",
                    "-strategy",
                    "all",
                    "-in",
                    tile.raster,
                    "-vec",
                    tile_output.training_polygons,
                    "-instats",
                    training_stats,
                    "-out",
                    tile_output.training_points,
                ]
                sample_selection_commands.append(command)

                command = [
                    "otbcli_SampleSelection",
                    "-field",
                    "crop_code",
                    "-strategy",
                    "all",
                    "-in",
                    tile.raster,
                    "-vec",
                    tile_output.validation_polygons,
                    "-instats",
                    validation_stats,
                    "-out",
                    tile_output.validation_points,
                ]
                sample_selection_commands.append(command)

    pool = Pool()
    client = docker.from_env(timeout=600)

    containers = []
    for command in polygon_class_statistics_commands:
        container = ContainerInfo(
            image=OTB_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)

    containers = []
    for command in sample_selection_commands:
        container = ContainerInfo(
            image=OTB_IMAGE_NAME,
            command=command,
            working_dir=output_dir,
            volumes=volumes,
        )
        containers.append(container)
    run_containers_concurrently(client, pool, containers)

    client.close()

    def get_sample_counts(dataset_path):
        ds = gdal.OpenEx(dataset_path)
        lyr = ds.ExecuteSQL("select crop_code, count(*) from output group by crop_code")
        for feat in lyr:
            crop_code = feat.GetField(0)
            count = feat.GetField(1)
            yield crop_code, count

    for stratum_id, tile_outputs in stratum_tile_outputs.items():
        crop_statistics = statistics[stratum_id]

        for tile_output in tile_outputs.values():
            for crop_code, count in get_sample_counts(tile_output.training_points):
                crop_statistics[crop_code].training_samples += count
            for crop_code, count in get_sample_counts(tile_output.validation_points):
                crop_statistics[crop_code].validation_samples += count

    statistics_json = defaultdict(lambda: {})
    stratum_items = list(statistics.items())
    stratum_items.sort(key=lambda x: x[0])
    for stratum_id, stratum_statistics in stratum_items:
        crop_items = list(stratum_statistics.items())
        crop_items.sort(key=lambda x: str(x[0]))

        crop_statistics = {}
        for crop_code, stats in crop_items:
            crop_statistics[crop_code] = {
                "training_polygons": stats.training_polygons,
                "validation_polygons": stats.validation_polygons,
                "estimated_training_pixels": stats.estimated_training_pixels,
                "estimated_validation_pixels": stats.estimated_validation_pixels,
                "training_samples": stats.training_samples,
                "validation_samples": stats.validation_samples,
                "smote_samples": stats.smote_samples,
            }
        statistics_json[stratum_id] = crop_statistics

    with open("polygon-statistics.json", "w") as f:
        json.dump(statistics_json, f, indent=2)


if __name__ == "__main__":
    main()
