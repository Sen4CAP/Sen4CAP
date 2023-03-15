#!/usr/bin/env python
from __future__ import print_function

import argparse
from collections import defaultdict
import csv
from datetime import date
import docker
import glob
import json
import logging
from lxml import etree
from lxml.builder import E
import multiprocessing.dummy
import os
import os.path
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
import psycopg2.extensions
import subprocess

from configparser import ConfigParser


OTB_IMAGE_NAME = "docker.io/orfeotoolbox/otb:8.1.1"


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


class Tile(object):
    def __init__(
        self,
        id,
        raster,
        spatial_ref,
        training_polygons,
        validation_polygons,
        training_dataset,
        validation_dataset,
        training_layer,
        validation_layer,
    ):
        self.id = id
        self.raster = raster
        self.spatial_ref = spatial_ref
        self.training_polygons = training_polygons
        self.validation_polygons = validation_polygons
        self.training_dataset = training_dataset
        self.validation_dataset = validation_dataset
        self.training_layer = training_layer
        self.validation_layer = validation_layer

        self.training_points = None
        self.validation_points = None


def main():
    parser = argparse.ArgumentParser(description="Select polygons for S4S/Crop Type")
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
    parser.add_argument("--tiles", help="tile filter", nargs="*")
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
    with get_connection(config) as conn:
        site_name = get_site_name(conn, config.site_id)

        parcels_table = "in_situ_polygons_{}_{}".format(site_name, args.year)
        attributes_table = "polygon_attributes_{}_{}".format(site_name, args.year)
        statistical_data_table = "in_situ_data_{}_{}".format(site_name, args.year)

        parcels_table_id = Identifier(parcels_table)
        attributes_table_id = Identifier(attributes_table)
        statistical_data_id = Identifier(statistical_data_table)

        sample_ratio_hi = 0.25
        sample_ratio_lo = 0.75
        smote_ratio = 0.0075

        driver = ogr.GetDriverByName("ESRI Shapefile")

        parcel_id_field = ogr.FieldDefn("id", ogr.OFTInteger)
        crop_code_field = ogr.FieldDefn("crop_code", ogr.OFTInteger)
        pix_10m_field = ogr.FieldDefn("pix_10m", ogr.OFTInteger)
        strategy_field = ogr.FieldDefn("strategy", ogr.OFTInteger)

        training_feature_defn = ogr.FeatureDefn()
        training_feature_defn.AddFieldDefn(parcel_id_field)
        training_feature_defn.AddFieldDefn(crop_code_field)
        training_feature_defn.AddFieldDefn(pix_10m_field)
        training_feature_defn.AddFieldDefn(strategy_field)

        validation_feature_defn = ogr.FeatureDefn()
        validation_feature_defn.AddFieldDefn(parcel_id_field)
        validation_feature_defn.AddFieldDefn(crop_code_field)
        validation_feature_defn.AddFieldDefn(pix_10m_field)
        validation_feature_defn.AddFieldDefn(strategy_field)

        # feature_defn = None

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

        tile_rasters = glob.glob(os.path.join(insitu_path, "*.tif"))
        tiles = {}
        transforms = {}
        output_dir = os.path.abspath(".")  # TODO
        for path in tile_rasters:
            name = os.path.splitext(os.path.basename(path))[0]
            parts = name.split("_")
            tile_id = parts[len(parts) - 2]
            if args.tiles is not None and tile_id not in args.tiles:
                continue

            ds = gdal.Open(path, gdal.gdalconst.GA_ReadOnly)
            projection = ds.GetSpatialRef()
            print(
                "Tile id: {}\nSource SRS: {}\nDestination SRS: {}\n".format(
                    tile_id, site_srs.ExportToWkt(), projection.ExportToWkt()
                ),
            )

            if tile_id not in transforms:
                transform = osr.CoordinateTransformation(site_srs, projection)
                transforms[tile_id] = transform

            training_polygons = "training_polygons_{}.shp".format(tile_id)
            validation_polygons = "validation_polygons_{}.shp".format(tile_id)
            if os.path.exists(training_polygons):
                driver.DeleteDataSource(training_polygons)
            if os.path.exists(validation_polygons):
                driver.DeleteDataSource(validation_polygons)
            training_dataset = driver.CreateDataSource(training_polygons)
            validation_dataset = driver.CreateDataSource(validation_polygons)

            training_layer = training_dataset.CreateLayer(
                "polygons",
                projection,
                ogr.wkbMultiPolygon,
            )
            validation_layer = validation_dataset.CreateLayer(
                "polygons",
                projection,
                ogr.wkbMultiPolygon,
            )

            training_layer.CreateField(parcel_id_field)
            training_layer.CreateField(crop_code_field)
            training_layer.CreateField(pix_10m_field)
            training_layer.CreateField(strategy_field)

            validation_layer.CreateField(parcel_id_field)
            validation_layer.CreateField(crop_code_field)
            validation_layer.CreateField(pix_10m_field)
            validation_layer.CreateField(strategy_field)

            tile = Tile(
                tile_id,
                path,
                projection,
                training_polygons,
                validation_polygons,
                training_dataset,
                validation_dataset,
                training_layer,
                validation_layer,
            )
            tiles[tile_id] = tile

        if args.tiles is not None:
            tile_filter = SQL("where site_tiles.tile_id = any(%s)")
        else:
            tile_filter = SQL("")

        if args.remapping_set_id:
            crop_code_column = SQL("crop_remapping_set_detail.remapped_code_pre")
            remapping_set_join = SQL(
                "inner join crop_remapping_set_detail on (crop_remapping_set_detail.crop_remapping_set_id, crop_remapping_set_detail.original_code) = ({}, statistical_data.crop_code)"
            ).format(Literal(args.remapping_set_id))

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

            try:
                os.remove("remapping-table.csv")
            except OSError:
                pass

        query = SQL(
            """
    with site_config as (
        select key,
               value
        from v_site_config
        where site_id = %s
    ),
    config (
        pix_min,
        pix_best,
        pix_ratio_min,
        poly_min,
        pix_ratio_hi,
        pix_ratio_lo,
        monitored_land_covers,
        monitored_crops,
        smote_ratio,
        sample_ratio_hi,
        sample_ratio_lo
    ) as (
        select
                (
                    select value :: int as pix_min
                    from site_config
                    where key = 'processor.s4s_crop_mapping.pix-min'
                ),
                (
                    select value :: int as pix_best
                    from site_config
                    where key = 'processor.s4s_crop_mapping.pix-best'
                ),
                (
                    select value :: float as pix_ratio_min
                    from site_config
                    where key = 'processor.s4s_crop_mapping.pix-ratio-min'
                ),
                (
                    select value :: int as poly_min
                    from site_config
                    where key = 'processor.s4s_crop_mapping.poly-min'
                ),
                (
                    select value :: float as pix_ratio_hi
                    from site_config
                    where key = 'processor.s4s_crop_mapping.pix-ratio-hi'
                ),
                (
                    select value :: float as pix_ratio_lo
                    from site_config
                    where key = 'processor.s4s_crop_mapping.pix-ratio-lo'
                ),
                (
                    select nullif(value, '') :: int[] as monitored_land_covers
                    from site_config
                    where key = 'processor.s4s_crop_mapping.monitored-land-covers'
                ),
                (
                    select nullif(value, '') :: int[] as monitored_crops
                    from site_config
                    where key = 'processor.s4s_crop_mapping.monitored-crops'
                ),
                (
                    select value :: float as smote_ratio
                    from site_config
                    where key = 'processor.s4s_crop_mapping.smote-ratio'
                ),
                (
                    select value :: float as sample_ratio_hi
                    from site_config
                    where key = 'processor.s4s_crop_mapping.sample-ratio-hi'
                ),
                (
                    select value :: float as sample_ratio_lo
                    from site_config
                    where key = 'processor.s4s_crop_mapping.sample-ratio-lo'
                )
    ),
    site_tiles as (
        select unnest(tiles) as tile_id
        from site_tiles
        where site_id = %s
          and satellite_id = 1
    ),
    site_tile_geom as (
        select site_tiles.tile_id,
               ST_Transform(geom, %s) as geom
        from site_tiles
        inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id
        {}
    ),
    eligible_polygons as (
        select polygons.parcel_id
             , ST_Multi(ST_Buffer(polygons.wkb_geometry, -10)) as wkb_geometry
             , attributes.pix_10m
             , {} as crop_code
             , sum(pix_10m) over (partition by {}) as crop_pixels
             , sum(pix_10m) over () as total_pixels
             , count(*) over (partition by {}) as polygon_num
             , site_tile_geom.tile_id as tile_id
        from config,
             {} polygons
                 inner join site_tile_geom on ST_Intersects(polygons.wkb_geometry, site_tile_geom.geom)
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
          and pix_10m >= pix_min
          and (monitored_land_covers is null
            or code_n1 = any (monitored_land_covers))
          and (monitored_crops is null
            or crop_code = any (monitored_crops))
    ),
    eligible_polygons_with_attr as (
        select *,
               crop_pixels :: float / total_pixels as pixel_ratio
        from eligible_polygons
    ),
    selected_polygons as (
        select eligible_polygons_with_attr.*,
               case
                   when pix_10m < pix_best then 4
                   when pixel_ratio >= pix_ratio_hi then 1
                   when pixel_ratio >= pix_ratio_lo then 2
                   else 3
                   end as strategy
        from config,
             eligible_polygons_with_attr
        where pixel_ratio >= pix_ratio_min
          and polygon_num >= poly_min
    )
select selected_polygons.parcel_id,
       ST_AsBinary(selected_polygons.wkb_geometry),
       selected_polygons.pix_10m,
       selected_polygons.crop_code,
       selected_polygons.crop_pixels,
       selected_polygons.total_pixels,
       selected_polygons.polygon_num,
       selected_polygons.pixel_ratio,
       selected_polygons.strategy,
       selected_polygons.tile_id
from selected_polygons
-- inner join site_tile_geom on ST_Intersects(selected_polygons.wkb_geometry, site_tile_geom.geom)
order by random();
            """
        ).format(
            tile_filter,
            crop_code_column,
            crop_code_column,
            crop_code_column,
            parcels_table_id,
            attributes_table_id,
            statistical_data_id,
            remapping_set_join,
        )
        logging.debug(query.as_string(conn))

        training_pixels = defaultdict(lambda: 0)
        training_target = {}

        smote_targets = {}
        with conn.cursor() as cursor:
            if args.tiles is not None:
                query_args = (config.site_id, config.site_id, site_srid, args.tiles)
            else:
                query_args = (config.site_id, config.site_id, site_srid)

            cursor.execute(query, query_args)
            print("smote_ratio", smote_ratio)
            print("sample_ratio_lo", sample_ratio_lo)
            print("sample_ratio_hi", sample_ratio_hi)
            for (
                parcel_id,
                geometry,
                pix_10m,
                crop_code,
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

                if strategy != 4:
                    if strategy == 1:
                        crop_target = sample_ratio_hi * crop_pixels
                        if crop_code not in training_target:
                            print(
                                "Target pixels for crop {}: {}".format(
                                    crop_code, crop_target
                                )
                            )
                        training_target[crop_code] = crop_target
                    elif strategy == 2 or strategy == 3:
                        crop_target = sample_ratio_lo * crop_pixels
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
                                smote_ratio * total_pixels
                                - sample_ratio_lo * crop_pixels
                            )
                        )
                        if crop_code not in smote_targets:
                            smote_targets[crop_code] = smote_target

                    pixels = training_pixels[crop_code]
                    if pixels + pix_10m <= crop_target:
                        training_pixels[crop_code] = pixels + pix_10m
                        purpose = 0  # training
                    else:
                        purpose = 1  # validation
                else:
                    purpose = 1  # validation

                tile = tiles[tile_id]
                if purpose == 0:
                    feature = ogr.Feature(training_feature_defn)
                    feature.SetFID(parcel_id)
                    feature.SetField("id", parcel_id)
                    feature.SetField("crop_code", crop_code)
                    feature.SetField("pix_10m", pix_10m)
                    feature.SetField("strategy", strategy)
                    feature.SetGeometry(geom)

                    tile.training_layer.CreateFeature(feature)
                else:
                    feature = ogr.Feature(validation_feature_defn)
                    feature.SetFID(parcel_id)
                    feature.SetField("id", parcel_id)
                    feature.SetField("crop_code", crop_code)
                    feature.SetField("pix_10m", pix_10m)
                    feature.SetField("strategy", strategy)
                    feature.SetGeometry(geom)

                    tile.validation_layer.CreateFeature(feature)

        with open("smote-targets.json", "wt") as file:
            json.dump(smote_targets, file)

        for tile in tiles.values():
            # HACK
            tile.training_layer.SyncToDisk()
            tile.training_dataset.SyncToDisk()
            tile.validation_layer.SyncToDisk()
            tile.validation_dataset.SyncToDisk()

            tile.training_layer = None
            tile.validation_layer = None
            tile.training_dataset = None
            tile.validation_dataset = None

            training_stats = "training_statistics_{}.xml".format(tile.id)
            validation_stats = "validation_statistics_{}.xml".format(tile.id)

            tile.training_points = "training_points_{}.shp".format(tile.id)
            tile.validation_points = "validation_points_{}.shp".format(tile.id)

            command_training_statistics = [
                "otbcli_PolygonClassStatistics",
                "-field",
                "crop_code",
                "-in",
                tile.raster,
                "-vec",
                tile.training_polygons,
                "-out",
                training_stats,
            ]

            command_validation_statistics = [
                "otbcli_PolygonClassStatistics",
                "-field",
                "crop_code",
                "-in",
                tile.raster,
                "-vec",
                tile.validation_polygons,
                "-out",
                validation_stats,
            ]

            command_training_samples = [
                "otbcli_SampleSelection",
                "-field",
                "crop_code",
                "-strategy",
                "all",
                "-in",
                tile.raster,
                "-vec",
                tile.training_polygons,
                "-instats",
                training_stats,
                "-out",
                tile.training_points,
            ]

            command_validation_samples = [
                "otbcli_SampleSelection",
                "-field",
                "crop_code",
                "-strategy",
                "all",
                "-in",
                tile.raster,
                "-vec",
                tile.validation_polygons,
                "-instats",
                validation_stats,
                "-out",
                tile.validation_points,
            ]

            client = docker.from_env()
            commands = [
                command_training_statistics,
                command_validation_statistics,
            ]
            volumes = {
                output_dir: {"bind": output_dir, "mode": "rw"},
                insitu_path: {"bind": insitu_path, "mode": "ro"},
            }

            if args.mounts:
                for mount in args.mounts:
                    volumes[mount] = {"bind": mount, "mode": "ro"}

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

            commands = [
                command_training_samples,
                command_validation_samples,
            ]

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
            client.close()


if __name__ == "__main__":
    main()
