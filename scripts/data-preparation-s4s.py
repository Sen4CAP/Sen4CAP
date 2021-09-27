#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
from collections import defaultdict
from datetime import date
import logging
import multiprocessing.dummy
import os
import os.path
from posixpath import dirname
from osgeo import osr
from osgeo import ogr
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
import psycopg2.extensions
import shutil
import subprocess
import sys

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


PRODUCT_TYPE_S4S_PARCELS = 14
PROCESSOR_LPIS = 8

STRATUM_TYPE_CLASSIFICATION = 1
STRATUM_TYPE_YIELD = 2

GDAL_IMAGE_NAME = "osgeo/gdal:ubuntu-full-3.3.1"
OTB_IMAGE_NAME = "sen4cap/processors:2.0.0"


def try_rm_file(f):
    try:
        os.remove(f)
        return True
    except OSError:
        return False


def try_mkdir(p):
    try:
        os.makedirs(p)
    except OSError:
        pass


class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        self.site_id = args.site_id


class RasterizeDatasetCommand(object):
    def __init__(
        self,
        input,
        output,
        tile,
        resolution,
        sql,
        field,
        srs,
        dst_xmin,
        dst_ymin,
        dst_xmax,
        dst_ymax,
    ):
        self.input = input
        self.output = output
        self.tile = tile
        self.resolution = resolution
        self.sql = sql
        self.field = field
        self.srs = srs
        self.dst_xmin = dst_xmin
        self.dst_ymin = dst_ymin
        self.dst_xmax = dst_xmax
        self.dst_ymax = dst_ymax

    def run(self):
        command = []
        command += ["gdal_rasterize", "-q"]
        command += ["-a", self.field]
        command += ["-a_srs", self.srs]
        command += ["-te", self.dst_xmin, self.dst_ymin, self.dst_xmax, self.dst_ymax]
        command += ["-tr", self.resolution, self.resolution]
        command += ["-sql", self.sql]
        command += ["-ot", "Int32"]
        command += ["-co", "COMPRESS=DEFLATE"]
        command += ["-co", "PREDICTOR=2"]
        command += [self.input, self.output]
        run_command(command)


class ComputeClassCountsCommand(object):
    def __init__(self, input, output):
        self.input = input
        self.output = output

    def run(self):
        output_dir = os.path.dirname(self.output)
        command = []
        command += ["docker", "run", "--rm"]
        command += [
            "-v",
            "{}:{}".format(
                self.input,
                self.input,
            ),
        ]
        command += [
            "-v",
            "{}:{}".format(
                output_dir,
                output_dir,
            ),
        ]
        command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
        command += [OTB_IMAGE_NAME]
        command += ["otbcli", "ComputeClassCounts"]
        command += ["-in", self.input]
        command += ["-out", self.output]
        run_command(command)


class MergeClassCountsCommand(object):
    def __init__(self, inputs, output):
        self.inputs = inputs
        self.output = output

    def run(self):
        command = []
        command += ["merge-counts"]
        command += [self.output]
        command += self.inputs
        run_command(command)


def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    logging.debug(cmd_line)
    subprocess.call(args, env=env)


def get_esri_wkt(epsg_code):
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    srs.MorphToESRI()
    return srs.ExportToWkt()


class Tile(object):
    def __init__(self, tile_id, epsg_code, tile_extent):
        self.tile_id = tile_id
        self.epsg_code = epsg_code
        self.tile_extent = tile_extent


def get_column_type(conn, schema, table, column):
    with conn.cursor() as cursor:
        query = SQL(
            """
select data_type
from information_schema.columns
where table_schema = %s
  and table_name = %s
  and column_name = %s;"""
        )
        cursor.execute(query, (schema, table, column))
        res = cursor.fetchone()
        if res:
            col_type = res[0]
        else:
            col_type = None
        return col_type


def get_table_columns(conn, schema, table):
    with conn.cursor() as cursor:
        query = SQL(
            """
select column_name
from information_schema.columns
where table_schema = %s
    and table_name = %s;"""
        )
        cursor.execute(query, (schema, table))
        cols = []
        for row in cursor:
            cols.append(row[0])
        return cols


def get_column_concat_sql(cols):
    idents = [Identifier(col) for col in cols]
    return SQL(" || '-' || ").join(idents)


def table_exists(conn, schema, name):
    with conn.cursor() as cursor:
        query = SQL(
            """
select exists (
    select *
    from pg_class
    inner join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where pg_class.relname = %s
      and pg_namespace.nspname = %s
);"""
        )
        cursor.execute(query, (name, schema))
        return cursor.fetchone()[0]


def index_exists(conn, name):
    with conn.cursor() as cursor:
        query = SQL(
            """
select exists (
    select *
    from pg_index
    inner join pg_class on pg_class.oid = pg_index.indexrelid
    where pg_class.relname = %s
);"""
        )
        cursor.execute(query, (name,))
        return cursor.fetchone()[0]


def column_exists(conn, schema, table, column):
    return get_column_type(conn, schema, table, column) is not None


def add_table_column(conn, schema, table, column, ty):
    with conn.cursor() as cursor:
        query = SQL("alter table {}.{} add column {} {}\n").format(
            Identifier(schema), Identifier(table), Identifier(column), SQL(ty)
        )
        logging.debug(query.as_string(conn))
        cursor.execute(query)


def rename_table_column(conn, schema, table, old_name, new_name):
    with conn.cursor() as cursor:
        query = SQL("alter table {}.{} rename column {} to {}\n").format(
            Identifier(schema),
            Identifier(table),
            Identifier(old_name),
            Identifier(new_name),
        )
        logging.debug(query.as_string(conn))
        cursor.execute(query)


def drop_table_columns(conn, schema, table, columns):
    with conn.cursor() as cursor:
        query = SQL("alter table {}\n").format(Identifier(table))
        to_add = []
        for column in columns:
            if get_column_type(conn, schema, table, column) is not None:
                q = SQL("drop column if exists {}").format(Identifier(column))
                to_add.append(q)
        if len(to_add) > 0:
            query += SQL(",\n").join(to_add)
            query += SQL(";")
            logging.debug(query.as_string(conn))
            cursor.execute(query)


def create_index(conn, table, columns):
    with conn.cursor() as cursor:
        name = "ix_{}_{}".format(table, "_".join(columns))
        if not index_exists(conn, name):
            cols = SQL(", ").join([Identifier(c) for c in columns])
            query = SQL("create index {} on {}({});").format(
                Identifier(name), Identifier(table), cols
            )
            logging.debug(query.as_string(conn))
            cursor.execute(query)


def drop_index(conn, table, columns):
    with conn.cursor() as cursor:
        name = "ix_{}_{}".format(table, "_".join(columns))
        query = SQL("drop index if exists {};").format(Identifier(name))
        logging.debug(query.as_string(conn))
        cursor.execute(query)


def create_spatial_index(conn, table, column):
    with conn.cursor() as cursor:
        name = "ix_{}_{}".format(table, column)
        if not index_exists(conn, name):
            query = SQL("create index {} on {} using gist({});").format(
                Identifier(name), Identifier(table), Identifier(column)
            )
            logging.debug(query.as_string(conn))
            cursor.execute(query)


def drop_spatial_index(conn, table, column):
    with conn.cursor() as cursor:
        name = "ix_{}_{}".format(table, column)
        query = SQL("drop index if exists {};").format(Identifier(name))
        logging.debug(query.as_string(conn))
        cursor.execute(query)


def create_primary_key(conn, table, columns):
    with conn.cursor() as cursor:
        name = "{}_pkey".format(table)
        if not index_exists(conn, name):
            cols = SQL(", ").join([Identifier(c) for c in columns])
            query = SQL("alter table {} add constraint {} primary key({});").format(
                Identifier(table), Identifier(name), cols
            )
            logging.debug(query.as_string(conn))
            cursor.execute(query)


def get_site_name(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL("select short_name from site where id = %s")
        cursor.execute(query, (site_id,))
        rows = cursor.fetchall()
        conn.commit()
        return rows[0][0]


def get_site_srid(conn, parcels_table):
    with conn.cursor() as cursor:
        query = SQL("select Find_SRID('public', %s, 'wkb_geometry')")
        cursor.execute(query, (parcels_table,))
        rows = cursor.fetchall()
        conn.commit()
        return rows[0][0]


def get_site_utm_epsg_codes(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select distinct
                   shape_tiles_s2.epsg_code
            from sp_get_site_tiles(%s :: smallint, 1 :: smallint) site_tiles
            inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id;"""
        )

        cursor.execute(query, (site_id,))
        rows = cursor.fetchall()

        result = []
        for (epsg_code,) in rows:
            result.append(epsg_code)

        return result


def get_site_tiles(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
select shape_tiles_s2.tile_id,
       shape_tiles_s2.epsg_code,
       ST_AsBinary(ST_SnapToGrid(ST_Transform(shape_tiles_s2.geom, shape_tiles_s2.epsg_code), 1)) as tile_extent
from sp_get_site_tiles(%s :: smallint, 1 :: smallint) site_tiles
inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id;"""
        )
        logging.debug(query.as_string(conn))
        cursor.execute(query, (site_id,))

        rows = cursor.fetchall()
        conn.commit()

        result = []
        for (tile_id, epsg_code, tile_extent) in rows:
            tile_extent = ogr.CreateGeometryFromWkb(bytes(tile_extent))
            result.append(Tile(tile_id, epsg_code, tile_extent))

        return result


class DataPreparation(object):
    DB_UPDATE_BATCH_SIZE = 1000

    def __init__(self, config, year, working_path):
        self.config = config
        self.year = year
        self.pool = multiprocessing.dummy.Pool()

        with self.get_connection() as conn:
            print("Retrieving site tiles")
            site_name = get_site_name(conn, config.site_id)
            self.tiles = get_site_tiles(conn, config.site_id)

        self.parcels_table = "in_situ_polygons_{}_{}".format(site_name, year)
        self.parcels_table_staging = "in_situ_polygons_{}_{}_staging".format(
            site_name, year
        )
        self.parcel_attributes_table = "polygon_attributes_{}_{}".format(
            site_name, year
        )
        self.statistical_data_table = "in_situ_data_{}_{}".format(site_name, year)
        self.statistical_data_table_staging = "in_situ_data_{}_{}_staging".format(
            site_name, year
        )

        insitu_path = get_insitu_path(conn, config.site_id)
        insitu_path = insitu_path.replace("{year}", str(year))
        insitu_path = insitu_path.replace("{site}", site_name)

        if not working_path:
            working_path = insitu_path
        self.site_name = site_name
        self.insitu_path = insitu_path
        self.working_path = working_path

    def get_connection(self):
        return psycopg2.connect(
            host="/var/run/postgresql",
            dbname=self.config.dbname,
        )

    def get_ogr_connection_string(self):
        return "PG:host={} dbname={}".format(
            "/var/run/postgresql",
            self.config.dbname,
        )

    def find_overlaps(self, srid, tile_counts, total):
        q = multiprocessing.dummy.Queue()
        res = self.pool.map_async(
            lambda t: self.get_overlapping_parcels(srid, q, t), self.tiles
        )

        progress = 0
        sys.stdout.write("Finding overlapping parcels: 0.00%")
        sys.stdout.flush()
        for i in range(len(self.tiles)):
            tile = q.get()
            progress += tile_counts[tile.tile_id]
            sys.stdout.write(
                "\rFinding overlapping parcels: {0:.2f}%".format(
                    100.0 * progress / total
                )
            )
            sys.stdout.flush()
        sys.stdout.write("\n")
        sys.stdout.flush()

        overlaps = list(set.union(*map(set, res.get())))
        logging.info("{} overlapping parcels".format(len(overlaps)))
        self.mark_overlapping_parcels(overlaps)

    def find_duplicates(self, srid, tile_counts, total):
        q = multiprocessing.dummy.Queue()
        res = self.pool.map_async(
            lambda t: self.get_duplicate_parcels(srid, q, t), self.tiles
        )

        progress = 0
        sys.stdout.write("Finding duplicate parcels: 0.00%")
        sys.stdout.flush()
        for i in range(len(self.tiles)):
            tile = q.get()
            progress += tile_counts[tile.tile_id]
            sys.stdout.write(
                "\rFinding duplicate parcels: {0:.2f}%".format(100.0 * progress / total)
            )
            sys.stdout.flush()
        sys.stdout.write("\n")
        sys.stdout.flush()

        duplicates = list(set.union(*map(set, res.get())))
        logging.info("{} duplicate parcels".format(len(duplicates)))
        self.mark_duplicate_parcels(duplicates)

    def prepare_strata(
        self,
        stratum_type_id,
        strata,
    ):
        if stratum_type_id == STRATUM_TYPE_CLASSIFICATION:
            stratum_type = "classification"
        elif stratum_type_id == STRATUM_TYPE_YIELD:
            stratum_type = "yield"
        else:
            raise ValueError("Unknown stratum type id", stratum_type_id)

        print("Importing {} strata".format(stratum_type))
        strata_table_staging = "strata_classification_{}_{}_staging".format(
            self.site_name, self.year
        )

        cmd = []
        cmd += ["docker", "run", "--rm"]
        cmd += [
            "-v",
            "{}:{}".format(
                os.path.dirname(os.path.realpath(strata)),
                os.path.dirname(os.path.realpath(strata)),
            ),
        ]
        cmd += ["-v", "/etc/passwd:/etc/passwd"]
        cmd += ["-v", "/etc/group:/etc/group"]
        cmd += ["-v", "/var/run/postgresql:/var/run/postgresql"]
        cmd += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
        cmd += [GDAL_IMAGE_NAME]
        cmd += ["ogr2ogr"]
        cmd += [
            "PG:host=/var/run/postgresql dbname={}".format(self.config.dbname),
            os.path.realpath(strata),
        ]
        cmd += ["-overwrite"]
        cmd += ["-lco", "UNLOGGED=YES"]
        cmd += ["-lco", "SPATIAL_INDEX=NONE"]
        cmd += ["-nln", strata_table_staging]
        cmd += ["-nlt", "POLYGON"]
        run_command(cmd)

        with self.get_connection() as conn:
            drop_table_columns(conn, "public", strata_table_staging, ["ogc_fid"])
            with conn.cursor() as cursor:
                strata_table_staging_id = Identifier(strata_table_staging)
                print("Removing old strata")
                query = SQL(
                    """
delete
from stratum
where (site_id, year, stratum_type_id) = ({}, {}, {})
"""
                ).format(
                    Literal(self.config.site_id),
                    Literal(self.year),
                    Literal(stratum_type_id),
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Copying data")
                query = SQL(
                    """
insert into stratum(site_id, year, stratum_type_id, stratum_id, wkb_geometry)
select {},
       {},
       {},
       id,
       wkb_geometry
from {}
"""
                ).format(
                    Literal(self.config.site_id),
                    Literal(self.year),
                    Literal(stratum_type_id),
                    strata_table_staging_id,
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)
                conn.commit()

                query = SQL("drop table {}").format(strata_table_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)
                conn.commit()

    def prepare_parcels_staging(
        self,
        parcels,
    ):
        ds = ogr.Open(parcels, 0)
        layer = ds.GetLayer()
        srs = layer.GetSpatialRef()
        is_projected = srs.IsProjected()
        del ds

        print("Importing parcels")
        cmd = []
        cmd += ["docker", "run", "--rm"]
        cmd += [
            "-v",
            "{}:{}".format(
                os.path.dirname(os.path.realpath(parcels)),
                os.path.dirname(os.path.realpath(parcels)),
            ),
        ]
        cmd += ["-v", "/etc/passwd:/etc/passwd"]
        cmd += ["-v", "/etc/group:/etc/group"]
        cmd += ["-v", "/var/run/postgresql:/var/run/postgresql"]
        cmd += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
        cmd += [GDAL_IMAGE_NAME]
        cmd += ["ogr2ogr"]
        cmd += [
            "PG:host=/var/run/postgresql dbname={}".format(self.config.dbname),
            os.path.realpath(parcels),
        ]
        cmd += ["-overwrite"]
        cmd += ["-lco", "SPATIAL_INDEX=NONE"]
        cmd += ["-nln", self.parcels_table_staging]
        cmd += ["-nlt", "MULTIPOLYGON"]
        run_command(cmd)

        print("Preparing parcels")
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                print("Initializing")
                parcels_table_id = Identifier(self.parcels_table)
                parcels_table_staging_id = Identifier(self.parcels_table_staging)
                parcel_attributes_table_id = Identifier(self.parcel_attributes_table)

                print("Fixing types")
                query = SQL(
                    """
alter table {}
alter column parcel_id type int,
alter column parcel_id set not null,
alter column segment_id type int;
"""
                ).format(parcels_table_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Removing old parcels")
                query = SQL("drop table if exists {};").format(parcels_table_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                query = SQL("drop table if exists {};").format(
                    parcel_attributes_table_id
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)
                conn.commit()

                drop_table_columns(
                    conn, "public", self.parcels_table_staging, ["ogc_fid"]
                )
                conn.commit()

                print("Making sure destination tables exist")
                query = SQL("create table if not exists {} (like {});").format(
                    parcels_table_id, parcels_table_staging_id
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                query = SQL(
                    """create table if not exists {} (
parcel_id int not null,
geom_valid boolean not null,
duplicate boolean,
overlap boolean not null,
area_meters real not null,
shape_index real,
multipart boolean not null,
municipality_code text,
stratum_crop_id smallint not null,
stratum_yield_id smallint not null,
pix_10m int not null default 0
);"""
                ).format(parcel_attributes_table_id, parcels_table_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)
                conn.commit()

                drop_spatial_index(conn, self.parcels_table, "wkb_geometry")

                print("Inserting parcels")
                query = SQL("insert into {} select * from {};").format(
                    parcels_table_id,
                    parcels_table_staging_id,
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                conn.commit()

                print("Filtering municipalities")
                query = SQL(
                    """
create temporary table site_municipalities as
with polygons_srid as (
         select Find_SRID('public', {}, 'wkb_geometry') as srid
     ),
     polygons_extent as (
         select ST_Extent(wkb_geometry) as extent
         from {}
     ),
     polygons_extent_4326 as (
         select ST_Transform(ST_SetSRID(polygons_extent.extent,
                                        polygons_srid.srid),
                             4326) as geog
         from polygons_extent,
              polygons_srid
     )
insert
into site_municipalities
select municipality.municipality_code,
       ST_Transform(municipality.geom, srid) as geom
from municipality,
     polygons_extent_4326,
     polygons_srid
where ST_Intersects(municipality.geom, polygons_extent_4326.geog);
"""
                ).format(Literal(self.parcels_table_staging), parcels_table_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                query = SQL("create index on site_municipalities using gist (geom);")
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Computing polygon attributes")
                if is_projected:
                    area_expr = SQL("coalesce(ST_Area(ST_MakeValid(wkb_geometry)), 0)")
                    perimeter_expr = SQL("ST_Perimeter(ST_MakeValid(wkb_geometry))")
                else:
                    area_expr = SQL(
                        "coalesce(ST_Area(ST_MakeValid(wkb_geometry) :: geography), 0)"
                    )
                    perimeter_expr = SQL(
                        "ST_Perimeter(ST_MakeValid(wkb_geometry) :: geography)"
                    )
                query = SQL(
                    """
insert into {} (
    parcel_id,
    geom_valid,
    duplicate,
    overlap,
    area_meters,
    shape_index,
    multipart,
    municipality_code,
    stratum_crop_id,
    stratum_yield_id
)
select
    parcel_id,
    coalesce(ST_IsValid(wkb_geometry), false),
    false,
    false,
    {},
    {} / (2 * sqrt(pi() * nullif({}, 0))),
    ST_NumGeometries(wkb_geometry) > 1,
    (
        select site_municipalities.municipality_code
        from site_municipalities
        where ST_Intersects(site_municipalities.geom, polygons.wkb_geometry)
        limit 1
    ),
    coalesce(
        (select stratum_id
         from stratum
         where (site_id, year, stratum_type_id) = ({}, {}, {})
           and ST_Intersects(polygons.wkb_geometry, stratum.wkb_geometry)
         limit 1)
        , 0),
    coalesce(
        (select stratum_id
         from stratum
         where (site_id, year, stratum_type_id) = ({}, {}, {})
           and ST_Intersects(polygons.wkb_geometry, stratum.wkb_geometry)
         limit 1)
        , 0)
from {} polygons;"""
                ).format(
                    parcel_attributes_table_id,
                    area_expr,
                    perimeter_expr,
                    area_expr,
                    Literal(self.config.site_id),
                    Literal(self.year),
                    Literal(STRATUM_TYPE_CLASSIFICATION),
                    Literal(self.config.site_id),
                    Literal(self.year),
                    Literal(STRATUM_TYPE_YIELD),
                    parcels_table_staging_id,
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                conn.commit()

                print("Cleaning up")
                query = SQL("drop table {};").format(parcels_table_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Creating indexes")
                create_spatial_index(conn, self.parcels_table, "wkb_geometry")
                create_primary_key(conn, self.parcels_table, ["parcel_id"])
                create_primary_key(conn, self.parcel_attributes_table, ["parcel_id"])

    def prepare_statistical_data(
        self,
        statistical_data,
    ):
        statistical_data_id = Identifier(self.statistical_data_table)
        statistical_data_staging_id = Identifier(self.statistical_data_table_staging)

        print("Importing statistical data")
        cmd = []
        cmd += ["docker", "run", "--rm"]
        cmd += [
            "-v",
            "{}:{}".format(
                os.path.realpath(statistical_data), os.path.realpath(statistical_data)
            ),
        ]
        cmd += ["-v", "/etc/passwd:/etc/passwd"]
        cmd += ["-v", "/etc/group:/etc/group"]
        cmd += ["-v", "/var/run/postgresql:/var/run/postgresql"]
        cmd += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
        cmd += [GDAL_IMAGE_NAME]
        cmd += ["ogr2ogr"]
        cmd += [
            "PG:host=/var/run/postgresql dbname={}".format(self.config.dbname),
            os.path.realpath(statistical_data),
        ]
        cmd += ["-overwrite"]
        cmd += ["-lco", "UNLOGGED=YES"]
        cmd += ["-nln", self.statistical_data_table_staging]
        cmd += ["-oo", "AUTODETECT_TYPE=YES"]
        run_command(cmd)

        print("Preparing entries")
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                add_table_column(
                    conn,
                    "public",
                    self.statistical_data_table_staging,
                    "crop_id",
                    "int",
                )

                print("Renumbering entries")
                query = SQL(
                    """update {} new
set crop_id = t.rn
from (
    select ogc_fid, row_number() over (order by ogc_fid) as rn
    from {}
) t
where new.ogc_fid = t.ogc_fid;"""
                ).format(statistical_data_staging_id, statistical_data_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                conn.commit()

                query = SQL("alter table {} alter column crop_id set not null").format(
                    statistical_data_staging_id
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                conn.commit()

                drop_table_columns(
                    conn, "public", self.statistical_data_table_staging, ["ogc_fid"]
                )
                conn.commit()

                print("Removing old parcels")
                query = SQL("drop table if exists {};").format(statistical_data_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)
                query = SQL(
                    """
create table {} (
    like {}
);
"""
                ).format(statistical_data_id, statistical_data_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                query = SQL("insert into {} select * from {};").format(
                    statistical_data_id,
                    statistical_data_staging_id,
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Dropping staging table")
                query = SQL("drop table {}").format(statistical_data_staging_id)
                logging.debug(query.as_string(conn))
                cursor.execute(query)

                print("Creating indexes")
                create_primary_key(conn, self.statistical_data_table, ["crop_id"])
                create_index(conn, self.statistical_data_table, ["parcel_id"])

                conn.commit()

    def prepare_parcels(self):
        with self.get_connection() as conn:
            print("Retrieving site SRID")
            srid = get_site_srid(conn, self.parcels_table)

        tile_counts = self.get_tile_parcel_counts(srid)
        total = 0
        for c in tile_counts.values():
            total += c
        self.find_overlaps(srid, tile_counts, total)
        self.find_duplicates(srid, tile_counts, total)

    def export_parcels(self):
        with self.get_connection() as conn:
            if not table_exists(conn, "public", self.parcels_table):
                logging.info("Parcels table does not exist, skipping export")
                return

        commands = []
        class_counts = []
        base = self.parcels_table

        with self.get_connection() as conn:
            srid = get_site_srid(conn, self.parcels_table)

            for tile in self.tiles:
                zone_srs = osr.SpatialReference()
                zone_srs.ImportFromEPSG(tile.epsg_code)

                (
                    dst_xmin,
                    dst_xmax,
                    dst_ymin,
                    dst_ymax,
                ) = tile.tile_extent.GetEnvelope()

                output = "{}_{}_10m.tif".format(base, tile.tile_id)
                output = os.path.join(self.insitu_path, output)

                sql = SQL(
                    """
with transformed as (
    select epsg_code, ST_Transform(shape_tiles_s2.geom, {}) as geom
    from shape_tiles_s2
    where tile_id = {}
)
select parcel_id, ST_Buffer(ST_Transform(wkb_geometry, epsg_code), -10)
from {}, transformed
where ST_Intersects(wkb_geometry, transformed.geom);
"""
                )
                sql = sql.format(
                    Literal(srid),
                    Literal(tile.tile_id),
                    Identifier(self.parcels_table),
                )
                sql = sql.as_string(conn)

                rasterize_dataset = RasterizeDatasetCommand(
                    self.get_ogr_connection_string(),
                    output,
                    tile.tile_id,
                    10,
                    sql,
                    "parcel_id",
                    "EPSG:{}".format(tile.epsg_code),
                    int(dst_xmin),
                    int(dst_ymin),
                    int(dst_xmax),
                    int(dst_ymax),
                )

                commands.append((rasterize_dataset, 1))

        q = multiprocessing.dummy.Queue()

        def work(w):
            (c, cost) = w
            c.run()
            q.put(cost)

        res = self.pool.map_async(work, commands)

        total = len(self.tiles)
        progress = 0
        sys.stdout.write("Rasterizing parcels: 0.00%")
        sys.stdout.flush()
        for i in range(len(commands)):
            progress += q.get()
            sys.stdout.write(
                "\rRasterizing parcels: {0:.2f}%".format(100.0 * progress / total)
            )
            sys.stdout.flush()
        sys.stdout.write("\n")
        sys.stdout.flush()

        res.get()

        commands = []
        class_counts = []
        for tile in self.tiles:
            output = "{}_{}_10m.tif".format(base, tile.tile_id)
            output = os.path.join(self.insitu_path, output)

            counts = "counts_{}.csv".format(tile.tile_id)
            counts = os.path.join(self.working_path, counts)
            class_counts.append(counts)

            compute_class_counts = ComputeClassCountsCommand(output, counts)
            commands.append((compute_class_counts, 1))

        q = multiprocessing.dummy.Queue()

        def work(w):
            (c, cost) = w
            c.run()
            q.put(cost)

        res = self.pool.map_async(work, commands)

        total = len(self.tiles)
        progress = 0
        sys.stdout.write("Counting pixels: 0.00%")
        sys.stdout.flush()
        for i in range(len(commands)):
            progress += q.get()
            sys.stdout.write(
                "\rCounting pixels: {0:.2f}%".format(100.0 * progress / total)
            )
            sys.stdout.flush()
        sys.stdout.write("\n")
        sys.stdout.flush()

        res.get()

        print("Merging pixel counts")
        counts = "counts.csv"
        counts = os.path.join(self.working_path, counts)

        merge_class_counts = MergeClassCountsCommand(class_counts, counts)
        merge_class_counts.run()

        for f in class_counts:
            try_rm_file(f)

        print("Reading pixel counts")
        class_counts = read_counts_csv(counts)

        if class_counts:
            del class_counts[0]
            updates = class_counts.items()
            del class_counts

            def update_batch(b):
                id = [e[0] for e in b]
                s2_pix = [e[1] for e in b]

                sql = SQL(
                    """update {} parcel_attributes
set pix_10m = upd.pix_10m
from (select unnest(%s) as id,
             unnest(%s) as pix_10m
     ) upd
where upd.id = parcel_attributes.parcel_id;"""
                )
                sql = sql.format(Identifier(self.parcel_attributes_table))

                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        logging.debug(sql.as_string(conn))
                        cursor.execute(sql, (id, s2_pix))
                        conn.commit()

            def work(w):
                (c, cost) = w
                c()
                q.put(cost)

            total = len(updates)
            progress = 0
            sys.stdout.flush()
            sys.stdout.write("Updating pixel counts: 0.00%")
            sys.stdout.flush()

            commands = []
            for b in batch(updates, self.DB_UPDATE_BATCH_SIZE):

                def f(b=b):
                    try:
                        update_batch(b)
                    except Exception as e:
                        print(e)

                commands.append((f, len(b)))

            res = self.pool.map_async(work, commands)

            for i in range(len(commands)):
                progress += q.get()
                sys.stdout.write(
                    "\rUpdating pixel counts: {0:.2f}%".format(100.0 * progress / total)
                )
                sys.stdout.flush()
            sys.stdout.write("\n")
            sys.stdout.flush()

            res.get()

            try_rm_file(counts)

            with conn.cursor() as cursor:
                print("Cleaning up")
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                sql = SQL("vacuum full {};").format(Identifier(self.parcels_table))
                logging.debug(sql.as_string(conn))
                cursor.execute(sql)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_DEFAULT)

                tiles = [t.tile_id for t in self.tiles]
                name = "SEN4STAT_PARCELS_S{}_{}".format(self.config.site_id, self.year)
                dt = date(self.year, 1, 1)
                sql = SQL(
                    """delete
from product
where site_id = %s
and product_type_id = %s
and created_timestamp = %s;"""
                )
                logging.debug(sql.as_string(conn))
                cursor.execute(sql, (self.config.site_id, PRODUCT_TYPE_S4S_PARCELS, dt))

                sql = SQL(
                    """
insert into product(product_type_id, processor_id, site_id, name, full_path, created_timestamp, tiles)
values(%s, %s, %s, %s, %s, %s, %s);"""
                )
                logging.debug(sql.as_string(conn))
                cursor.execute(
                    sql,
                    (
                        PRODUCT_TYPE_S4S_PARCELS,
                        PROCESSOR_LPIS,
                        self.config.site_id,
                        name,
                        self.insitu_path,
                        dt,
                        tiles,
                    ),
                )
                conn.commit()

                epsg_codes = get_site_utm_epsg_codes(conn, self.config.site_id)

                commands = []

                csv = "{}.csv".format(self.parcels_table)
                csv = os.path.join(self.insitu_path, csv)

                try_rm_file(csv)

                gpkg = "{}.gpkg".format(self.parcels_table)
                real_working_path = os.path.realpath(self.working_path)
                gpkg_working = os.path.join(real_working_path, gpkg)
                gpkg = os.path.join(self.insitu_path, gpkg)

                try_rm_file(gpkg_working)

                sql = SQL(
                    """
select parcels.parcel_id,
       parcels.segment_id,
       parcels.wkb_geometry,
       parcel_attributes.geom_valid,
       parcel_attributes.duplicate,
       parcel_attributes.overlap,
       parcel_attributes.area_meters,
       parcel_attributes.shape_index,
       parcel_attributes.multipart,
       parcel_attributes.municipality_code,
       parcel_attributes.stratum_crop_id,
       parcel_attributes.stratum_yield_id,
       parcel_attributes.pix_10m,
       statistical_data.crop_code,
       statistical_data.crop_id,
       statistical_data.harvest_date,
       statistical_data.yield_estimate,
       statistical_data.yield_method,
       statistical_data.crop_density,
       statistical_data.crop_quality,
       statistical_data.irrigated,
       statistical_data.associated,
       crop_list_n3.code_n3,
       crop_list_n2.code_n2,
       crop_list_n2.code_n1
from {} parcels
inner join {} parcel_attributes using (parcel_id)
inner join {} statistical_data using (parcel_id)
inner join crop_list_n4 on statistical_data.crop_code = crop_list_n4.code_n4
inner join crop_list_n3 using (code_n3)
inner join crop_list_n2 using (code_n2)
"""
                ).format(
                    Identifier(self.parcels_table),
                    Identifier(self.parcel_attributes_table),
                    Identifier(self.statistical_data_table),
                )
                sql = sql.as_string(conn)

                command = []
                command += ["docker", "run", "--rm"]
                command += [
                    "-v",
                    "{}:{}".format(self.insitu_path, self.insitu_path),
                ]
                command += ["-v", "/etc/passwd:/etc/passwd"]
                command += ["-v", "/etc/group:/etc/group"]
                command += ["-v", "/var/run/postgresql:/var/run/postgresql"]
                command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
                command += [GDAL_IMAGE_NAME]
                command += ["ogr2ogr"]
                command += ["-overwrite"]
                command += ["-lco", "STRING_QUOTING=IF_NEEDED"]
                command += ["-sql", sql]
                command += [csv]
                command += [
                    "PG:host=/var/run/postgresql dbname={}".format(self.config.dbname)
                ]
                commands.append((command, 5))

                srid = get_site_srid(conn, self.parcels_table)
                command = []
                command += ["docker", "run", "--rm"]
                command += [
                    "-v",
                    "{}:{}".format(real_working_path, real_working_path),
                ]
                command += ["-v", "/etc/passwd:/etc/passwd"]
                command += ["-v", "/etc/group:/etc/group"]
                command += ["-v", "/var/run/postgresql:/var/run/postgresql"]
                command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
                command += [GDAL_IMAGE_NAME]
                command += ["ogr2ogr"]
                command += ["-overwrite"]
                command += ["-a_srs", "EPSG:{}".format(srid)]
                command += ["-sql", sql]
                command += [gpkg_working]
                command += [
                    "PG:host=/var/run/postgresql dbname={}".format(self.config.dbname)
                ]
                commands.append((command, 12))

                for epsg_code in epsg_codes:
                    wkt = get_esri_wkt(epsg_code)

                    output = "{}_{}_buf_{}m.shp".format(
                        self.parcels_table, epsg_code, 10
                    )
                    output = os.path.join(self.insitu_path, output)
                    prj = "{}_{}_buf_{}m.prj".format(self.parcels_table, epsg_code, 10)
                    prj = os.path.join(self.insitu_path, prj)

                    with open(prj, "wb") as f:
                        f.write(wkt)

                    sql = SQL(
                        "select parcel_id, ST_Buffer(ST_Transform(wkb_geometry, {}), -10) from {}"
                    )
                    sql = sql.format(
                        Literal(epsg_code),
                        Identifier(self.parcels_table),
                    )
                    sql = sql.as_string(conn)

                    command = []
                    command += ["docker", "run", "--rm"]
                    command += [
                        "-v",
                        "{}:{}".format(self.insitu_path, self.insitu_path),
                    ]
                    command += ["-v", "/etc/passwd:/etc/passwd"]
                    command += ["-v", "/etc/group:/etc/group"]
                    command += ["-v", "/var/run/postgresql:/var/run/postgresql"]
                    command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
                    command += [GDAL_IMAGE_NAME]
                    command += ["ogr2ogr"]
                    command += ["-overwrite"]
                    command += ["-sql", sql]
                    command += [output]
                    command += [
                        "PG:host=/var/run/postgresql dbname={}".format(
                            self.config.dbname
                        )
                    ]
                    commands.append((command, 23))

        q = multiprocessing.dummy.Queue()

        def work(w):
            (c, cost) = w
            run_command(c)
            q.put(cost)

        res = self.pool.map_async(work, commands)

        total = sum([cost for (_, cost) in commands])
        progress = 0
        sys.stdout.write("Exporting data: 0.00%")
        sys.stdout.flush()
        for i in range(len(commands)):
            progress += q.get()
            sys.stdout.write(
                "\rExporting data: {0:.2f}%".format(100.0 * progress / total)
            )
            sys.stdout.flush()
        sys.stdout.write("\n")
        sys.stdout.flush()

        res.get()

        if self.working_path != self.insitu_path:
            print("Moving exported table")
            shutil.copy2(gpkg_working, gpkg)
            try_rm_file(gpkg_working)

    def get_tile_parcel_counts(self, srid):
        print("Counting polygons")
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = SQL(
                    """
with tiles as (
    select tile_id, ST_Transform(geom, %s) as geom
    from shape_tiles_s2
    where tile_id = any(%s)
)
select tile_id, (
    select count(*)
    from {} polygons
    where ST_Intersects(polygons.wkb_geometry, tiles.geom)
) as count
from tiles;"""
                ).format(Identifier(self.parcels_table))
                logging.debug(query.as_string(conn))
                tiles = [t.tile_id for t in self.tiles]
                cursor.execute(query, (srid, tiles))

                counts = {}
                for r in cursor:
                    counts[r[0]] = r[1]

                conn.commit()
                return counts

    def get_overlapping_parcels(self, srid, q, tile):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = SQL(
                    """
with tile as (
    select ST_Transform(geom, %s) as geom
    from shape_tiles_s2
    where tile_id = %s
)
select parcel_id
from tile, {} parcels
inner join {} parcel_attributes using (parcel_id)
where geom_valid
and exists (
    select 1
    from {} t
    inner join {} ta using (parcel_id)
    where t.parcel_id != parcels.parcel_id
    and ta.geom_valid
    and ST_Intersects(t.wkb_geometry, tile.geom)
    and ST_Intersects(t.wkb_geometry, parcels.wkb_geometry)
    having sum(ST_Area(ST_Intersection(t.wkb_geometry, parcels.wkb_geometry))) / nullif(parcel_attributes.area_meters, 0) > 0.1
)
and ST_Intersects(parcels.wkb_geometry, tile.geom);"""
                )
                parcels_table_id = Identifier(self.parcels_table)
                parcel_attributes_table_id = Identifier(self.parcel_attributes_table)
                query = query.format(
                    parcels_table_id,
                    parcel_attributes_table_id,
                    parcels_table_id,
                    parcel_attributes_table_id,
                )
                logging.debug(query.as_string(conn))
                cursor.execute(query, (srid, tile.tile_id))

                q.put(tile)
                return [r[0] for r in cursor]

    def get_duplicate_parcels(self, srid, q, tile):
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = SQL(
                    """
with tile as (
    select ST_Transform(geom, %s) as geom
    from shape_tiles_s2
    where tile_id = %s
)
select parcel_id
from (
    select parcel_id,
            count(*) over(partition by wkb_geometry) as count
    from {}, tile
    where ST_Intersects(wkb_geometry, tile.geom)
) t where count > 1;"""
                )
                query = query.format(Identifier(self.parcels_table))
                logging.debug(query.as_string(conn))
                cursor.execute(query, (srid, tile.tile_id))

                q.put(tile)
                return [r[0] for r in cursor]

    def mark_overlapping_parcels(self, parcels):
        total = len(parcels)
        if not total:
            return
        progress = 0
        sys.stdout.write("Marking overlapping parcels: 0.00%")
        sys.stdout.flush()
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for b in batch(parcels, self.DB_UPDATE_BATCH_SIZE):
                    sql = SQL("update {} set overlap = true where parcel_id = any(%s)")
                    sql = sql.format(Identifier(self.parcel_attributes_table))
                    logging.debug(sql.as_string(conn))
                    cursor.execute(sql, (b,))
                    conn.commit()

                    progress += len(b)
                    sys.stdout.write(
                        "\rMarking overlapping parcels: {0:.2f}%".format(
                            100.0 * progress / total
                        )
                    )
                    sys.stdout.flush()
                sys.stdout.write("\n")
                sys.stdout.flush()

    def mark_duplicate_parcels(self, parcels):
        total = len(parcels)
        if not total:
            return
        progress = 0
        sys.stdout.write("Marking duplicate parcels: 0.00%")
        sys.stdout.flush()
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for b in batch(parcels, self.DB_UPDATE_BATCH_SIZE):
                    sql = SQL(
                        "update {} set duplicate = true where parcel_id = any(%s)"
                    )
                    sql = sql.format(Identifier(self.parcel_attributes_table))
                    logging.debug(sql.as_string(conn))
                    cursor.execute(sql, (b,))
                    conn.commit()

                    progress += len(b)
                    sys.stdout.write(
                        "\rMarking duplicate parcels: {0:.2f}%".format(
                            100.0 * progress / total
                        )
                    )
                    sys.stdout.flush()
                sys.stdout.write("\n")
                sys.stdout.flush()


def batch(iterable, n=1):
    count = len(iterable)
    for first in range(0, count, n):
        yield iterable[first : min(first + n, count)]


def get_insitu_path(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
select value
from sp_get_parameters('processor.insitu.path')
where site_id is null or site_id = %s
order by site_id;"""
        )
        cursor.execute(query, (site_id,))

        path = cursor.fetchone()[0]
        conn.commit()

        return path


def read_counts_csv(path):
    counts = {}

    with open(path, "r") as file:
        reader = csv.reader(file)

        for row in reader:
            seq_id = int(row[0])
            count = int(row[1])

            counts[seq_id] = count

    return counts


def main():
    parser = argparse.ArgumentParser(description="Imports parcels")
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("--year", help="year", type=int, default=date.today().year)
    parser.add_argument("--parcels-geom", help="parcel dataset")
    parser.add_argument("--statistical-data", help="statistical dataset")
    parser.add_argument("--classification-strata", help="classification strata dataset")
    parser.add_argument("--yield-strata", help="yield strata dataset")
    parser.add_argument("--export", help="export dataset", action="store_true")

    required_args = parser.add_argument_group("required named arguments")
    required_args.add_argument(
        "-s", "--site-id", type=int, required=True, help="site ID to filter by"
    )
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    parser.add_argument("--working-path", help="working path")

    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level)

    config = Config(args)
    data_preparation = DataPreparation(config, args.year, args.working_path)

    try_mkdir(data_preparation.insitu_path)
    try_mkdir(data_preparation.working_path)

    if args.classification_strata:
        data_preparation.prepare_strata(
            STRATUM_TYPE_CLASSIFICATION, args.classification_strata
        )
    if args.yield_strata:
        data_preparation.prepare_strata(STRATUM_TYPE_YIELD, args.classification_strata)
    if args.parcels_geom:
        data_preparation.prepare_parcels_staging(
            args.parcels_geom,
        )
        data_preparation.prepare_parcels()
    if args.statistical_data:
        data_preparation.prepare_statistical_data(args.statistical_data)

    if args.parcels_geom or args.lut or args.export:
        data_preparation.export_parcels()


if __name__ == "__main__":
    main()
