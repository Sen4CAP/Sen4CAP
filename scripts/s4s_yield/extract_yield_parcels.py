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

def try_rm_file(f):
    try:
        os.remove(f)
        return True
    except OSError:
        return False

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

def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    logging.debug(cmd_line)
    subprocess.call(args, env=env)


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

def get_site_srid(conn, parcels_table):
    with conn.cursor() as cursor:
        query = SQL("select Find_SRID('public', %s, 'wkb_geometry')")
        cursor.execute(query, (parcels_table,))
        rows = cursor.fetchall()
        conn.commit()
        return rows[0][0]

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


class DataExtraction(object):

    def __init__(self, config, year, output):
        self.config = config
        self.year = year

        with self.get_connection() as conn:
            print("Retrieving site name")
            site_name = get_site_name(conn, config.site_id)
            print(site_name)

        self.parcels_table = "in_situ_polygons_{}_{}".format(site_name, year)
        self.parcel_attributes_table = "polygon_attributes_{}_{}".format(
            site_name, year
        )
        self.statistical_data_table = "in_situ_data_{}_{}".format(site_name, year)

        insitu_path = get_insitu_path(conn, config.site_id)
        insitu_path = insitu_path.replace("{year}", str(year))
        insitu_path = insitu_path.replace("{site}", site_name)

        self.site_name = site_name
        self.insitu_path = insitu_path
        self.output = output

    def get_connection(self):
        return psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.dbname,
            user=self.config.user,
            password=self.config.password,
        )

    def get_ogr_connection_string(self):
        return "PG:dbname={} host={} port={} user={} password={}".format(
            self.config.dbname,
            self.config.host,
            self.config.port,
            self.config.user,
            self.config.password,
        )

    def export_parcels(self):
        with self.get_connection() as conn:
            if not table_exists(conn, "public", self.parcels_table):
                logging.info("Parcels table does not exist, skipping export")
                sys.exit(1)

            try_rm_file(self.output)

            sql = SQL(
                """
select parcels.parcel_id,
    parcels.wkb_geometry,
   parcel_attributes.geom_valid,
   statistical_data.crop_code,
   statistical_data.crop_id
from {} parcels
inner join {} parcel_attributes using (parcel_id)
inner join {} statistical_data using (parcel_id)
where parcel_attributes.geom_valid = true

"""
            ).format(
                Identifier(self.parcels_table),
                Identifier(self.parcel_attributes_table),
                Identifier(self.statistical_data_table),
            )
            sql = sql.as_string(conn)
            
            srid = get_site_srid(conn, self.parcels_table)
            command = []
            command += ["ogr2ogr"]
            command += ["-overwrite"]
            command += ["-a_srs", "EPSG:{}".format(srid)]
            command += ["-sql", sql]
            command += [self.output]
            command += [ self.get_ogr_connection_string() ]
            
            run_command(command)

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



def main():
    parser = argparse.ArgumentParser(description="Export parcels for yield data extraction")
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("-y", "--year", help="year", type=int, default=date.today().year)
    parser.add_argument(
        "-s", "--site-id", type=int, required=True, help="site ID to filter by"
    )
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    parser.add_argument("-w", "--working-path", help="working path")
    parser.add_argument("-o", "--output", help="the output gpkg file")

    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level)

    config = Config(args)
    data_extraction = DataExtraction(config, args.year, args.output)

    data_extraction.export_parcels()


if __name__ == "__main__":
    main()
