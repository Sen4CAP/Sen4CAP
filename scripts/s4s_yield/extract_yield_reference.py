#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
import dateutil.parser
import os.path
from osgeo import gdal, ogr
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
import sys

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


def get_year(start, end):
    if start.year == end.year:
        return start.year
    d1 = start.replace(month=12, day=31) - start
    d2 = end - end.replace(month=1, day=1)
    if d2 >= d1:
        return end.year
    else:
        return start.year


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
        query = SQL(
            """
            select short_name
            from site
            where id = {}
            """
        )
        site = Literal(site_id)
        query = query.format(site)
        print(query.as_string(conn))

        cursor.execute(query)
        rows = cursor.fetchall()
        conn.commit()
        return rows[0][0]


def save_to_csv(rows, path, headers):
    with open(path, "w") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def extract_yield_estimate(config, args, insitu_table):
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        query = """
            select parcel_id, yield_estimate from {}  order by parcel_id
            """
        headers = ["NewID", "yield_estimate"]
        with conn.cursor() as cursor:
            output = args.output
            query = SQL(query).format(Identifier(insitu_table))
            print(query.as_string(conn))

            cursor.execute(query)
            save_to_csv(cursor, output, headers)
            conn.commit()



def main():
    parser = argparse.ArgumentParser(
        description="Extracts parcels yield reference"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument("-b", "--season-start", help="Season start")
    parser.add_argument("-e", "--season-end", help="Season end")
    parser.add_argument("-o", "--output", help="Output file containing reference yield")

    args = parser.parse_args()

    config = Config(args)
    
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        site_name = get_site_name(conn, config.site_id)
        season_start = dateutil.parser.parse(args.season_start)
        season_end = dateutil.parser.parse(args.season_end)

        year = get_year(season_start, season_end)
        insitu_table = "in_situ_data_{}_{}".format(site_name, year)
        
        print("Using insitu table {}".format(insitu_table))

    extract_yield_estimate(config, args, insitu_table)

if __name__ == "__main__":
    main()
