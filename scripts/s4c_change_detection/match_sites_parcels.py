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

        self.site_id_p1 = args.site_id_p1
        self.site_id_p2 = args.site_id_p2

        self.year_p1 = args.year_p1
        self.year_p2 = args.year_p2


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
    with open(path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def extract_common_ids(config, conn, lpis_table_p1, lpis_table_p2, output_csv):
    with conn.cursor() as cursor:
        query = SQL("""
            select t1."NewID" as "NewID_ref",
                t2."NewID" as "NewID"
            from {} t1, {} t2
            where t1.ori_id = t2.ori_id 
            -- or 
            -- t1.wkb_geometry = t2.wkb_geometry
            order by t1."NewID"
            """
        )

        query = query.format(Identifier(lpis_table_p1), Identifier(lpis_table_p2))
        print(query.as_string(conn))

        cursor.execute(query)
        headers = ["NewID", "NewID_ref"]
        save_to_csv(cursor, output_csv, headers)
        conn.commit()

def main():
    parser = argparse.ArgumentParser(
        description="Extracts the mapping of common ids of two sites"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("--site-id-p1", type=int, help="the first site ID")
    parser.add_argument("--site-id-p2", type=int, help="the second site ID")
    parser.add_argument("--year-p1", type=int, help="The year for P1")
    parser.add_argument("--year-p2", type=int, help="The year for P2")
    parser.add_argument("-o", "--output", help="Output file containing the mapping ids", required=True)
    
    args = parser.parse_args()

    config = Config(args)

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        site_name_p1 = get_site_name(conn, config.site_id_p1)
        site_name_p2 = get_site_name(conn, config.site_id_p2)

        lpis_table_p1 = "decl_{}_{}".format(site_name_p1, args.year_p1)
        lpis_table_p2 = "decl_{}_{}".format(site_name_p2, args.year_p2)

        extract_common_ids(config, conn, lpis_table_p1, lpis_table_p2, args.output)


if __name__ == "__main__":
    main()
