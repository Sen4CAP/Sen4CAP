#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
from configparser import ConfigParser

import dateutil.parser
import psycopg2
import psycopg2.extras


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


def save_to_csv(rows, path, headers):
    with open(path, "wb") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def extract_tile_footprints(conn, site_id, file):
    with conn.cursor() as cursor:
        query = """select shape_tiles_s2.tile_id,
        shape_tiles_s2.epsg_code,
        ST_AsText(shape_tiles_s2.geog) as geog
from shape_tiles_s2
where shape_tiles_s2.tile_id in (
    select tile_id
    from sp_get_site_tiles(%s(site_id)s :: smallint, 1 :: smallint)
);
"""
        print(query.as_string(conn))
        cursor.execute(query, {"site_id": site_id})

        save_to_csv(cursor, file, ["tile_id", "epsg_code", "geog"])
        conn.commit()


def extract_radar_products(conn, site_id, season_start, season_end, file):
    with conn.cursor() as cursor:
        query = """select
    created_timestamp :: date as date,
    site_tiles.tile_id,
    product.orbit_type_id,
    case
        when product.name like '%_VH_%' then 'VH'
        else 'VV'
    end as polarization,
    product.product_type_id,
    product.full_path
from sp_get_site_tiles(%(site_id)s :: smallint, 1 :: smallint) as site_tiles
inner join product on
    product.site_id = %(site_id)s
and product.product_type_id in (10, 11) -- s1_l2a_amp, s1_l2a_cohe
and site_tiles.tile_id = any(product.tiles)
and product.created_timestamp between %(start_date)s and %(end_date)s + interval '1 day'
order by date;
"""
        print(query.as_string(conn))
        cursor.execute(
            query,
            {
                "site_id": site_id,
                "start_date": season_start,
                "end_date": season_end,
            },
        )

        save_to_csv(
            cursor,
            file,
            [
                "dt",
                "tile_id",
                "orbit_type_id",
                "polarization",
                "radar_product_type",
                "full_path",
            ],
        )
        conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Extracts input data for the L4A processor"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument("--season-start", help="season start date")
    parser.add_argument("--season-end", help="season end date")
    parser.add_argument(
        "tile_footprints", help="output tile footprints", default="tiles.csv"
    )
    parser.add_argument(
        "radar_products", help="output radar products", default="radar.csv"
    )

    args = parser.parse_args()

    config = Config(args)

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        season_start = dateutil.parser.parse(args.season_start).date()
        season_end = dateutil.parser.parse(args.season_end).date()

        extract_tile_footprints(conn, args.site_id, args.tile_footprints)
        extract_radar_products(
            conn,
            args.site_id,
            season_start,
            season_end,
            args.radar_products,
        )


if __name__ == "__main__":
    main()
