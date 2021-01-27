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
    with open(path, "wb") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)


def extract_parcels(config, args, lpis_table, lut_table, id, geom):
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        q1 = """
            select lpis."NewID",
                lpis."Area_meters",
                lpis."S1Pix",
                lpis."S2Pix",
                lut.ctnuml4a as "CTnumL4A",
                lut.ctl4a as "CTL4A",
                lut.lc
            from {} lpis
            natural join {} lut
            where "GeomValid"
            and not "Duplic"
            and not "Overlap"
            and not is_deleted
            order by "NewID"
            """
        q2 = """
            with stratum as (
                select ST_Transform(ST_GeomFromText({}, {}), Find_SRID('public', {}, 'wkb_geometry')) as geom
            )
            select lpis."NewID",
                lpis."Area_meters",
                lpis."S1Pix",
                lpis."S2Pix",
                lut.ctnuml4a as "CTnumL4A",
                lut.ctl4a as "CTL4A",
                lut.lc
            from stratum
            inner join {} lpis on lpis.wkb_geometry && stratum.geom and ST_Relate(lpis.wkb_geometry, stratum.geom, '2********')
            natural join {} lut
            where "GeomValid"
            and not "Duplic"
            and not "Overlap"
            and not is_deleted
            order by "NewID"
            """

        headers = ["NewID", "Area_meters", "S1Pix", "S2Pix", "CTnumL4A", "CTL4A", "LC"]
        with conn.cursor() as cursor:
            if args.strata is None:
                output = args.parcels
                query = SQL(q1).format(Identifier(lpis_table), Identifier(lut_table))
                print(query.as_string(conn))

                cursor.execute(query)
                save_to_csv(cursor, output, headers)
                conn.commit()
            else:
                dirname = os.path.dirname(args.parcels)
                basename = os.path.basename(args.parcels)
                split = os.path.splitext(basename)

                query = SQL(q2).format(
                    Literal(geom.ExportToWkt()),
                    Literal(args.srid),
                    Literal(lpis_table),
                    Identifier(lpis_table),
                    Identifier(lut_table),
                )

                print(query.as_string(conn))

                cursor.execute(query)
                output = os.path.join(dirname, "{}-{}{}".format(split[0], id, split[1]))
                save_to_csv(cursor, output, headers)
                conn.commit()


def extract_lut(config, args, lut_table):
    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        q = 'select distinct ctnuml4a as "CTnumL4A", ctl4a as "CTL4A" from {}'

        with conn.cursor() as cursor:
            output = args.lut
            query = SQL(q).format(Identifier(lut_table))
            print(query.as_string(conn))

            cursor.execute(query)

            headers = ["CTnumL4A", "CTL4A"]
            save_to_csv(cursor, output, headers)
            conn.commit()


def extract_tile_footprints(conn, site_id, file):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select shape_tiles_s2.tile_id,
                   shape_tiles_s2.epsg_code,
                   ST_AsText(shape_tiles_s2.geog) as geog
            from shape_tiles_s2
            where shape_tiles_s2.tile_id in (
                select tile_id
                from sp_get_site_tiles({} :: smallint, 1 :: smallint)
            );
            """
        )

        site_id_filter = Literal(site_id)
        query = query.format(site_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)

        save_to_csv(cursor, file, ["tile_id", "epsg_code", "geog"])
        conn.commit()


def extract_optical_products(
    conn, site_id, satellite_id, season_start, season_end, tiles, products, file
):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select site_id,
                   name,
                   full_path,
                   unnest(tiles) as tile,
                   created_timestamp
            from product
            where satellite_id = {}
              and product_type_id = 1
              and created_timestamp between {} and {} + interval '1 day'
              and site_id = {}
            """
        )

        satellite_filter = Literal(satellite_id)
        start_date_filter = Literal(season_start.date())
        end_date_filter = Literal(season_end.date())
        site_filter = Literal(site_id)
        query = query.format(
            satellite_filter, start_date_filter, end_date_filter, site_filter
        )

        if tiles is not None:
            tile_filter = SQL(
                """
                and tiles && {} :: character varying[]
                """
            )
            query += tile_filter.format(Literal(tiles))

        if products is not None:
            products_filter = SQL(
                """
                and name = any({})
                """
            )
            query += products_filter.format(Literal(products))

        query += SQL(
            """
            order by site_id, created_timestamp;
            """
        )
        print(query.as_string(conn))
        cursor.execute(query)

        save_to_csv(
            cursor, file, ["site_id", "name", "full_path", "tile", "created_timestamp"]
        )

        conn.commit()


def extract_radar_products(conn, site_id, season_start, season_end, tiles, products, file):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select *
            from (
                select
                    greatest(substr(split_part(product.name, '_', 4), 2), split_part(product.name, '_', 5)) :: date as date,
                    site_tiles.tile_id,
                    product.orbit_type_id,
                    split_part(product.name, '_', 6) as polarization,
                    product.product_type_id,
                    product.full_path
                from sp_get_site_tiles({} :: smallint, 1 :: smallint) as site_tiles
                inner join shape_tiles_s2 on shape_tiles_s2.tile_id = site_tiles.tile_id
                inner join product on ST_Intersects(product.geog, shape_tiles_s2.geog)
                where product.satellite_id = 3
                  and product.site_id = {}
                  {}
                  {}
            ) products
            where date between {} and {} + interval '1 day'
            order by date;
            """
        )

        site_id_filter = Literal(site_id)
        start_date_filter = Literal(season_start.date())
        end_date_filter = Literal(season_end.date())

        if products is not None:
            products_filter = SQL(
                """
                and product.name = any({})
                """
            ).format(Literal(products))
        else:
            products_filter = SQL("")

        if tiles is not None:
            tiles_filter = SQL(
                """
                and site_tiles.tile_id = any({})
                """
            ).format(Literal(tiles))
        else:
            tiles_filter = SQL("")

        query = query.format(
            site_id_filter, site_id_filter,
            products_filter,
            tiles_filter,
            start_date_filter, end_date_filter,
        )

        print(query.as_string(conn))
        cursor.execute(query)

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


def extract_lpis_path(conn, site_id, season_end, file):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select full_path
            from product
            where site_id = {}
              and product_type_id = 14
              and created_timestamp <= {}
            order by created_timestamp desc
            limit 1;
            """
        )

        query = query.format(Literal(site_id), Literal(season_end))
        print(query.as_string(conn))
        cursor.execute(query)

        path = cursor.fetchone()[0]
        conn.commit()

        with open(file, "wt") as file:
            file.write(path)


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
    parser.add_argument("--tiles", nargs="+", help="tile filter")
    parser.add_argument("--products", nargs="+", help="product filter")
    parser.add_argument("--strata", help="strata definition")
    parser.add_argument("--srid", help="strata SRID")
    parser.add_argument(
        "parcels", help="output parcels file", default="parcels.csv"
    )
    parser.add_argument("lut", help="output LUT file", default="lut.csv")
    parser.add_argument(
        "tile_footprints", help="output tile footprints", default="tiles.csv"
    )
    parser.add_argument(
        "optical_products", help="output optical products", default="optical.csv"
    )
    parser.add_argument(
        "radar_products", help="output radar products", default="radar.csv"
    )
    parser.add_argument(
        "lpis_path", help="output LPIS path file", default="lpis.txt"
    )

    args = parser.parse_args()
    dir(args)
    print(args)

    config = Config(args)

    if args.strata is not None:
        if args.srid is None:
            print("--srid is required with --strata")
            sys.exit(1)

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
        lpis_table = "decl_{}_{}".format(site_name, year)
        lut_table = "lut_{}_{}".format(site_name, year)

        extract_tile_footprints(conn, args.site_id, args.tile_footprints)
        extract_optical_products(
            conn,
            args.site_id,
            1,
            season_start,
            season_end,
            args.tiles,
            args.products,
            args.optical_products,
        )
        extract_radar_products(
            conn,
            args.site_id,
            season_start,
            season_end,
            args.tiles,
            args.products,
            args.radar_products,
        )
        extract_lpis_path(conn, args.site_id, season_end, args.lpis_path)

    if args.strata is None:
        extract_parcels(config, args, lpis_table, lut_table, None, None)
    else:
        ds = ogr.Open(args.strata, gdal.gdalconst.GA_ReadOnly)
        layer = ds.GetLayer()
        for feature in layer:
            id = feature.GetField("id")
            geom = feature.GetGeometryRef()
            extract_parcels(config, args, lpis_table, lut_table, id, geom)
    extract_lut(config, args, lut_table)


if __name__ == "__main__":
    main()
