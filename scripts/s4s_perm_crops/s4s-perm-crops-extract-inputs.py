#!/usr/bin/env python
from __future__ import print_function

import argparse
import glob
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
    with open(path, "wb") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)

def save_to_file(rows, path):
    with open(path, "wb") as f:
        for row in rows:
            f.write("%s\n" % row)
        
def extract_optical_products(
    conn, site_id, satellite_id, season_start, season_end, tiles, products, file
):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select full_path,
                   unnest(tiles) as tile,
                   created_timestamp,
                   name
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
            order by created_timestamp;
            """
        )
        print(query.as_string(conn))
        cursor.execute(query)

        all_rasters = []
        for row in cursor:
            product = row[0]
            tile = row[1]
            hdr = get_tile_hdr(tile, product)
            if hdr:
                raster_b4 = get_product_rasters(hdr, 4)
                raster_b8 = get_product_rasters(hdr, 8)
                all_rasters.append(raster_b4)
                all_rasters.append(raster_b8)
      
        save_to_file(all_rasters, file)

        conn.commit()


def get_tile_hdr(tile, path):
    pat = "*_SSC_*_{}_*.HDR".format(tile)
    entries = glob.glob(os.path.join(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        entries = glob.glob(os.path.join(path, "*_SSC_*_{}_*.DBL.DIR/*.TIF".format(tile)))
        for raster_type in ["FRE", "CLD", "MSK", "QLT"]:
            for res in ["R1", "R2"]:
                pat = "_{}_{}.DBL.TIF".format(raster_type, res)
                ok = False
                for entry in entries:
                    if entry.endswith(pat):
                        ok = True
                        break
                if not ok:
                    print(
                        "No {} raster found for tile {} in {}".format(pat, tile, path)
                    )
                    return None
        return hdr

    pat = "*_T{}_*/*_MTD_ALL.xml".format(tile)
    entries = glob.glob(os.path.join(path, pat))
    print("Checking path {} with pattern {}".format(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        return hdr

    pat = "MTD_MSIL2A.xml"
    entries = glob.glob(os.path.join(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        return hdr

    print("No HDR found for tile {} in {}. It will be ignored ...".format(tile, path))
    return None

def get_product_rasters(product, band_no):
    file = os.path.basename(product)
    parts = os.path.splitext(file)
    extension = parts[1].lower()
    directory = os.path.dirname(product)
    if extension == ".xml":
        files = glob.glob(os.path.join(directory, "*_FRE_B{}.tif".format(band_no)))
        if files:
            return files[0]
        else:
            directory = os.path.join(directory, "GRANULE")
            # Check for Sen2Cor reference raster
            for subdir in os.listdir(directory):
                directory = os.path.join(directory, subdir)
                if os.path.isdir(directory):
                    directory = os.path.join(directory, "IMG_DATA")
                    directory = os.path.join(directory, "R10m")
                    break
            files = glob.glob(os.path.join(directory, "*_B0{}_10m.jp2".format(band_no)))
            if files:
                return files[0]
            else:
                files = glob.glob(os.path.join(directory, "*PENTE*"))
                if files:
                    return files[0]
                else:
                    files = glob.glob(os.path.join(directory, "*"))
                    raise Exception("Unable to find a reference raster for MAJA or SPOT product", directory, files)
    elif extension == ".hdr":
        dir = os.path.join(directory, parts[0] + ".DBL.DIR")
        files = glob.glob(os.path.join(dir, "*_FRE_R1.DBL.TIF"))
        if files:
            return files[0]
        files = glob.glob(os.path.join(dir, "*_FRE.DBL.TIF"))
        if files:
            return files[0]
        else:
            files = glob.glob(os.path.join(directory, "*"))
            raise Exception("Unable to find a reference raster for MACCS product", dir, files)

    raise Exception("Unable to determine product type", product)

def main():
    parser = argparse.ArgumentParser(
        description="Extracts input raster list for the Sen4Stat permanent crops"
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
    parser.add_argument('-o', '--out-file', default='rasters.csv', help="Output path")
    
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

        extract_optical_products(
            conn,
            args.site_id,
            1,
            season_start,
            season_end,
            args.tiles,
            args.products,
            args.out_file,
        )

if __name__ == "__main__":
    main()
