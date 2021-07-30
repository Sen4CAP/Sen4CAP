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
import re
import fnmatch

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


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

class RadarProduct(object):
    def __init__(self, name, orbit_type_id, polarization, radar_product_type, path, orbit_id):
        self.name = name
        self.orbit_type_id = orbit_type_id
        self.polarization = polarization
        self.radar_product_type = radar_product_type
        self.path = path
        self.orbit_id = orbit_id
        
    def to_csv_line(self) :
        # "dt", "name", "orbit_type_id", "orbit_id", "polarization", "radar_product_type", "full_path"
        return [self.name, self.orbit_type_id, self.orbit_id, self.polarization, self.radar_product_type, self.path]

class L3BProductFile(object):
    def __init__(self, tile_id, path):
        self.tile_id = tile_id
        self.path = path
        
    def to_csv_line(self) :
        # "tile", "full_path"
        return [self.tile_id, self.path]

def save_to_csv(products, path, headers):
    with open(path, "wb") as csvfile:
        print("Writing to file {} a number of {} entries ... ".format(path, len(products)))
        writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        # writer.writerows(products)
        for row in products:
           writer.writerow(row.to_csv_line())


def extract_s1_products(conn, site_id, season_start, season_end, prds_list):

    prds_names_list=[]
    if prds_list is not None and len(prds_list) > 0 :
        print ("Extracting S1 infos from the database only for the list of products!!!")
        for prd in prds_list:
            prds_names_list.append(os.path.splitext(os.path.basename(prd))[0])
        if len (prds_names_list) > 1:
            prdsSubstr = tuple(prds_names_list)
        else :
            # prdsSubstr = "('{}')".format(prds_names_list[0])
            prdsSubstr = tuple(prds_names_list,)
    else :
        print ("Extracting S1 infos from the database for the whole season interval!!!")
        
    with conn.cursor() as cursor:
        query = SQL(
            """
            with products as (
                select product.site_id,
                    product.name,
                    case
                        when substr(split_part(product.name, '_', 4), 2, 8) > substr(split_part(product.name, '_', 5), 1, 8) then substr(split_part(product.name, '_', 4), 2, 8)
                        else substr(split_part(product.name, '_', 5), 1, 8)
                    end :: date as date,
                    coalesce(product.orbit_type_id, 1) as orbit_type_id,
                    split_part(product.name, '_', 6) as polarization,
                    product.processor_id,
                    product.product_type_id,
                    substr(product.name, length(product.name) - strpos(reverse(product.name), '_') + 2) as radar_product_type,
                    product.orbit_id,
                    product.full_path
                from product where product.satellite_id = 3 {}
            )
            select products.date,
                    products.name,
                    products.orbit_type_id,
                    products.polarization,
                    products.radar_product_type,
                    products.full_path, 
                    products.orbit_id
            from products
            {}
            order by date;
            """
        )
        if prds_list is None or len(prds_list) == 0 :
            product_filter = SQL(
                """
                    and product.site_id = {}
                """
            ).format(Literal(site_id))
            
            season_filter = SQL(
                """
                    where date between {} and {} + interval '1 day'
                """
            ).format(Literal(season_start), Literal(season_end))
        else:
            product_filter = SQL(
                """
                    and product.name in {}
                """
            ).format(Literal(prdsSubstr))
            
            season_filter = SQL("")

        query = query.format(product_filter, season_filter)
        # print(query.as_string(conn))
        
        # execute the query
        cursor.execute(query)            
            
        results = cursor.fetchall()
        conn.commit()

        products = []
        # We are performing this search to have a warning on not present products but also to have the same order of products as in the inputs
        if len(prds_names_list) > 0 :
            for i in range(len(prds_names_list)):
                prd_name = prds_names_list[i]
                prd = prds_list[i]
                prdAdded = False
                for (dt, name, orbit_type_id, polarization, radar_product_type, full_path, orbit_id) in results:
                    if os.path.splitext(os.path.basename(prd_name))[0] == name : 
                        products.append(RadarProduct(name, orbit_type_id, polarization, radar_product_type, os.path.normpath(prd), orbit_id))
                        prdAdded = True
                        break
                if prdAdded == False :
                    print ("Product {} was not found in the database!!!".format(prd))
        else :
            for (dt, name, orbit_type_id, polarization, radar_product_type, full_path, orbit_id) in results:
                products.append(RadarProduct(name, orbit_type_id, polarization, radar_product_type, full_path, orbit_id))

        return products

def get_s2_products_from_tiffs(input_products_list):
    products = []
    l3b_file_regex = "(S2AGRI_L3B_([A-Z]{5,11})_A([0-9]{8})T([0-9]{6})_T([0-9]{2}[A-Z]{3})\.)"
    regex = re.compile(l3b_file_regex)
    for tifFilePath in input_products_list:
        tifFileName = os.path.basename(tifFilePath)
        result = regex.match(tifFileName)
        if result and len(result.groups()) == 5:
            dt_str = "{}T{}".format(result.group(3), result.group(4))
            dt = datetime.datetime.strptime(dt_str, '%Y%m%dT%H%M%S')
            products.append(NdviProduct(dt, result.group(5), tifFilePath))
    
    return products

def extract_l3b_products_files(conn, site_id, season_start, season_end, prds_are_tif, input_products_list):
    with conn.cursor() as cursor:
        if input_products_list is not None and len(input_products_list) > 0 :
            print ("prds_are_tif = {}".format(prds_are_tif))
            print ("Extracting NDVI infos from the database only for the list of products!!!")
            if prds_are_tif == 1 : 
                print ("Extracting NDVI infos from TIF files!!!")            
                return get_s2_products_from_tiffs(input_products_list)
            
            # Otherwise, get the products from DB and extract the tiffs
            if len (input_products_list) > 1:
                prdsSubstr = tuple(input_products_list)
            else :
                #prdsSubstr = "({})".format(input_products_list[0])
                prdsSubstr = tuple(input_products_list,)
        else:
            print ("Extracting NDVI infos from the database for the whole season interval!!!")
            
        query = SQL(
            """
            with products as (
                select product.site_id,
                    product.name,
                    product.created_timestamp as date,
                    product.processor_id,
                    product.product_type_id,
                    product.full_path,
                    product.tiles
                from product
                where product.site_id = {} and product.product_type_id = 3 {}
            )
            select products.date,
                    products.tiles,
                    products.full_path
            from products
            {}
            order by date;
            """
        )

        if input_products_list is None or len(input_products_list) == 0 :
            product_filter = SQL("")
            season_filter = SQL(
                """
                    where date between {} and {} + interval '1 day'
                """
            ).format(Literal(season_start), Literal(season_end))
        else:
            product_filter = SQL(
                """
                    and product.full_path in {}
                """
            ).format(Literal(prdsSubstr))
            
            season_filter = SQL("")
            
        query = query.format(Literal(site_id), product_filter, season_filter)
        # print(query.as_string(conn))
        
        # execute the query
        cursor.execute(query)            
            
        results = cursor.fetchall()
        conn.commit()

        products = []
        for (dt, tiles, full_path) in results:
            # print ("Handling product : {} with tiles {} ...".format(full_path, tiles))
            tilesPath = os.path.join(full_path, "TILES")
            try:
                tilesDirs = os.listdir(tilesPath)
            except:
                print("Product {} found in DB but not on disk".format(full_path))
                continue       
                
            for tile in tiles :   
                # print ("Handling tile {} for product : {} ...".format(tile, full_path))
                # print ("TILE DIRS: {} ...".format(tilesDirs))
                
                # Ignore the L8 tiles
                if re.match("\d{6}", tile) :
                    print ("Ignoring L8 tile {}".format(tile))
                    continue
                tilePaths = fnmatch.filter(tilesDirs, "S2AGRI_L3B_A*_T{}".format(tile))
                if len(tilePaths) == 1:
                    subPath = tilePaths[0]
                    fullTilePath = os.path.join(tilesPath, subPath)
                    tileImgDataPath = os.path.join(fullTilePath, "IMG_DATA")
                    try:
                        tileDirFiles = os.listdir(tileImgDataPath)
                    except:
                        print("Expected L3B product structure found but the path {} does not exists".format(tileImgDataPath))
                        continue
                    prdFiles = fnmatch.filter(tileDirFiles, "S2AGRI_L3B_S*_A*_T{}.TIF".format(tile))
                    for prdFile in prdFiles:
                        # print ("Using product tif: {} ...".format(os.path.join(tileImgDataPath, prdFile)))
                        products.append(L3BProductFile(tile, os.path.join(tileImgDataPath, prdFile)))
                
        return products

def main():
    parser = argparse.ArgumentParser(
        description="Extracts radar input data for the S4C L4B processor"
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
    parser.add_argument("--s1-products", nargs="+", help="S1 product filter")
    parser.add_argument("--l3b-products", nargs="+", help="L3B product filter")
    parser.add_argument("--out-l3b-products-file", help="output optical products", default="")
    parser.add_argument("--out-s1-products-file", help="output radar products", default="")
    parser.add_argument('--l3b-prds-are-tif', help="Specify that the L3B products in the l3b-products are actually the TIF files", type=int, default="0")

    args = parser.parse_args()

    if args.out_s1_products_file == "" and args.out_l3b_products_file == "" :
        print("Please provide at least one of the parameters out_l3b_products_file or out_s1_products_file")
        return 1
    
    season_start = None
    season_end = None
    if args.season_start and args.season_end : 
        season_start = dateutil.parser.parse(args.season_start)
        season_end = dateutil.parser.parse(args.season_end)

    config = Config(args)

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        if args.out_s1_products_file != "" :
            products = extract_s1_products(conn, config.site_id, season_start, season_end, args.s1_products)
            save_to_csv(
                    products,
                    args.out_s1_products_file, 
                    [
                        "name",
                        "orbit_type_id",
                        "orbit_id",
                        "polarization",
                        "radar_product_type",
                        "full_path",
                    ],
                )
        if args.out_l3b_products_file != "" :
            products = extract_l3b_products_files(conn, config.site_id, season_start, season_end, args.l3b_prds_are_tif, args.l3b_products)
            save_to_csv(
                    products,
                    args.out_l3b_products_file, 
                    [
                        "tile",
                        "full_path",
                    ],
                )

if __name__ == "__main__":
    main()
