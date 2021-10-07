#!/usr/bin/env python
from __future__ import print_function

import argparse
import multiprocessing.dummy
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import os.path
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal, Identifier
import psycopg2.extras
import subprocess
import re
import sys
import csv

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

ERR_CANNOT_DETERMINE_SEASON = 1

class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        # work around Docker networking scheme
        if self.host == "127.0.0.1" or self.host == "::1" or self.host == "localhost":
            self.host = "172.17.0.1"

        self.site_short_name = args.site_short_name
        self.year = args.year
        self.ogr2ogr_path = args.ogr2ogr_path
        self.out = args.out        

class Season(object) :
    def __init__(self, id, site_id, start_date, mid_date, end_date):
        self.id = id
        self.site_id = site_id
        self.start_date = start_date
        self.mid_date = mid_date
        self.end_date = end_date
        
def get_export_table_command(ogr2ogr_path, destination, source, *options):
    command = []
    command += [ogr2ogr_path]
    command += [destination, source]
    command += options
    return command
    
def get_srid(conn, lpis_table):
    with conn.cursor() as cursor:
        query = SQL("select Find_SRID('public', {}, 'wkb_geometry')")
        query = query.format(Literal(lpis_table))
        print(query.as_string(conn))

        cursor.execute(query)
        srid = cursor.fetchone()[0]
        conn.commit()
        return srid
   
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)

def exportMDB3PracticesFile(config, conn, pg_path, site_id, out_file):
    lpis_table = "decl_{}_{}".format(config.site_short_name, config.year)
    lut_table = "lut_{}_{}".format(config.site_short_name, config.year)
    
    srid = get_srid(conn, lpis_table)
    
    seasons = getSiteSeasons(conn, site_id)
    season = getBestSeasonForYear(site_id, seasons, config.year)
    
    default_veg_start = season.start_date.strftime('%Y-%m-%d')
    default_harvest_start = season.mid_date.strftime('%Y-%m-%d')
    default_harvest_end = season.end_date.strftime('%Y-%m-%d')
    
    with conn.cursor() as cursor:
        query = SQL(
            """
            select
                lpis."NewID" as "SEQ_ID",
                lpis.ori_id as "FIELD_ID",
                coalesce(ap.country, 'NA') as "COUNTRY",
                coalesce(ap.year, {}) as "YEAR",
                coalesce(ap.main_crop, 'NA') as "MAIN_CROP",
                coalesce(ap.veg_start, {}) as "VEG_START",
                coalesce(ap.h_start, {}) as "H_START",
                coalesce(ap.h_end, {}) as "H_END",
                coalesce(ap.practice, 'NA') as "PRACTICE",
                coalesce(ap.p_type, 'NA') as "P_TYPE",
                coalesce(ap.p_start, 'NA') as "P_START",
                coalesce(ap.p_end, 'NA') as "P_END",
                lpis."GeomValid",
                lpis."Duplic",
                lpis."Overlap",
                lpis."Area_meters" as "Area_meter",
                lpis."ShapeInd",
                lut.ctnum as "CTnum",
                lut.ct as "CT",
                lut.lc as "LC",                
                lpis."S1Pix",
                lpis."S2Pix"
            from {} lpis
            left join l4c_practices ap on lpis.ori_id = ap.orig_id and site_id = {} and year = {}
            inner join {} lut using (ori_crop)
            where not lpis.is_deleted and (lut.lc > 0 and lut.lc < 5) order by lpis."NewID" """
        )
        query = query.format( Literal(config.year), Literal(default_veg_start), Literal(default_harvest_start), Literal(default_harvest_end), 
                            Identifier(lpis_table), Literal(site_id), Literal(config.year), Identifier(lut_table))
        query_str = query.as_string(conn)
        print(query_str)

        name = os.path.basename(out_file)
        table_name = os.path.splitext(name)[0].lower()
        command = get_export_table_command(config.ogr2ogr_path, out_file, pg_path, "-nln", table_name, "-sql", query_str, "-a_srs", "EPSG:" + str(srid), "-gt", 100000)
        run_command(command)
            
def getSiteId(conn, siteShortName):
    site_id = -1
    with conn.cursor() as cursor:
        query = SQL(
            """
            select id from site
            where short_name = {}
            """
        )
        query = query.format(Literal(siteShortName))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            site_id = row[0]
        conn.commit()
    return site_id

def getSiteSeasons(conn, site_id):
    seasons = []
    with conn.cursor() as cursor:
        query = SQL(
            """
            select id, start_date, mid_date, end_date from season
            where site_id = {} order by start_date asc
            """
        )
        query = query.format(Literal(site_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            seasons.append(Season(row[0], site_id, row[1], row[2], row[3]))
        conn.commit()
    return seasons

def getBestSeasonForYear(site_id, seasons, year):
    start_year_date = datetime.strptime("{}-01-01".format(year), "%Y-%m-%d").date()
    for season in seasons:
        # this is the perfect match, start of year between start date and end date of season
        if season.start_date <= start_year_date and start_year_date <= season.end_date:
            return season
        
        if season.start_date > start_year_date and year == season.start_date.year:
            # we have the first season with the year in the same year with the start date 
            return season
    
    print("Cannot determine the season for site_id = {} and year = {}".format(site_id, year))
    return None

def getSiteConfigKey(conn, key, site_id):
    value = ''
    with conn.cursor() as cursor:
        query = SQL(
            """
            select value from config
            where key = {} and site_id = {}
            """
        )
        query = query.format(Literal(key), Literal(site_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            value = row[0]
        conn.commit()
        
        # get the default value if not found
        if value == '' :
            query = SQL(
                """ select value from config where key = {} and site_id is null """
            )
            query = query.format(Literal(key))
            print(query.as_string(conn))

            cursor.execute(query)
            for row in cursor:
                value = row[0]
            conn.commit()
        
    return value

def main():
    parser = argparse.ArgumentParser(description="Generates the Sen4CAP MDB3 input tables")
    parser.add_argument('-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location")
    parser.add_argument('-s', '--site-short-name', required=True, help="Site short name for which the input tables are extracted")
    parser.add_argument('-y', '--year', type=int, help="The year")
    parser.add_argument('-o', '--out', required=True, help="Output file name containing the input tables")
    parser.add_argument('-g', '--ogr2ogr-path', default='ogr2ogr', help="The path to ogr2ogr")
    args = parser.parse_args()

    config = Config(args)
    
    if not os.path.exists(os.path.dirname(config.out)):
        os.makedirs(os.path.dirname(config.out)) 
    
    pg_path = 'PG:dbname={} host={} port={} user={} password={}'.format(config.dbname, config.host,
                                                                        config.port, config.user, config.password)

    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        site_id = getSiteId(conn, config.site_short_name)
       
        # generate the practices files
        print ("MDB3 input tables will be generated into : ".format(config.out))
        exportMDB3PracticesFile(config, conn, pg_path, site_id, config.out)


        
if __name__ == "__main__":
    main()
