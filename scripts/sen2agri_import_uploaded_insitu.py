#!/usr/bin/env python
from __future__ import print_function

import argparse
from configparser import ConfigParser
import os
import os.path
import glob
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras
import subprocess
from shutil import copyfile
from zipfile import ZipFile

class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        self.site_id = args.site_id
        self.input = args.input_file
        
def getSiteShortName(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
            select short_name
            from site 
            where id = {}
            """
        )
        query = query.format(Literal(site_id))
        cursor.execute(query)

        results = cursor.fetchall()
        conn.commit()
        
    if results :   
        for row in results :
            return row[0]    
       
def get_insitu_path(conn, site_id):
    with conn.cursor() as cursor:
        query = SQL(
            """
select value
from sp_get_parameters('processor.l4a.reference_data_dir')
where site_id is null or site_id = %s
order by site_id;"""
        )
        cursor.execute(query, (site_id,))

        path = cursor.fetchone()[0]
        conn.commit()

        return path
       
def unzipFiles(filePath, outDir):
    with ZipFile(filePath, 'r') as zipObj:
       # Extract all the contents of zip file in different directory
       zipObj.extractall(outDir)            
       
def main():
    parser = argparse.ArgumentParser(
        description="Handles the upload of the L4A and L4B insitu data. It just unzip the provided zip to the provided directory"
    )
    parser.add_argument(
        '-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location"
    )

    parser.add_argument(
        '-s', '--site-id', default=0, type=int, help="Site ID to filter by"
    )
    
    parser.add_argument(
        "-i", "--input-file", required=True, help="The input zip file containing the shapefile with insitu data"
    )

    args = parser.parse_args()
    
    config = Config(args)
    
    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        site_short_name = getSiteShortName(conn, config.site_id)
        
        out_dir = get_insitu_path(conn, config.site_id)
        out_dir = out_dir.replace("{site}", site_name)
        
        os.makedirs(out_dir, exist_ok=True)
        
        unzipFiles(config.input, out_dir)
        
if __name__ == "__main__":
    main()
