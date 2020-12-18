#!/usr/bin/env python

import argparse
import os
import errno
import csv
import re
import pipes
import subprocess

from datetime import date, datetime

import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


EXPORT_TO_IPC_SCRIPT="extract_l4c_markers.py"

def parse_date(str):
    return datetime.strptime(str, "%Y-%m-%d").date()

 
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
        self.year = args.year
        self.new_prd_info_file = args.new_prd_info_file
        self.prds_history_files = args.prds_history_files
        self.add_no_data_rows = args.add_no_data_rows
        
        self.output = args.output
        
 
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
    
def is_int(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
        
def get_product_id_by_full_path(conn, prd_full_path):
    product_id = -1
    with conn.cursor() as cursor:
        query = SQL(
            """ select id from product where full_path = {} """
        )
        query = query.format(Literal(prd_full_path))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            product_id = row[0]
        conn.commit()
    return product_id

def get_product_id_by_name(conn, prd_name):
    product_id = -1
    with conn.cursor() as cursor:
        query = SQL(
            """ select id from product where name = {} """
        )
        query = query.format(Literal(prd_name))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            product_id = row[0]
        conn.commit()
    return product_id

def get_product_id(conn, prd_descr):
    if is_int(prd_descr):
        return prd_descr
    if os.path.isdir(prd_descr) : 
        return get_product_id_by_full_path(conn, prd_descr)
    else :
        return get_product_id_by_name(conn, prd_descr)

def get_product_path(conn, prd_id):
    product_path = ''
    with conn.cursor() as cursor:
        query = SQL(
            """ select full_path from product where id = {} """
        )
        query = query.format(Literal(prd_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            product_path = row[0]
        conn.commit()
    return product_path

def main():
    parser = argparse.ArgumentParser(description="Create a new column with aggredated values from other columns.")
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument("-y", "--year", help="year")
    parser.add_argument('-n', '--new-prd-info-file', help="File containing the id and full path of a new product")
    parser.add_argument('-f', '--prds-history-files', help="File containing the id and the full path of the previous L4C products created by scheduled jobs")
    parser.add_argument('-g', '--add-no-data-rows', required=False, type=int, default=1, help="Add also rows having all values invalid")
    parser.add_argument('-o', '--output', help="The output IPC file")
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )    

    args = parser.parse_args()
    
    config = Config(args)
    
    prd_hist_prds = []
    if os.path.isfile(args.prds_history_files) : 
        with open(args.prds_history_files) as f:
            prd_hist_prds = [line.rstrip() for line in f]    
    
    file=open( args.new_prd_info_file, "r")
    reader = csv.reader(file, delimiter=";")
    for line in reader:
        if len(line) > 0 :
            with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
                prdId = get_product_id(conn, line[0])
                prdPath = get_product_path(conn, prdId)
                prd_hist_prds.append(prdPath)
            
                command = []
                command += [EXPORT_TO_IPC_SCRIPT]
                command += ["-o", config.output]
                command += ["-g", config.add_no_data_rows]
                command += prd_hist_prds

                run_command(command)
            
                with open(args.prds_history_files, "a+") as histFile:
                    histFile.write(prdPath)            
        else :
            print ("Error reading product id and path from the file {}".format(args.product_ids_file))
            exit(1)    
        
if __name__ == '__main__':
    main()
    
    
    
    
    