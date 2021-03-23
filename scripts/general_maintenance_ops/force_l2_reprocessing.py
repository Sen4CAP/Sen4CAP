#!/usr/bin/env python

import argparse
import sys
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        self.product_names = []
        for prd_name in args.product_names:
            product_name = prd_name
            if "MSIL2A" in product_name:
                product_name = product_name.replace("MSIL2A", "MSIL1C")
                print("Using L1C product name {} derived from {} ...".format(product_name, prd_name))
            if "LC08_L2A_" in product_name:
                product_name = product_name.replace("LC08_L2A_", "LC08_L1TP_")

            self.product_names.append(product_name)

            
def do_reset(conn, config) :
    for product_name in config.product_names:
        with conn.cursor() as cursor:
            query = SQL(""" delete from l1_tile_history where downloader_history_id in (select id from downloader_history where product_name = {}) """).format(Literal(product_name))
            print(query.as_string(conn))
            cursor.execute(query)
            conn.commit() 
        
        with conn.cursor() as cursor:
            query = SQL(""" update downloader_history set status_id = 2 where  product_name= {} """).format(Literal(product_name))
            print(query.as_string(conn))
            cursor.execute(query)
            conn.commit() 
    
def main():

    parser = argparse.ArgumentParser(description='Forces querying for products from the beginning of the season')
    
    parser.add_argument('-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location")
    parser.add_argument('-p', '--product-names', nargs='+', help='<Required> Product names to be reprocessed (MSIL1C or MSIL2A)', required=True)
    
    args = parser.parse_args()
    
    config = Config(args)

    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        do_reset(conn, config)
                
###################################################################################################
if __name__ == '__main__':
    sys.exit(main())
