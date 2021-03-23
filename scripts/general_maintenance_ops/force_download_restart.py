#!/usr/bin/env python

import argparse
import sys
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras
import pipes
import subprocess

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

        self.site_id = args.site_id
        self.s1 = args.s1
        self.s2 = args.s2
        self.l8 = args.l8
        
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
    return

def do_reset_satellite(conn, site_id, satId) :
    with conn.cursor() as cursor:
        key = "downloader.{}.forcestart".format(satId)
        if site_id == 0:
            query = SQL(""" INSERT INTO config(key, site_id, value) VALUES ({}, null, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = true """).format(Literal(key))
        else:
            query = SQL(""" INSERT INTO config(key, site_id, value) VALUES ({}, {}, true) on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = true """).format(Literal(key), Literal(site_id))
        
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit() 

    
def do_reset(conn, config) :

    if config.s1:
        do_reset_satellite(conn, config.site_id, "S1")

    if config.s2:
        do_reset_satellite(conn, config.site_id, "S2")

    if config.l8:
        do_reset_satellite(conn, config.site_id, "L8")
    
def print_sites(conn) :
    with conn.cursor() as cursor:
        query = SQL(
            """ select id, name, short_name, enabled from site order by id asc """
        )
        cursor.execute(query)

        results = cursor.fetchall()
        conn.commit()
        
    if results :   
        for row in results :
            print(row)          
    else :
        print("Nothing to display!")
    
def main():

    parser = argparse.ArgumentParser(description='Forces querying for products from the beginning of the season')
    
    parser.add_argument('-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location")
    parser.add_argument('-p', '--print-sites', action='store_true')
    
    parser.add_argument('-s', '--site-id', default=0, type=int, help="Site ID to filter by")
    
    parser.add_argument('--s1', help='Reset S1', default=1, type=int)
    parser.add_argument('--s2', help='Reset S2', default=1, type=int)
    parser.add_argument('--l8', help='Reset L8', default=1, type=int)

    args = parser.parse_args()
    
    config = Config(args)

    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        if args.print_sites:
            print_sites(conn)
            return 1
        
        do_reset(conn, config)
        
        command = ["sudo", "systemctl", "restart", "sen2agri-services"]
        run_command(command)

                
###################################################################################################
if __name__ == '__main__':
    sys.exit(main())
