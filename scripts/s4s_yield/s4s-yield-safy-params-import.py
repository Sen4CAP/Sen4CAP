#!/usr/bin/env python
from __future__ import print_function

import argparse
from configparser import ConfigParser
import os
import os.path
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras
import subprocess
from shutil import copyfile

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

        self.site_id = args.site_id
        self.year = args.year


def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)


def getSiteShortName(conn, site_id):
    site_short_name = ""
    with conn.cursor() as cursor:
        query = SQL(
            """
            select short_name from site
            where id = {}
            """
        )
        query = query.format(Literal(site_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            site_short_name = row[0]
        conn.commit()
    return site_short_name

def setConfigValue(conn, site_id, key, value):
    with conn.cursor() as cursor:
        id = -1
        if not site_id:
            query = SQL(
                """ select id from config where key = {} and site_id is null"""
            ).format(Literal(key), Literal(site_id))
        else:
            query = SQL(
                """ select id from config where key = {} and site_id = {}"""
            ).format(Literal(key), Literal(site_id))
        print(query.as_string(conn))
        cursor.execute(query)
        for row in cursor:
            id = row[0]
        conn.commit()

        if id == -1:
            if not site_id:
                query = SQL(
                    """ insert into config (key, value) values ({}, {}) """
                ).format(Literal(key), Literal(value))
            else:
                query = SQL(
                    """ insert into config (key, site_id, value) values ({}, {}, {}) """
                ).format(Literal(key), Literal(site_id), Literal(value))
            print(query.as_string(conn))
            cursor.execute(query)
            conn.commit()
        else:
            if not site_id:
                query = SQL(
                    """ update config set value = {} where key = {} and site_id is null """
                ).format(Literal(value), Literal(key), Literal(site_id))
            else:
                query = SQL(
                    """ update config set value = {} where key = {} and site_id = {} """
                ).format(Literal(value), Literal(key), Literal(site_id))
            print(query.as_string(conn))
            cursor.execute(query)
            conn.commit()

        if not site_id:
            query = SQL(
                """ select value from config where key = {} and site_id is null"""
            ).format(Literal(key), Literal(site_id))
        else:
            query = SQL(
                """ select value from config where key = {} and site_id = {}"""
            ).format(Literal(key), Literal(site_id))
        print(query.as_string(conn))
        cursor.execute(query)
        read_value = ""
        for row in cursor:
            read_value = row[0]
        conn.commit()

        print("========")
        if str(value) == str(read_value):
            print(
                "Key {} succesfuly updated for site id {} with value {}".format(
                    key, site_id, value
                )
            )
        else:
            print(
                "Error updating key {} for site id {} with value {}. The read value was: {}".format(
                    key, site_id, value, read_value
                )
            )
        print("========")


def getSiteConfigKey(conn, key, site_id):
    value = ""
    with conn.cursor() as cursor:
        query = SQL(""" select value from config where key = {} and site_id = {} """)
        query = query.format(Literal(key), Literal(site_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            value = row[0]
        conn.commit()

        # get the default value if not found
        if value == "":
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


def copyConfigFile(config, conn, site_short_name, config_file):
    cfgKey = "processor.s4s_yield_feat.safy_params_path"
    cfgFile = getSiteConfigKey(conn, cfgKey, config.site_id)
    if cfgFile == "":
        print("The {} key is not configured in the database. Creating it ...".format(cfgKey))
        cfgFile = "/mnt/archive/s4s_yield/{site}/{year}/SAFY_Config/safy_params.json"
        setConfigValue(conn, None, cfgKey, cfgFile,)

    cfgFile = cfgFile.replace("{site}", site_short_name)
    cfgFile = cfgFile.replace("{year}", str(config.year))
    os.makedirs(os.path.dirname(cfgFile), exist_ok=True)
    print("Copying the SAFY config file from {} to {}".format(config_file, cfgFile))
    copyfile(config_file, cfgFile)

def main():
    parser = argparse.ArgumentParser(
        description="Handles the upload of the SAFY config file"
    )
    parser.add_argument("-c", "--config-file", default="/etc/sen2agri/sen2agri.conf", help="Configuration file location")
    parser.add_argument("-s", "--site-id", required=True, help="Site id for which the file was uploaded")
    parser.add_argument("-y", "--year", type=int, required=True, help="The year")
    parser.add_argument("-i", "--input-file", required=True, help="The uploaded config file")
    args = parser.parse_args()

    config = Config(args)

    with psycopg2.connect(
        host=config.host,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        site_short_name = getSiteShortName(conn, config.site_id)
        copyConfigFile(config, conn, site_short_name, args.input_file)


if __name__ == "__main__":
    main()
