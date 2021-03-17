#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen2Agri-Processors
   Language:     Python
   Copyright:    2015-2021, CS Romania, office@c-s.ro
   See COPYRIGHT file for details.

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
_____________________________________________________________________________

"""

from __future__ import print_function
from __future__ import with_statement
from __future__ import absolute_import
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
from psycopg2.errorcodes import SERIALIZATION_FAILURE, DEADLOCK_DETECTED
from psycopg2.sql import SQL, Literal
from psycopg2 import Error, connect
from time import sleep
from random import uniform
from l2a_commons import log

DATABASE_DOWNLOADER_STATUS_DOWNLOADING_VALUE = 1
DATABASE_DOWNLOADER_STATUS_DOWNLOADED_VALUE = 2
DATABASE_DOWNLOADER_STATUS_FAILED_VALUE = 3
DATABASE_DOWNLOADER_STATUS_ABORTED_VALUE = 4
DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE = 5
DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE = 6

class DBConfig:
    def __init__(self):
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.database = None

    def connect(self):
        return connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    @staticmethod
    def load(config_file, log_dir, log_file_name):
        config = DBConfig()
        try:
            parser = ConfigParser()
            parser.read([config_file])

            config.host = parser.get("Database", "HostName")
            #for py3: config.port = int(parser.get("Database", "Port", fallback="5432"))
            int(parser.get("Database", "Port", vars={"Port": "5432"}))
            config.user = parser.get("Database", "UserName")
            config.password = parser.get("Database", "Password")
            config.database = parser.get("Database", "DatabaseName")
        except Exception as e:
            log(
                log_dir,
                 "(launcher err) <master>: Can NOT read db configuration file due to: {}".format(e),
                log_file_name
            )
        finally:
            return config

def handle_retries(conn, f, log_dir, log_file):
    nb_retries = 10
    max_sleep = 0.1

    while True:
        try:
            with conn.cursor() as cursor:
                ret_val = f(cursor)
                conn.commit()
                return ret_val
        except Error as e:
            conn.rollback()
            if (
                e.pgcode in (SERIALIZATION_FAILURE, DEADLOCK_DETECTED)
                and nb_retries > 0
            ):
                log(log_dir, "Recoverable error {} on database query, retrying".format(e.pgcode), log_file)
                sleep(uniform(0, max_sleep))
                max_sleep *= 2
                nb_retries -= 1
                continue
            raise
        except Exception as e:
            conn.rollback()
            raise

def db_get_unprocessed_tile(db_config, db_func_name, log_dir, log_file):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        q2 = SQL("select * from {}()".format(db_func_name))
        cursor.execute(q2)
        tile_info = cursor.fetchall()
        return tile_info

    with db_config.connect() as connection:
        tile_info = handle_retries(connection, _run, log_dir, log_file)
        log(log_dir, "Unprocessed tile info: {}".format(tile_info), log_file)
        if tile_info == []:
            return None
        else:
            return tile_info[0]

def db_clear_pending_tiles(db_config, db_func_name, log_dir, log_file):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        q2 = SQL("select * from {}()".format(db_func_name))
        cursor.execute(q2)
        return cursor.fetchall()

    with db_config.connect() as connection:
        (_,) = handle_retries(connection, _run, log_dir, log_file)

def db_get_site_short_name(db_config, site_id, log_dir, log_file):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        q2 = SQL("select short_name from site where id={}").format(Literal(site_id))
        cursor.execute(q2)
        cursor_ret = cursor.fetchone()
        if cursor_ret:
            (short_name,) = cursor_ret
            return short_name
        else:
            return ""

    with db_config.connect() as connection:
        short_name = handle_retries(connection, _run, log_dir, log_file)
        log(log_dir, "get_site_short_name({}) = {}".format(site_id, short_name), log_file)
        return short_name

def db_get_processing_context(db_config, processing_context, processor_name, log_dir, log_file):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        q2 = SQL("select * from sp_get_parameters('processor.{}.')".format(processor_name))
        cursor.execute(q2)
        params = cursor.fetchall()
        if params:
            return params
        else:
            return None

    with db_config.connect() as connection:
        params = handle_retries(connection, _run, log_dir, log_file)
        for param in params:
            processing_context.add_parameter(param)
        log(log_dir, "Processing context acquired.", log_file)