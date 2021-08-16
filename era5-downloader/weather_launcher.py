#!/usr/bin/env python

import os
import pipes
import signal
import errno
from datetime import datetime, timedelta
import argparse
from psycopg2.sql import SQL
from db_commons import DBConfig, handle_retries
from db_commons import DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE, DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE
from l2a_commons import LogHandler, run_command, stop_containers, get_guid, get_docker_gid, create_recursive_dirs
from l2a_commons import MASTER_ID
from osgeo import ogr

OUTPUT_DIR = "/mnt/archive/{site_name}/era5_weather"
CONTAINER_IMAGE = "sen4x/era5-weather:0.0.1"
SCRIPT_PATH = "/usr/share/weather/weather.py"
WRK_DIR = "/mnt/archive/{site_name}/era5_weather/working_dir/"
LAUNCHER_LOG_DIR = "/var/log/sen2agri/"
LAUNCHER_LOG_FILE_NAME = "weather_launcher.log"
DATE_FORMAT = "%Y%m%d"
OUTPUT_FILE_NAME = "weather_{date}.nc"
WEATHER_PRODUCT_ID = 29
WEATHER_PROCESSOR_ID = 18
VALID_PROCESSING = 0 
ERR_ERA5_DOWNLOAD = 2 #era5 file from cds can NOT be downloaded
ERR_DAILY_DATA_PROCESSING = 3 #when computing the daily average there are some errors
ERR_DAILY_DATA_WRITING = 4 #when writing the daily average data to file there are some errors
ERR_INVALID_PARAMETERS = 5 #command line parameters are wrong
MAX_NB_RETRIES = 3


class SiteContext:
    def __init__(self, site_info, log):
        self.log = log
        self.is_valid = self.check_site_info(site_info)
        if self.is_valid:
            self.site_id = site_info[0]
            self.geog = site_info[1]
            print("1 :{}".format(site_info[1]))
            geom = ogr.CreateGeometryFromWkt(self.geog)
            self.envelope = geom.GetEnvelope()
            self.xmin = self.envelope[0]
            self.xmax = self.envelope[1]
            self.ymin = self.envelope[2]
            self.ymax = self.envelope[3]
            self.short_name = site_info[2]
            self.start_date = site_info[3].strftime(DATE_FORMAT)
            self.end_date = site_info[4].strftime(DATE_FORMAT)
            self.footprint = self.compute_footprint()
        else:
            self.log.error("Invalid site information", print_msg = True)

    def check_site_info(self, site_info):
        if len(site_info) != 5:
            return False
        for info in site_info:
            if info is None: return False
        return True

    def print_info(self):
        self.log.info("Site id: {}".format(self.site_id), print_msg = True)
        self.log.info("Short name: {}".format(self.short_name), print_msg = True)
        self.log.info("Geog: {}".format(self.geog), print_msg = True)
        self.log.info("Envelope: {}".format(self.envelope), print_msg = True)
        self.log.info("Footprint: {}".format(self.footprint), print_msg = True)
        self.log.info("Start date: {}".format(self.start_date), print_msg = True)
        self.log.info("End date: {}".format(self.end_date), print_msg = True)
        self.log.info("Xmax {} Xmin {}".format(self.xmax, self.xmin), print_msg = True)
        self.log.info("Ymax {} Ymin {}".format(self.ymax, self.ymin), print_msg = True)
        
    def compute_footprint(self):
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint_2D(self.xmin, self.ymin)
        ring.AddPoint_2D(self.xmin, self.ymax)
        ring.AddPoint_2D(self.xmax, self.ymax)
        ring.AddPoint_2D(self.xmax, self.ymin)
        ring.AddPoint_2D(self.xmin, self.ymin) 
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        return poly.ExportToWkt()

def process_date(site_context, date, log):
    docker_gid = get_docker_gid()
    script_command = []
    #docker run
    script_command.append("docker")
    script_command.append("run")
    script_command.append("--rm")
    script_command.append("-u")
    script_command.append("{}:{}".format(os.getuid(), os.getgid()))
    script_command.append("--group-add")
    script_command.append("{}".format(docker_gid))
    script_command.append("-v")
    script_command.append("/etc/localtime:/etc/localtime")
    script_command.append("-v")
    script_command.append("/usr/share/zoneinfo:/usr/share/zoneinfo")
    script_command.append("-v")
    script_command.append("{}:{}".format("/var/lib/cdsapi","/var/lib/cdsapi"))
    site_wrk_dir = WRK_DIR.replace("{site_name}", site_context.short_name)
    script_command.append("-v")
    script_command.append("{}:{}".format(site_wrk_dir, site_wrk_dir))

    site_output_dir = OUTPUT_DIR.replace("{site_name}",site_context.short_name)
    script_command.append("-v")
    script_command.append("{}:{}".format(site_output_dir, site_output_dir))
    guid = get_guid(8)
    container_name = "weather-{}-{}-{}".format(site_context.short_name, date.strftime(DATE_FORMAT) ,guid)
    script_command.append("--name")
    script_command.append(container_name)
    script_command.append(CONTAINER_IMAGE)

    #actual weather.py command
    script_command.append(SCRIPT_PATH)
    script_command.append("-date")
    script_command.append(date.strftime(DATE_FORMAT))
    script_command.append("-wrk")
    script_command.append(site_wrk_dir)
    script_command.append("-out")
    script_command.append(site_output_dir)
    script_command.append("-xmin")
    script_command.append(str(site_context.xmin))
    script_command.append("-xmax")
    script_command.append(str(site_context.xmax))
    script_command.append("-ymin")
    script_command.append(str(site_context.ymin))
    script_command.append("-ymax")
    script_command.append(str(site_context.ymax))

    cmd_str = " ".join(map(pipes.quote, script_command))
    log.info("Running command: {}".format(cmd_str))
    running_containers.append(container_name)
    command_return = run_command(script_command, log)
    running_containers.remove(container_name)

    return command_return

def signal_handler(signum, frame):
    launcher_log.info("Signal caught: {}.".format(signum), print_msg = True)
    stop_containers(running_containers, launcher_log)
    os._exit(0)

def db_get_enabled_sites(db_config, log):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)
        cursor.execute("""select site.id, ST_AsText(geog), site.short_name , season.start_date, season.end_date from site inner join season on site.id = season.site_id where site.enabled and ((select config.value = 'true' from config where config.site_id is null and config.key='processor.era5_weather.enabled') or site.id in (select config.site_id from config where config.key = 'processor.era5_weather.enabled' and config.value = 'true'));""")
        enabled_sites = cursor.fetchall()
        return enabled_sites

    with db_config.connect() as connection:
        enabled_sites = handle_retries(connection, _run, log)
        return enabled_sites

def db_invalid_product_update(db_config, site_id, product_name, full_path, status_id, product_date, status_reason, nb_retries, dh_id, log):
    def _run(cursor):   
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)

        # update product table
        if dh_id is None:
            cursor.execute(
                    """insert into downloader_history (site_id, product_name, full_path, status_id, product_date, status_reason, product_type_id) values (%(site_id)s :: smallint,
                                                                                                                        %(product_name)s :: character varying,
                                                                                                                        %(full_path)s :: character varying,
                                                                                                                        %(status_id)s :: smallint,
                                                                                                                        %(product_date)s ::timestamp,
                                                                                                                        %(status_reason)s :: character varying,
                                                                                                                        %(product_type_id)s :: smallint)  RETURNING id""",
                    {
                        "site_id": site_id, 
                        "product_name": product_name,
                        "full_path": full_path,  
                        "status_id": status_id, 
                        "product_date": product_date,
                        "status_reason": status_reason, 
                        "product_type_id": WEATHER_PRODUCT_ID
                    }

                )
        else:
            cursor.execute(
                """update downloader_history set no_of_retries = %(nb_retries)s, status_id = %(status_id)s, status_reason = %(status_reason)s where id = %(dh_id)s """,
                {
                    "nb_retries" : nb_retries,
                    "status_id" : status_id,
                    "status_reason" : status_reason,
                    "dh_id" : dh_id
                }
            )

    with db_config.connect() as connection:
        handle_retries(connection, _run, log)

def db_valid_product_update(db_config, site_id, full_path, product_name, footprint, status_id, product_date, nb_retries, dh_id, log):
    def _run(cursor):   
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)

        # update downloader history table
        if dh_id is None:
            cursor.execute(
                    """insert into downloader_history (site_id, product_name, full_path, status_id, footprint, product_date, product_type_id) values (%(site_id)s :: smallint,
                                                                                                                        %(product_name)s :: character varying,
                                                                                                                        %(full_path)s :: character varying,
                                                                                                                        %(status_id)s :: smallint,
                                                                                                                        %(footprint)s,
                                                                                                                        %(product_date)s ::timestamp,
                                                                                                                        %(product_type_id)s :: smallint)  RETURNING id""",
                    {
                        "site_id": site_id, 
                        "product_name": product_name, 
                        "full_path": full_path, 
                        "status_id": status_id, 
                        "footprint": footprint,
                        "product_date": product_date,
                        "product_type_id": WEATHER_PRODUCT_ID
                    }

                )
            downloader_history_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """update downloader_history set no_of_retries = %(nb_retries)s, status_id = %(status_id)s where id = %(dh_id)s """,
                {
                    "nb_retries" : nb_retries,
                    "status_id" : status_id,
                    "dh_id" : dh_id
                }
            )
            downloader_history_id = dh_id
        # update product table
        cursor.execute(
                   """select * from sp_insert_product(%(product_type_id)s :: smallint,
                                   %(processor_id)s :: smallint,
                                   %(satellite_id)s :: smallint,
                                   %(site_id)s :: smallint,
                                   %(job_id)s :: smallint,
                                   %(full_path)s :: character varying,
                                   %(created_timestamp)s :: timestamp,
                                   %(name)s :: character varying,
                                   %(quicklook_image)s :: character varying,
                                   %(footprint)s,
                                   %(orbit_id)s :: integer,
                                   %(tiles)s :: json,
                                   %(orbit_type_id)s :: smallint,
                                   %(downloader_history_id)s :: integer)""",
                                    {
                                        "product_type_id" : WEATHER_PRODUCT_ID,
                                        "processor_id" : WEATHER_PROCESSOR_ID,
                                        "satellite_id" : None,
                                        "site_id" : site_id,
                                        "job_id" : None,
                                        "full_path" : full_path,
                                        "created_timestamp" : product_date,
                                        "name" : product_name,
                                        "quicklook_image" : None,
                                        "footprint" : footprint,
                                        "orbit_id" : None,
                                        "tiles" : None,
                                        "orbit_type_id" : None,
                                        "downloader_history_id" : downloader_history_id
                                    }
            )

    with db_config.connect() as connection:
        handle_retries(connection, _run, log)

def db_product_entry_exists(db_config, site_id, name, log):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)

        #check product
        cursor.execute("""select id from product where product.site_id = %(site_id)s :: smallint and product_type_id = %(product_type_id)s :: smallint  and name = %(name)s :: character varying;""",
            {
               "site_id" : site_id,
               "product_type_id" : WEATHER_PRODUCT_ID,
               "name" : name, 
            }
        )
        if cursor.fetchone() is None:
            product_exists = False
        else:
            product_exists = True
        return product_exists
        

    with db_config.connect() as connection:
        product_exists = handle_retries(connection, _run, log)
        return product_exists

def db_downloader_history_entry_exists(db_config, site_id, name, log):
    def _run(cursor):
        q1 = SQL("set transaction isolation level serializable")
        cursor.execute(q1)


        #check product
        cursor.execute("""select id, no_of_retries, status_id from downloader_history where downloader_history.site_id = %(site_id)s :: smallint and downloader_history.product_name = %(product_name)s :: character varying;""",
            {
               "site_id" : site_id,
               "product_name" : name, 
            }
        )
        result = cursor.fetchone()
        if result is None:
            return None, None, None
        else:
            return result[0], result[1], result[2]

    with db_config.connect() as connection:
        id, no_retries, status = handle_retries(connection, _run, log)
        return id, no_retries, status


parser = argparse.ArgumentParser(description="Launcher for Weather script")
parser.add_argument('-c', '--config', default="/etc/sen2agri/sen2agri.conf", help="configuration file")
parser.add_argument('-l', '--log-level', default = 'info',
                    choices = ['debug' , 'info', 'warning' , 'error', 'critical'], 
                    help = 'Minimum logging level')
args = parser.parse_args()


launcher_log_path = os.path.join(LAUNCHER_LOG_DIR, LAUNCHER_LOG_FILE_NAME)
launcher_log = LogHandler(launcher_log_path, "launcher_log", args.log_level, MASTER_ID)
db_config = DBConfig.load(args.config, launcher_log)
running_containers = []
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

#determine the sites enabled for processings
enabled_sites = db_get_enabled_sites(db_config, launcher_log)
launcher_log.info("Nb. of sites enabled: {}".format(str(len(enabled_sites))), print_msg=True)
if len(enabled_sites) > 0:
    for site in enabled_sites:
        site_context = SiteContext(site, launcher_log) #parse the site info obtained from the db
        if site_context.is_valid: #if the site infomration is valid proceed
            site_context.print_info()
            site_output_dir = OUTPUT_DIR.replace("{site_name}",site_context.short_name)
            if not create_recursive_dirs(site_output_dir):
                launcher_log.critical("Can NOT create site output dir: {}".format(site_output_dir), print_msg = True)
                os._exit(1)
            site_wrk_dir = WRK_DIR.replace("{site_name}", site_context.short_name)
            if not create_recursive_dirs(site_wrk_dir):
                launcher_log.critical("Can NOT create site wrk dir: {}".format(site_wrk_dir), print_msg = True)
                os._exit(1)
            site_log_path = os.path.join(site_output_dir, "weather.log")
            launcher_log.info("Site processing console can be found at: {}".format(site_log_path), print_msg=True)
            start_date = datetime.strptime(site_context.start_date, DATE_FORMAT)
            end_date = datetime.strptime(site_context.end_date, DATE_FORMAT)
            delta = end_date - start_date
            for i in range(delta.days + 1):
                date = start_date + timedelta(days=i)
                #check if the product already exists
                daily_data_file_name = OUTPUT_FILE_NAME.replace("{date}", date.strftime(DATE_FORMAT))
                daily_data_file_path = os.path.join(site_output_dir, daily_data_file_name)
                if not db_product_entry_exists(db_config, site_context.site_id, daily_data_file_name, launcher_log):    
                    dh_id, dh_retries, dh_status = db_downloader_history_entry_exists(db_config, site_context.site_id, daily_data_file_name, launcher_log)
                    if dh_id is None:
                        # initial processing of an era5 product
                        nb_retries = 0
                        do_process = True
                    else:        
                        # reprocessing of an era5 product            
                        if dh_status != DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE and dh_retries < MAX_NB_RETRIES:
                            # product was processed previously with an error and the number of retries is small than 3
                            nb_retries = dh_retries + 1
                            do_process = True
                        else:
                            # product was processed previously 3 more times, no additional reprocessing will be done
                            nb_retries = dh_retries
                            do_process = False
                    if do_process:
                        processing_return_code = process_date(site_context, date, launcher_log)
                        if (processing_return_code == VALID_PROCESSING) and (os.path.isfile(daily_data_file_path)):
                            db_valid_product_update(
                                db_config,
                                site_context.site_id,
                                daily_data_file_path,
                                daily_data_file_name,
                                site_context.footprint,
                                DATABASE_DOWNLOADER_STATUS_PROCESSED_VALUE,
                                date.strftime(DATE_FORMAT),
                                nb_retries,
                                dh_id, 
                                launcher_log
                            )
                        else:
                            if processing_return_code == ERR_ERA5_DOWNLOAD:
                                rejection_reason = "Can NOT download Era5 data."
                            elif processing_return_code == ERR_DAILY_DATA_PROCESSING:
                                rejection_reason = "Can NOT process Era5 data."
                            elif processing_return_code == ERR_DAILY_DATA_WRITING:
                                rejection_reason = "Can NOT write daily computed data to file."
                            else:
                                rejection_reason = "Unknown Error: {}.".format(processing_return_code)
                            launcher_log.error(rejection_reason, print_msg=True)
                            db_invalid_product_update(
                                db_config,
                                site_context.site_id,
                                daily_data_file_name,
                                daily_data_file_path,
                                DATABASE_DOWNLOADER_STATUS_PROCESSING_ERR_VALUE,
                                date.strftime(DATE_FORMAT),
                                rejection_reason, 
                                nb_retries,
                                dh_id,
                                launcher_log
                            ) 
                    else:
                        launcher_log.info("Era5 product with downloader history id {} was already processed {} which is >= than the maximum allowed nb retries of {}, no additional reprocessing".format(
                            dh_id, dh_retries, MAX_NB_RETRIES
                            ),
                            print_msg=True
                        )
                else:
                    pass #do nothing as product is already available
        else:
            launcher_log.error("Invalid site info: {}".format(site), print_msg=True)

launcher_log.info("No more sites to process", print_msg=True)   