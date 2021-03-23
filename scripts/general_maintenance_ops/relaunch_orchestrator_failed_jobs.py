#!/usr/bin/env python

import argparse
import os
import pipes
import subprocess
import sys
import re
import shutil
import getopt
import datetime
from datetime import timedelta, date
import fnmatch
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras
import glob

from dateutil.relativedelta import relativedelta

import xml.etree.ElementTree
import logging

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

        self.job_id = args.job_id

def get_job_status(config, conn):
    with conn.cursor() as cursor:
        query = SQL(
            """ select status_id from job where id = {}; """
        )

        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)

        results = cursor.fetchall()
        conn.commit()
        
        print(results)
        
        status_id = None
        for (status, ) in results: 
            status_id = status
        return status_id

def reset_job(config, conn) :

    with conn.cursor() as cursor:
        query = SQL(
            """ UPDATE step
                    SET status_id = 1, --Submitted
                        status_timestamp = now()
                    FROM task
                    WHERE task.id = step.task_id AND task.job_id = {}
                            AND step.status_id in (4, 7, 8); -- Running, Cancelled or Failed; """
        )
        # TODO : should we consider here (7, 8); -- Canceled or Failed instead of Running, Canceled and Failed ???
        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit()

    with conn.cursor() as cursor:
        query = SQL(
            """ 	UPDATE task
                    SET status_id = CASE WHEN EXISTS (SELECT * FROM step WHERE step.task_id = task.id AND step.status_id = 6) THEN 4 --Running
                        ELSE 1 --Submitted
                    END,
                    status_timestamp = now()
                    WHERE task.job_id = {} and status_id in (4, 7, 8); -- Running, Cancelled or Failed """
        )
        # TODO : should we consider here only (7, 8); -- Canceled or Failed instead of Running, Canceled and Failed ???
        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit()

    with conn.cursor() as cursor:
        query = SQL(
            """ 	UPDATE job
                    SET status_id = CASE WHEN EXISTS (SELECT * FROM task WHERE task.job_id = job.id AND status_id IN (4,6)) 
                            THEN 4 --Running
                            ELSE 1 --Submitted
                        END,
                        status_timestamp = now()
                    WHERE id = {}
                            AND status_id = 8; -- Error; """
        )

        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit()
        
def pause_job(config, conn) :
    with conn.cursor() as cursor:
        query = SQL(
            """ set transaction isolation level repeatable read; 
                select * from sp_mark_job_paused({}); """
        )

        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit()

def resume_job(config, conn) :
    with conn.cursor() as cursor:
        query = SQL(
            """ set transaction isolation level repeatable read;
                select * from sp_mark_job_resumed({}); """
        )

        job_id_filter = Literal(config.job_id)
        query = query.format(job_id_filter)
        print(query.as_string(conn))
        cursor.execute(query)
        conn.commit()

    
def main():

    parser = argparse.ArgumentParser(description='Resets the failed status of a job (for resuming it)')
    parser.add_argument('-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location")
    parser.add_argument('-j', '--job-id', default=0, type=int, help="Job ID to be relaunched")
    
    args = parser.parse_args()
    
    config = Config(args)
    
    if config.job_id == 0 :
        print("Please provide the job id!!!")
        return

    log_time=datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")       

    print("{}: Resetting job with id {}".format(log_time,config.job_id))
    with psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password) as conn:
        job_status = get_job_status(config, conn)
        if job_status != 8:
            print ("Job with id {} either does not exist or is not in Error state".format(config.job_id, job_status))
            return 
        
        # we are first resetting the products and then simulate a pause and then a resume job (in order to make events inserted into the database)
        reset_job(config, conn)
        pause_job(config, conn)        
        resume_job(config, conn)
        
###################################################################################################
if __name__ == '__main__':
    sys.exit(main())
