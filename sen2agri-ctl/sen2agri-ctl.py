#!/usr/bin/env python
from __future__ import print_function
import argparse
import dbus
import json
import numbers
import sys
import psycopg2
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
        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

class Site(object):

    def __init__(self, site_id, name, short_name):
        self.site_id = site_id
        self.name = name
        self.short_name = short_name

    def __cmp__(self, other):
        if hasattr(other, 'site_id'):
            return self.site_id.__cmp__(other.site_id)

class Processor(object):

    def __init__(self, id, short_name):
        self.id = id
        self.short_name = short_name

    def __cmp__(self, other):
        if hasattr(other, 'id'):
            return self.id.__cmp__(other.id)

class NewJob(object):

    def __init__(self, name, description, processor_id, site_id, start_type,
                 parameters, configuration):
        self.name = name
        self.description = description
        self.processor_id = processor_id
        self.site_id = site_id
        self.start_type = start_type
        self.parameters = parameters
        self.configuration = configuration


class Sen2AgriClient(object):
    def __init__(self, config):
        self.config = config
        
    def get_sites(self):
        connection = self.get_connection()
        cur = self.get_cursor(connection)
        cur.execute("""SELECT * FROM sp_get_sites()""")
        rows = cur.fetchall()
        connection.commit()
        connection.close()

        sites = []
        for row in rows:
            sites.append(Site(row['id'], row['name'], row['short_name']))

        return sorted(sites)

    def get_processors(self):
        connection = self.get_connection()
        cur = self.get_cursor(connection)
        cur.execute("""SELECT * FROM sp_get_processors()""")
        rows = cur.fetchall()
        connection.commit()

        processors = []
        for row in rows:
            processors.append(Processor(row['id'], row['short_name']))

        return sorted(processors)

    def submit_job(self, job):
        connection = self.get_connection()
        cur = self.get_cursor(connection)
        cur.execute("""SELECT * FROM sp_submit_job(%(name)s :: character varying, %(description)s ::
        character varying, %(processor_id)s :: smallint,
                       %(site_id)s :: smallint, %(start_type_id)s :: smallint, %(parameters)s ::
                       json, %(configuration)s :: json)""",
                    {"name": job.name,
                     "description": job.description,
                     "processor_id": job.processor_id,
                     "site_id": job.site_id,
                     "start_type_id": job.start_type,
                     "parameters": job.parameters,
                     "configuration": json.JSONEncoder().encode([dict(c) for c in job.configuration]) # [{"key": c.key, "value": c.value} for c in job.configuration]
                    })
        rows = cur.fetchall()
        connection.commit()
        connection.close()

        jobId = rows[0][0]

#        bus = dbus.SystemBus()
#        orchestrator_proxy = bus.get_object('org.esa.sen2agri.orchestrator',
#                                            '/org/esa/sen2agri/orchestrator')
#        orchestrator_proxy.NotifyEventsAvailable()

        return jobId

    def get_connection(self):
        return psycopg2.connect(host=self.config.host, dbname=self.config.dbname, user=self.config.user, password=self.config.password)

    def get_cursor(self, connection):
        return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


class Sen2AgriCtl(object):

    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Controls the Sen2Agri system")
        
        parser.add_argument('-c', '--config-file', default='/etc/sen2agri/sen2agri.conf', help="Configuration file location")
        
        subparsers = parser.add_subparsers()

        parser_list_sites = subparsers.add_parser(
            'list-sites', help="Lists the available sites")
        parser_list_sites.set_defaults(func=self.list_sites)

        parser_submit_job = subparsers.add_parser(
            'submit-job', help="Submits a new job")
        parser_submit_job.add_argument('-s', '--site',
                                       required=True, help="site")
        parser_submit_job_subparsers = parser_submit_job.add_subparsers()

        parser_composite = parser_submit_job_subparsers.add_parser(
            'composite', help="Submits a new composite type job")
        parser_composite.add_argument('-i', '--input',
                                      nargs='+', required=True,
                                      help="input products")
        parser_composite.add_argument(
            '-d', '--synthesis-date',
            required=True, help="The synthesis date (YYYYMMDD)")
        parser_composite.add_argument(
            '-s', '--half-synthesis',
            required=False, help="Half synthesis interval in days")
        parser_composite.add_argument(
            '-res', '--resolution', type=int, required=False, help="resolution in m")
        parser_composite.add_argument(
            '-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_composite.set_defaults(func=self.submit_composite)

        parser_l3b = parser_submit_job_subparsers.add_parser(
            'l3b', help="Submits a new L3B type job")
        parser_l3b.add_argument('-i', '--input',
                                      nargs='+', required=True,
                                      help="input products")
        parser_l3b.add_argument(
            '-res', '--resolution', type=int, required=False, help="resolution in m")
        parser_l3b.add_argument(
            '-m', '--monolai', type=int, required=False, help="boolean specifying if LAI mono-date should be done")
        parser_l3b.add_argument(
            '-r', '--reproc', type=int, required=False, help="boolean specifying if reprocessing should be done")
        parser_l3b.add_argument(
            '-f', '--fitted', type=int, required=False, help="boolean specifying if fitted should be done")
        parser_l3b.add_argument(
            '-g', '--genmodel', type=int,required=False,  help="boolean specifying if models should be generated")
        parser_l3b.add_argument(
            '-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_l3b.set_defaults(func=self.submit_l3b)

        parser_pheno_ndvi = parser_submit_job_subparsers.add_parser(
            'phenondvi', help="Submits a new Phenological NDVI Metrics type job")
        parser_pheno_ndvi.add_argument('-i', '--input',
                                       nargs='+', required=True,
                                       help="input products")
        parser_pheno_ndvi.add_argument(
            '--phenondvi', help="phenondvi")
        parser_pheno_ndvi.add_argument(
            '--resolution', type=int, help="resolution in m")
        parser_pheno_ndvi.add_argument(
            '-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_pheno_ndvi.set_defaults(func=self.submit_pheno_ndvi)

        parser_crop_mask = parser_submit_job_subparsers.add_parser(
            'crop-mask', help="Submits a new crop mask job")
        parser_crop_mask.add_argument('-i', '--input',
                                      nargs='+', required=True,
                                      help='input products')
        parser_crop_mask.add_argument('-res', '--resolution', type=int, 
                                      required=False, help="resolution in m")                                      
        parser_crop_mask.add_argument('-r', '--reference',
                                      required=False, metavar="SHAPEFILE",
                                      help="reference polygons")
        parser_crop_mask.add_argument('-refr', '--reference-raster',
                                      required=False,
                                      help="reference raster for noinsitu")
        parser_crop_mask.add_argument('-t', '--strata_shape',
                                      required=False, metavar="SHAPEFILE",
                                      help="Strata file")
        parser_crop_mask.add_argument('-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_crop_mask.set_defaults(func=self.submit_crop_mask)

        parser_crop_type = parser_submit_job_subparsers.add_parser(
            'crop-type', help="Submits a new crop type job")
        parser_crop_type.add_argument('-i', '--input',
                                      nargs='+', required=False,
                                      help="input products")
        parser_crop_type.add_argument('-r', '--reference',
            required=True, metavar="SHAPEFILE", help="reference polygons")
        parser_crop_type.add_argument('-t', '--strata_shape',
                                      required=False, metavar="SHAPEFILE",
                                      help="Strata file")
        parser_crop_type.add_argument('--crop-mask', help="crop mask")
        parser_crop_type.add_argument('--resolution', type=int, help="resolution in m")
        parser_crop_type.add_argument('-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_crop_type.set_defaults(func=self.submit_crop_type)

        parser_agric_pract = parser_submit_job_subparsers.add_parser(
            'agric_practices', help="Submits a new agricultural practices job")
        parser_agric_pract.add_argument('-ndvi', '--ndvi-inputs',
                                      nargs='+', required=False,
                                      help="input NDVI products")
        parser_agric_pract.add_argument('-amp', '--amp-inputs',
                                      nargs='+', required=False,
                                      help="input Amplitude products")
        parser_agric_pract.add_argument('-cohe', '--cohe-inputs',
                                      nargs='+', required=False,
                                      help="input Coherence products")
        parser_agric_pract.add_argument('-s', '--start-date',
            required=False, help="start date in case inputs are not given")
        parser_agric_pract.add_argument('-e', '--end-date',
            required=False, help="end date in case inputs are not given")
            
        parser_agric_pract.add_argument('-cfg', '--config-path',
                                      required=False,
                                      help="Site parameters configuration file path")
            
        parser_agric_pract.add_argument('-p', '--parameter', action='append', nargs=2,
            metavar=('KEY', 'VALUE'), help="override configuration parameter")
        parser_agric_pract.set_defaults(func=self.submit_agricultural_practices)
        
        args = parser.parse_args(sys.argv[1:])
        
        config = Config(args)
        self.client = Sen2AgriClient(config)
        
        args.func(args)

    def list_sites(self, args):
        for site in self.client.get_sites():
            print("Id: {}, Full Name: {}, Short name: {}".format(site.site_id, site.name, site.short_name))

    def submit_composite(self, args):
        parameters = {'input_products': args.input,
                      'synthesis_date': args.synthesis_date}
        if args.half_synthesis:
            parameters['half_synthesis'] = args.half_synthesis
        if args.resolution:
            parameters['resolution'] = args.resolution
        self.submit_job('l3a', parameters, args)

    def submit_l3b(self, args):
        parameters = {'input_products': args.input}
        if args.resolution:
            parameters['resolution'] = args.resolution
        if args.genmodel:
            parameters['genmodel'] = args.genmodel
        if args.monolai:
            parameters['monolai'] = args.monolai
        if args.reproc:
            parameters['reproc'] = args.reproc
        if args.fitted:
            parameters['fitted'] = args.fitted
        self.submit_job('l3b', parameters, args)

    def submit_pheno_ndvi(self, args):
        parameters = {'input_products': args.input}
        if args.resolution:
            parameters['resolution'] = args.resolution
        self.submit_job('l3e_pheno', parameters, args)

    def submit_crop_mask(self, args):
        parameters = {'input_products': args.input}
        if args.resolution:
            parameters['resolution'] = args.resolution
        if args.reference : 
            parameters['reference_polygons'] = args.reference
        if args.reference_raster:
            parameters['reference_raster'] = args.reference_raster
        if args.strata_shape:
            parameters['strata_shape'] = args.strata_shape
            
        self.submit_job('l4a', parameters, args)

    def submit_crop_type(self, args):
        parameters = {
                      'reference_polygons': args.reference}
        if args.input:
            parameters['input_products'] = args.input    
        if args.crop_mask:
            parameters['crop_mask'] = args.crop_mask
        if args.resolution:
            parameters['resolution'] = args.resolution
        if args.strata_shape:
            parameters['strata_shape'] = args.strata_shape
        self.submit_job('l4b', parameters, args)

    def submit_agricultural_practices(self, args):
        parameters = {}
        if args.ndvi_inputs:
            parameters['ndvi_input_products'] = args.ndvi_inputs
        if args.amp_inputs:
            parameters['amp_input_products'] = args.amp_inputs
        if args.cohe_inputs:
            parameters['cohe_input_products'] = args.cohe_inputs

        if args.start_date:
            parameters['start_date'] = args.start_date
        if args.end_date:
            parameters['end_date'] = args.end_date

        if args.config_path:
            parameters['config_path'] = args.config_path
            
        self.submit_job('s4c_l4c', parameters, args)
        
    def create_job(self, processor_id, parameters, args):
        config = config_from_parameters(args.parameter)

        site_id = self.get_site_id(args.site)
        if site_id is None:
            self.list_sites(args)
            raise RuntimeError("Invalid site '{}'".format(args.site))

        job = NewJob("", "", processor_id, site_id, 2,
                     json.JSONEncoder().encode(parameters), config)
        return job

    def submit_job(self, processor_short_name, parameters, args):
        processor_id = self.get_processor_id(processor_short_name)
        if processor_id is None:
            raise RuntimeError("Invalid processor id '{}'".format(processor_short_name))

        job = self.create_job(processor_id, parameters, args)
        job_id = self.client.submit_job(job)
        print("Submitted job {} for processor name {} having processor id {}".format(job_id, processor_short_name, processor_id))


    def get_site_id(self, name):
        if isinstance(name, numbers.Integral):
            return name

        sites = self.client.get_sites()
        for site in sites:
            if site.name == name or site.short_name == name:
                return site.site_id
        
        
        return None

    def get_processor_id(self, name):
        if isinstance(name, numbers.Integral):
            return name

        processors = self.client.get_processors()
        for processor in processors:
            if processor.short_name == name:
                return processor.id

        return None

def config_from_parameters(parameters):
    config = []
    if parameters:
        for param in parameters:
            config.append({'key': param[0], 'value': param[1]})
    return config

if __name__ == '__main__':
    try:
        Sen2AgriCtl()
    except Exception, err:
        print("ERROR:", err, file=sys.stderr)
