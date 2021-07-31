#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cdsapi
import numpy as np
from datetime import date, timedelta, datetime
import netCDF4 as nc4
import argparse
import os
import logging
import errno
import signal

DATE_FORMAT = "%Y%m%d"
DEFAULT_OUTPUT_DIRECTORY = "/usr/share/weather/out/test_site/"
DEFAULT_WORK_DIRECTORY = "/usr/share/weather/wrk/test_site/"
DEFAULT_ERA5_FILE_NAME = "era5_{}.nc"
DEFAULT_OUTPUT_FILE_NAME = "weather_{}.nc"
LOG_FILE_NAME = "weather.log"
VALID_PROCESSING = 0 
ERR_ERA5_DOWNLOAD = 2 #era5 file from cds can NOT be downloaded
ERR_DAILY_DATA_PROCESSING = 3 #when computing the daily average there are some errors
ERR_DAILY_DATA_WRITING = 4 #when writing the daily average data to file there are some errors
ERR_INVALID_PARAMETERS = 5 #command line parameters are wrong

class LogHandler(object):
    def __init__(self, log_path, name, level):
        self.path = log_path
        self.level = level
        self.name = name
        self.formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt = '%Y.%m.%d-%H:%M:%S%z')
        self.handler = logging.FileHandler(self.path, "a")        
        self.handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(self.name)
        if self.level == 'debug':
            log_level = logging.DEBUG
        elif self.level == 'info':
            log_level = logging.INFO
        elif self.level == 'warning':
            log_level = logging.WARNING
        elif self.level == 'error':
            log_level = logging.ERROR
        elif self.level == 'critical':
            log_level = logging.CRITICAL
        else:
            log_level = logging.DEBUG
        self.logger.setLevel(log_level)
        while self.logger.handlers:
            self.logger.handlers.pop()
        self.logger.addHandler(self.handler)
        self.logger.propagate = False

    def format_msg(self, msg):
        return msg

    def debug(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.debug(log_msg, exc_info = trace)

    def info(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.info(log_msg, exc_info = trace)

    def warning(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.warning(log_msg, exc_info = trace)

    def error(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.error(log_msg, exc_info = trace)

    def critical(self, msg, print_msg = False, trace = False):
        log_msg = self.format_msg(msg)
        if print_msg : print(log_msg)
        self.logger.critical(log_msg, exc_info = trace)

def signal_handler(signum, frame):
    log.info("Signal caught: {}.".format(signum), print_msg = True)
    os._exit(0)

def create_recursive_dirs(dir_path):
    try:
        os.makedirs(dir_path)
    except Exception as e:
        if e.errno != errno.EEXIST:
            print(e)
            return False
            
    return True

def get_hourly_era5_data(cds, day, outfile, latlonbox):
    yyyy="{:02d}".format(day.year)
    cds.retrieve(
    'reanalysis-era5-land',
    {'format': 'netcdf',
    'variable': ['2m_temperature', 'potential_evaporation', 'total_precipitation','surface_solar_radiation_downwards',\
    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3',
    'volumetric_soil_water_layer_4'],
    'month': ["{:02d}".format(day.month)],
    'day': ["{:02d}".format(day.day)],
    'time': ['00:00', '01:00', '02:00','03:00', '04:00', '05:00','06:00', '07:00', '08:00','09:00', '10:00', '11:00',
                '12:00','13:00', '14:00','15:00', '16:00', '17:00','18:00', '19:00', '20:00','21:00', '22:00', '23:00',],
    'area': latlonbox,
    'year': [yyyy]},
    outfile)

def get_hourly_data(cds, day, latlonbox):
    create_recursive_dirs(args.wrk)
    dwn_file_name = args.ef.replace("{}", day.strftime(DATE_FORMAT))
    dwn_file = os.path.join(args.wrk,dwn_file_name)
    print("Downloading to: {}".format(dwn_file))
    
    if not os.path.isfile(dwn_file):
        try:
            get_hourly_era5_data(cds, day, dwn_file, latlonbox)
            return dwn_file
        except:
            log.critical('Era5 download failed for: {}'.format(dwn_file_name), print_msg=True, trace=True)
            return None
    else:
        log.info("Era5 file downloaded in: {}".format(dwn_file))
        return dwn_file

def compute_daily_data(hourly_data_file_path):
    daily_data = {}
    try:
        hourly_data = nc4.Dataset(hourly_data_file_path,'r')
        tp = hourly_data.variables['tp'][:]
        tp[tp == -32767] = np.nan
        daily_data['evap'] = np.nan_to_num(np.nanmax(tp, axis=0) * 1000, nan = -32767)
        pev = hourly_data.variables['pev'][:]
        pev[pev == -32767] = np.nan
        daily_data['prec'] = np.nan_to_num(np.nanmin(pev, axis=0) * -1000, nan = -32767)
        t2m = hourly_data.variables['t2m'][:] 
        t2m[t2m == -32767] = np.nan
        daily_data['tmin'] = np.nan_to_num(np.nanmin(t2m ,axis=0)-273.15, nan = -32767)
        daily_data['tmax'] = np.nan_to_num(np.nanmax(t2m ,axis=0)-273.15, nan = -32767)
        daily_data['tmean'] = np.nan_to_num(np.nanmean(t2m ,axis=0)-273.15, nan = -32767)
        swvl1 = hourly_data.variables['swvl1'][:]
        swvl1[swvl1 == -32767] = np.nan
        daily_data['swvl1']= np.nan_to_num(np.nanmean(swvl1, axis=0), nan = -32767)
        swvl2 = hourly_data.variables['swvl2'][:]
        swvl2[swvl2 == -32767] = np.nan
        daily_data['swvl2']= np.nan_to_num(np.nanmean(swvl2, axis=0), nan = -32767)
        swvl3 = hourly_data.variables['swvl3'][:]
        swvl3[swvl3 == -32767] = np.nan
        daily_data['swvl3']= np.nan_to_num(np.nanmean(swvl3, axis=0), nan = -32767)
        swvl4 = hourly_data.variables['swvl4'][:]
        swvl4[swvl4 == -32767] = np.nan
        daily_data['swvl4']= np.nan_to_num(np.nanmean(swvl4, axis=0), nan = -32767)
        ssrd = hourly_data.variables['ssrd'][:]
        ssrd[ssrd == -32767] = np.nan
        daily_data['rad']= np.nan_to_num(np.nanmax(ssrd, axis=0)/1000, nan = -32767)
        daily_data['lat'] = hourly_data.variables['latitude'][:]
        daily_data['lon'] = hourly_data.variables['longitude'][:]
    except:
        log.critical("Can NOT compute daily data", print_msg=True, trace=True)
    finally:
        hourly_data.close()
    return daily_data

def export_daily_data(daily_data, day):
    try:
        create_recursive_dirs(args.out)
        daily_data_file_name = args.of.replace("{}", day.strftime(DATE_FORMAT))
        daily_data_file_path = os.path.join(args.out, daily_data_file_name)
        ncfile = nc4.Dataset(daily_data_file_path, mode='w', format='NETCDF4_CLASSIC')
        ncfile.title = "Sen4Stat daily weather data"
    
        #dimensions
        lat_dim = ncfile.createDimension('lat', len(daily_data['lat']))     # latitude axis
        lon_dim = ncfile.createDimension('lon', len(daily_data['lon']))    # longitude axis.
        crs_dim = ncfile.createDimension('crs', 1) # crs axis
        
        #spatial reference variables
        lat = ncfile.createVariable('lat', np.float64, ('lat',))
        lat.units = 'degrees_north'
        lat.long_name = 'latitude'
        lat.standard_name = 'latitude'
        lat.axis = 'Y'
        lon = ncfile.createVariable('lon', np.float64, ('lon',))
        lon.units = 'degrees_east'
        lon.long_name = 'longitude'
        lon.standard_name = 'longitude'
        lon.axis = "X"
        crs = ncfile.createVariable('crs', np.float64, ('crs',))
        crs.long_name = "CRS definition"
        nx = len(lon)
        ny = len(lat)
        xmin, ymin, xmax, ymax = [
            np.min(daily_data['lon'][:]),
            np.max(daily_data['lat'][:]),
            np.max(daily_data['lon'][:]),
            np.min(daily_data['lat'][:])]
        xres = (xmax - xmin) / float(nx)
        yres = (ymax - ymin) / float(ny)
        crs.grid_mapping = "latitude_longitude"
        crs.GeoTransform = "{} {} {} {} {}".format(xmin, xres, 0, ymax, 0, -yres)
        crs.grid_mapping_name = "latitude_longitude"
        crs.spatial_ref = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]'
        crs.longitude_of_prime_meridian = 0
        crs.inverse_flattening = 298.257223563
        crs.semi_major_axis = 6378137

        #feature variables
        evap = ncfile.createVariable('evap',np.float64,('lat','lon')) 
        evap.units = 'mm'
        evap.missing_value = -32767
        evap.standard_name = 'daily mean of era5 potential evaporation'  
        prec = ncfile.createVariable('prec',np.float64,('lat','lon')) 
        prec.units = 'mm'
        prec.missing_value = -32767
        prec.standard_name = 'daily mean of era5 precipitation'   
        tmax = ncfile.createVariable('tmax',np.float64,('lat','lon'))
        tmax.units = 'C'
        tmax.standard_name = 'daily maximum of era5 air temperature at 2m above ground level'
        tmin = ncfile.createVariable('tmin',np.float64,('lat','lon')) 
        tmin.units = 'C'
        tmin.standard_name = 'daily minimum of era5 air temperature at 2m above ground level' 
        tmean = ncfile.createVariable('tmean',np.float64,('lat','lon')) 
        tmean.units = 'C'
        tmean.standard_name = 'daily mean of era5 air temperature at 2m above ground level'   
        swvl1 = ncfile.createVariable('swvl1',np.float64,('lat','lon')) 
        swvl1.units = 'm**3 m**-3'
        swvl1.missing_value = -32767
        swvl1.standard_name = 'daily mean of era5 volumetric soil water layer 1'
        swvl2 = ncfile.createVariable('swvl2',np.float64,('lat','lon')) 
        swvl2.units = 'm**3 m**-3'
        swvl2.missing_value = -32767
        swvl2.standard_name = 'daily mean of era5 volumetric soil water layer 2'
        swvl3 = ncfile.createVariable('swvl3',np.float64,('lat','lon')) 
        swvl3.units = 'm**3 m**-3'
        swvl3.missing_value = -32767
        swvl3.standard_name = 'daily mean of era5 volumetric soil water layer 3' 
        swvl4 = ncfile.createVariable('swvl4',np.float64,('lat','lon')) 
        swvl4.units = 'm**3 m**-3'
        swvl4.missing_value = -32767
        swvl4.standard_name = 'daily mean of era5 volumetric soil water layer 4' 
        rad = ncfile.createVariable('rad',np.float64,('lat','lon')) 
        rad.units = 'J km**-2'
        rad.missing_value = -32767 
        rad.standard_name = 'daily mean of surface downwelling shortwave flux in air' 
        
        lat[:] = daily_data['lat']
        lon[:] = daily_data['lon']
        evap[:] = daily_data['evap']
        prec[:] = daily_data['prec']
        tmax[:] = daily_data['tmax']
        tmin[:] = daily_data['tmin']
        tmean[:] = daily_data['tmean']
        swvl1[:] = daily_data['swvl1']
        swvl2[:] = daily_data['swvl2']
        swvl3[:] = daily_data['swvl3']
        swvl4[:] = daily_data['swvl4']
        rad[:] = daily_data['rad']
    
        ncfile.close()    
    
    except:
        log.critical("Can NOT export daily data", print_msg=True, trace=True)
        return False
    else:
        log.info("Daily data exported to: {}".format(daily_data_file_path), print_msg=True)
        return True

def run(date):
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    cds = cdsapi.Client()
    
    log.info("Running for date: {}".format(date.strftime(DATE_FORMAT)), print_msg=True)
    
    #download era5 file
    latlonbox = [args.ymax, args.xmin, args.ymin, args.xmax] 
    hourly_data_file_path = get_hourly_data(cds, date, latlonbox)
    if (hourly_data_file_path is not None) and (not os.path.isfile(hourly_data_file_path)):
        log.critical("Can NOT download Era5 data.", print_msg=True)
        return ERR_ERA5_DOWNLOAD
    else:
        log.info("Era5 file found: {}".format(hourly_data_file_path), print_msg=True)

    #process hourly data file into daily data            
    daily_data = compute_daily_data(hourly_data_file_path)
    if daily_data == {}:
        log.critical("Daily data is empty, exiting", print_msg=True)
        return ERR_DAILY_DATA_PROCESSING
    else:            
        log.info("Daily data computed", print_msg=True)

    #write/export daily data to file    
    if not export_daily_data(daily_data, date):
        return ERR_DAILY_DATA_WRITING

    #postprocessing
    if args.de:
        try:
            os.remove(hourly_data_file_path)
        except:
            log.warning("Can NOT remove: {}".format(hourly_data_file_path), print_msg=True)

    return VALID_PROCESSING


parser = argparse.ArgumentParser(description="Launcher for daily weather data acquistion script")
parser.add_argument('-xmin', help="minimum longitude value", type=float, required=True)
parser.add_argument('-xmax', help="maximum longitude value", type=float, required=True)
parser.add_argument('-ymin', help="minimum latitude value", type=float, required=True)
parser.add_argument('-ymax', help="maximum longitude value", type=float, required=True)
parser.add_argument('-out', default = DEFAULT_OUTPUT_DIRECTORY, help="output directory")
parser.add_argument('-wrk', default = DEFAULT_WORK_DIRECTORY, help="working directory")
parser.add_argument('-of', default = DEFAULT_OUTPUT_FILE_NAME, help="output file name")
parser.add_argument('-ef', default = DEFAULT_ERA5_FILE_NAME, help="era5 file name")
parser.add_argument('-date', help="Processing date in format:" + DATE_FORMAT, required=True)
parser.add_argument('-de', default = False, help="If True era5 files will be deleted after daily data computations")
parser.add_argument('-l', '--log-level', default = 'info',
                    choices = ['debug' , 'info', 'warning' , 'error', 'critical'], 
                    help = 'Minimum logging level')
args = parser.parse_args()
start_time = datetime.now()

log_file_path = os.path.join(args.out, LOG_FILE_NAME)
log = LogHandler(
    log_path = log_file_path,
    name = "weather",
    level = args.log_level 
)

date = datetime.strptime(args.date, DATE_FORMAT)
script_return_code = run(date)
end_time = datetime.now()
delta_time = end_time - start_time
log.info("Script has run for: {}".format(str(delta_time)), print_msg=True)
os._exit(script_return_code)
