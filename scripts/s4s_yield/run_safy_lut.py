#!/usr/bin/env python

import argparse
import logging
from collections import defaultdict
from datetime import date
import datetime as dt
# from datetime import datetime
from datetime import timedelta
from glob import glob
import multiprocessing.dummy
import os
import os.path
import pipes
from osgeo import osr, gdal, ogr
from gdal import gdalconst
import re
import sys
import csv
import errno
import ntpath
import subprocess
from functools import partial
from multiprocessing import Pool,cpu_count
import json
import numpy as np

NETCDF_WEATHER_BANDS = ["tmin", "tmax", "rad"]
CROP_LIST = ['wheat','maize','sunfl']

class CmdArgs(object): 
    def __init__(self, feature, input, output, tile = ""):
        self.feature = feature
        self.input = input
        self.output = output
        self.tile = tile

def RunSafy(FourPar, Weather, Parameters, k):
    Parameters['Pfen_MrgD'] = FourPar[k][0]
    Parameters['Pgro_Lue' ] = FourPar[k][1]
    Parameters['Pfen_SenA' ] = FourPar[k][2]
    Parameters['Pfen_SenB' ] = FourPar[k][3]
    
    GLA,dum=SafyModel(Parameters,Weather)
    GLA = GLA * 1000
    GLA = GLA.astype(np.uint16)
    # print(Parameters)
    return GLA
    
def Temp_Stress(T=None, Tmin=None, Topt=None, Tmax=None, TpSn=None):
    if T < Tmin or T > Tmax:    # T outside the functioning range
        TpS = 0
    else:
        if T <= Topt:        # T lower than the optimal value
            TpS = 1 - np.power((T - Topt) / (Tmin - Topt), TpSn)
        else:        # T higher than the optimal value
            TpS = 1 - np.power((T - Topt) / (Tmax - Topt), TpSn)
    return TpS
    
def SafyModel(Parameters,Weather):
    #DAY OF SIMULATION
    I=0  
    ###################################################################
    # SUM of TEMPERATURE for DEGREE-DAY APPROACH (C)
    SMT = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # TEMPERATURE STRESS INDEX ([0-1],unitless)
    TpS = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # ABSORBED PHOTOSYNTHETIC ACTIVE RADIATION (MJ/m2/day)
    PAR = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # DRY AERIAL & GRAIN MASS (g/m2=100 x t/ha)
    DAM = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    DGM = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # PARTITION-TO-LEAF INDEX ([0-1],unitless)
    PRT = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # GREEN LEAF AREA INDEX (LAI, m2/m2) 
    GLA = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # DELTA of GREEN LAI from DAY D to D+1 
    # (P=plus;M=minus;m2/m2)
    DLP = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    DLM = np.zeros(Parameters['Pgen_StopSim']-Parameters['Pgen_StrtSim']+1)
    # Day of Total Senescence
    Day_Of_Anthesis=0
    Day_Of_Senescence=0
    ###################################################################
    # THREESHOLD ON LAI TO DECLARE TOTAL YELLOWING (m2/m2)
    InitLAI=Parameters['Pgro_Ms0']*Parameters['Pgro_Sla']  # = Initial Value of LAI  
    ###################################################################
    
    ##########################
    # VEGETATION FUNCTIONING & 
    ##########################
    # print(Parameters)
    for I in range(1,Parameters['Pgen_StopSim'] - Parameters['Pgen_StrtSim'] ):
        # print(I)
        if I == Parameters['Pfen_MrgD']:    # DAY OF EMERGENCE
            # Init Vegetation Model
            SMT[I] = np.maximum(Weather['Tair'][I] - Parameters['Ptfn_Tmin'], 0)
            DAM[I] = Parameters['Pgro_Ms0']
            GLA[I] = InitLAI
        elif I > Parameters['Pfen_MrgD'] and GLA[I-1] >= InitLAI :    # VEGETATIVE PERIOD
            # Temperature Sum and Stress
            TpS[I] = Temp_Stress(Weather['Tair'][I],\
               Parameters['Ptfn_Tmin'],\
               Parameters['Ptfn_Topt'],\
               Parameters['Ptfn_Tmax'],\
               Parameters['Ptfn_TpSn'])
            SMT[I] = SMT[I-1] + np.maximum(Weather['Tair'][I] - Parameters['Ptfn_Tmin'], 0)
            # Daily Total PAR Absorbed by the Canopy, Daily Dry Mass Production 
            
            PAR[I] = Parameters['Pgro_R2P'] * Weather['Rglb'][I] * (1 - np.exp(-Parameters['Pgro_Kex'] * GLA[I-1]))
            ddam = Parameters['Pgro_Lue'] * PAR[I] * TpS[I]
            DAM[I] = DAM[I-1] + ddam
            # PaRTitioning, Green LAI Increase (DLP) and Leave Senescence Function (DLM), 
            PRT[I] = np.maximum(1 - Parameters['Pfen_PrtA'] * np.exp(Parameters['Pfen_PrtB'] * SMT[I]), 0)
            DLP[I] = ddam * PRT[I] * Parameters['Pgro_Sla']
            if SMT[I] > Parameters['Pfen_SenA']:
                DLM[I] = GLA[I-1] * (SMT[I] - Parameters['Pfen_SenA']) / Parameters['Pfen_SenB']            
            GLA[I] = GLA[I-1] + DLP[I] - DLM[I]
            #Yield (Grain Mass increase after the leaf production period)
            if PRT[I] == 0:
                if PRT[I-1] > 0:  # End of Leaf Growing Period
                    Day_Of_Anthesis = I
                else:
                    DGM[I] = DGM[I-1] + Parameters['Pgro_P2G'] * DAM[I]
    return GLA,DGM

# def ReadWeather(Country = "spain",grid_no = "3407",year = "2019"):
#     ScheMa=Country
#     Command = 'SELECT tmin,tmax,rad from yield.'+Country+'_era5_data '+\
#     "WHERE date_part('year', date)="+str(year)+\
#     ' AND grid_no='+str(grid_no)+' ORDER BY date'
#     # print(Command)
#     table =  LaunchPGwithoutput(Command)
#     table =  np.array(list(zip(*table)))
#     Weather = {}
#     if np.sum(table[0] == None) < 100:
#         Weather['Tair'] = list((table[0]+table[1])/2)
#         Weather['Rglb'] = list(table[2]/1000)
#    return Weather

def get_weather_features(inputs) :
    all_tair_vals = []
    all_rglb_vals = []
    for input in inputs: 
        print("Extracting weather features from file {}".format(input))
        # src_ds = gdal.Open( input )
        # print "[ RASTER BAND COUNT ]: ", src_ds.RasterCount
        # srcband = src_ds.GetRasterBand(1)
        # rast_array = np.array(srcband.ReadAsArray())
        # print rast_array   

        sel_bands = []
        tair_vals = []    
        rglb_vals = []
        tmin_vals = []
        tmax_vals = []
        rad_vals = []
        for band in NETCDF_WEATHER_BANDS :
            band_ds = gdal.Open("NETCDF:\"{}\":{}".format(input, band))
            if band == "tmin" :
                tmin_vals = np.array(band_ds.ReadAsArray()).flatten()
            elif band == "tmax" :
                tmax_vals = np.array(band_ds.ReadAsArray()).flatten()
            else :
                rad_vals = np.array(band_ds.ReadAsArray()).flatten()
        
        # now fill the tair and rglb the current date
        tair_vals = (tmin_vals + tmax_vals) / 2
        rglb_vals = np.true_divide(rad_vals, 1000)
        
        all_tair_vals.append(tair_vals)
        all_rglb_vals.append(rglb_vals)
    
    
        # print("tmin_vals = {}".format(tmin_vals))
        # print("tmax_vals = {}".format(tmax_vals))
        # 
        # print("rad_vals = {}".format(rad_vals))

    all_tair_vals_arr = np.array(all_tair_vals)
    # print all_tair_vals_arr.shape[0]
    # print all_tair_vals_arr.shape[1]

    return np.array(all_tair_vals), np.array(all_rglb_vals)
    # print all_tair_vals_arr.shape[0]
    # print all_tair_vals_arr.shape[1]
    
def get_weather(tair_grid_vals, rglb_grid_vals) :
    Weather = {}
    if tair_grid_vals.shape[0] <= 100:
        print("Only {} values are in the array. It will be ignored ....".format(tair_grid_vals.shape[0]))
        return Weather

    Weather['Tair'] = list(tair_grid_vals)
    Weather['Rglb'] = list(rglb_grid_vals)    
    
    return Weather
    
def get_updated_weather(weather, parameters) :
    weather['Rglb']=weather['Rglb']\
           [parameters['Pgen_StrtSim']:parameters['Pgen_StopSim']]	
    weather['Tair']=weather['Tair']\
           [parameters['Pgen_StrtSim']:parameters['Pgen_StopSim']]   
    
    return weather
    
    
def load_safy_parameters(json_file) :
    print("Loading SAFY parameters ...")
    fff = open(json_file, 'r')
    parameters = json.load(fff)
    print(parameters)
    fff.close()
    return parameters
    
#def execute_safy(crop_type, weather, parameters) :
    # TODO    

def get_safy_range_params(params_dir) :
    print("Extracting SAFY range parameters ...")
    crop_params = {}
    for crop in CROP_LIST:
        OutFilePar = os.path.join(params_dir, 'SAFYLUT.Parameters_Range_'+crop+'_New.npz')
        print("Extracting safy range params in/from file {}".format(OutFilePar))
        if crop=='wheat':
            Pfen_MrgDrange  = np.arange(1,200,2)
            Pgro_Luerange =  np.arange(0, 6, 0.1)
            Pfen_SenArange  = np.arange(500,2000,200)
            Pfen_SenBrange  = np.arange(1000,20000,2000)
        elif crop=='maize':
            Pfen_MrgDrange  = np.arange(90,180,2)
            Pgro_Luerange =  np.arange(0, 6, 0.2)
            Pfen_SenArange  = np.arange(500,2000,200)
            Pfen_SenBrange  = np.arange(1000,20000,2000)
        elif crop=='sunfl':
            Pfen_MrgDrange  = np.arange(90,180,1)
            Pgro_Luerange =  np.arange(0, 6, 0.1)
            Pfen_SenArange  = np.arange(500,2000,200)
            Pfen_SenBrange  = np.arange(1000,20000,2000)
        MrgDv,LUEv,SenAv,SenBv=np.meshgrid(Pfen_MrgDrange,Pgro_Luerange,Pfen_SenArange,Pfen_SenBrange)
        MrgDv = MrgDv.flatten()
        LUEv  = LUEv.flatten()
        SenAv = SenAv.flatten() 
        SenBv = SenBv.flatten() 
        FourPar = list(zip(MrgDv,LUEv,SenAv,SenBv))
        FourPar = [list(x) for x in FourPar]  
        if not os.path.isfile(OutFilePar):
            np.savez(OutFilePar, MrgDv=MrgDv, LUEv=LUEv, SenAv=SenAv, SenBv=SenBv, FourPar=FourPar)
        # else:
        #     npzfile = np.load(OutFilePar)
        #     MrgDv = npzfile['MrgDv']
        #     LUEv = npzfile['LUEv']
        #     SenAv = npzfile['SenAv']
        #     SenBv = npzfile['SenBv']
        #     FourPar = npzfile['FourPar']
        
        crop_params[crop] = FourPar
    return crop_params
    
def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-i", "--input-list", help="Weather products",  nargs="+", required=True)
    parser.add_argument("-p", "--safy-params-file", help="JSON file containing the safy parameters", required=True)
    parser.add_argument("-r", "--safy-params-ranges-dir", help="Directory containing the safy parameter ranges files ", required=True)
    parser.add_argument("-o", "--out-lut-dir", help="Directory where the resulted LUT files are saved", required=True)
    
    args = parser.parse_args()
    
    # load safy parameters
    crop_jsons = load_safy_parameters(args.safy_params_file)

    # build or simply extract the safy range parameters
    safy_range_params = get_safy_range_params(args.safy_params_ranges_dir)
    
    # extract weather features
    all_tair_vals_arr, all_rglb_vals = get_weather_features(args.input_list)
    
    # run safy for all the crops and weather grids
    for grid_no in range(0, all_tair_vals_arr.shape[1]):
        print("Running SAFY for grid {} ...".format(grid_no))
        tair_grid_vals = all_tair_vals_arr[:, grid_no]
        rglb_grid_vals = all_rglb_vals[:, grid_no]

        #print("Values for grid {} : {}".format(grid_no, tair_grid_vals))
        
        weather_all = get_weather(tair_grid_vals, rglb_grid_vals)
        if len(weather_all.keys()) == 0:
            continue
        
        for crop in CROP_LIST:
            crop_params = crop_jsons[crop]
            weather_filtered = get_updated_weather(weather_all, crop_params)
            
            range_params = safy_range_params[crop]
            
            print("Running SAFY for crop {} and a number of {} params".format(crop, len(range_params)))
            p = Pool(cpu_count())
            OUT = p.map(partial(RunSafy, range_params, weather_filtered, crop_params), range(len(range_params)))
            p.close()
            LAImat = np.array(OUT)
            OutFile = os.path.join(args.out_lut_dir, 'SafyLUT.G'+str(grid_no)+'.'+crop+'_New.npz')
            np.savez_compressed(OutFile, LAImat=LAImat)
            
            
    # ReadWeather()

    
if __name__ == "__main__":
    main()
