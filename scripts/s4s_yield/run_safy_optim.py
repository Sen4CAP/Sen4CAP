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
from numpy.matlib import repmat
from scipy.optimize import minimize
from pandas import isnull as isnull
import numpy as np

import json

from copy import deepcopy

NETCDF_WEATHER_BANDS = ["tmin", "tmax", "rad"]
CROP_LIST = ['wheat','maize','sunfl']

CMN_ID_COL_NAME = "NewID"
# ID_COL_NAME = "parcel_id"
CT_COL_NAME = "crop_code"

# Output column names
D0OUT_COL_NAME = "d0out"
ELUEOUT_COL_NAME = "ELUEout"
SENAOUT_COL_NAME = "SenAout"
SENBOUT_COL_NAME = "SenBout"
RMSEMIN_COL_NAME = "RmseMin"
SIMYIELD_COL_NAME = "Yield"
CONF_COL_NAME = "Conf"

OUT_HEADER = [CMN_ID_COL_NAME, CT_COL_NAME, D0OUT_COL_NAME, ELUEOUT_COL_NAME, SENAOUT_COL_NAME, 
              SENBOUT_COL_NAME, RMSEMIN_COL_NAME, SIMYIELD_COL_NAME, CONF_COL_NAME]

cropNum={}
cropNum['wheat'] = [1152,1151,1115,1114,1113]
cropNum['maize'] = [1121]
cropNum['sunfl'] = [1438]
cropNum['all'] = [1152,1151,1115,1114,1113,1121,1438]

class CmdArgs(object): 
    def __init__(self, feature, input, output, tile = ""):
        self.feature = feature
        self.input = input
        self.output = output
        self.tile = tile

class SelectedColumns(object):
    def __init__(self, col_names, global_col_indices, id_col_global_idx, crop_type_col_idx, id_col_name = CMN_ID_COL_NAME, ct_col_name = CT_COL_NAME):
        
        col_names.sort()
        self.columns = col_names
        self.global_col_indices = global_col_indices
        self.id_col_global_idx = id_col_global_idx
        self.crop_type_col_idx = crop_type_col_idx
        
        dates = []
        self.id_col_name = id_col_name
        self.ct_col_name = ct_col_name
        self.mean_indices = []
        self.valid_pix_indices = []
        self.total_pix_indices = []
        cur_idx = 1 # We start from 1 as on the first position in data is the ID
        for col in col_names:
            # get the date until the first _
            idx = col.index('_')
            if idx > 0:
                date_str = col[:idx]
                date_time_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()
                if not date_time_obj in dates:
                    dates.append(date_time_obj)  
                    
            if "_mean_" in col:
                self.mean_indices.append(cur_idx)
                
            if "_valid_pixels_cnt_" in col:
                self.valid_pix_indices.append(cur_idx)
                
            if "_total_pixels_cnt_" in col:
                self.total_pix_indices.append(cur_idx)

            cur_idx = cur_idx+1
            
        self.dates = np.array(dates)
        self.all_column_names = [self.id_col_name] + self.columns
        
        print(self.mean_indices)

    def get_all_columns(self) :
        return self.all_column_names
        
    def get_all_global_col_indices(self) :
        return [self.id_col_global_idx] + self.global_col_indices

class LaiIdxFileInfos(object) :
    def __init__(self, parcel_id, pos, len):
        self.parcel_id = parcel_id
        self.pos = pos
        self.len = len
        
class LaiFileHandler(object) : 
    def __init__(self, lai_file):
        self.lai_file = lai_file
        self.lai_file_idx = lai_file + ".idx"
        
        self.selCols = []
        # Read LAI file header
        with open(self.lai_file, 'r') as lai_file_obj:
            csv_reader = csv.reader(lai_file_obj)
            header = next(csv_reader)
            self.selCols = self.get_selected_columns(header)

        # create the reader
        print("Reading idx file {} ...".format(self.lai_file_idx))
        self.lai_file_obj = open(lai_file, "rb")
        self.parcel_idx_infos = {}
        with open(self.lai_file_idx, 'r') as read_obj:
            idx_csv_reader = csv.reader(read_obj)
            for row in idx_csv_reader:
                if len(row) == 3 :
                    idxInfo = LaiIdxFileInfos(int(row[0]), int(row[1]), int(row[2]))
                    self.parcel_idx_infos[row[0]] = idxInfo
        
    def get_lai_rows(self, parcel_ids) :
        ret_lai = []
        for parcel_id in parcel_ids:
            idxInfo = self.parcel_idx_infos[parcel_id]
            self.lai_file_obj.seek(idxInfo.pos)
            lai_row = self.lai_file_obj.read(idxInfo.len).decode('utf8').strip()
            row = lai_row.split(",")
            ret_lai.append(row)
        
        return ret_lai
    
    def get_selected_columns(self, columns) : 
        bi_cols = ['_mean_LAI', '_valid_pixels_cnt_LAI']
        col_names = []
        cur_idx = 0
        global_col_indices = []
        id_col_global_idx = -1
        crop_type_col_idx = -1
        for name in columns:
            if name == CMN_ID_COL_NAME:
                id_col_global_idx = cur_idx
            elif name == CT_COL_NAME: 
                crop_type_col_idx = cur_idx
            else :
                for bi_col in bi_cols:
                    if bi_col in name:
                        col_names.append(name)
                        global_col_indices.append(cur_idx)
                
            cur_idx = cur_idx+1
        
        if len(col_names) == 0:
            print("ERROR: Could not select any column from header {}".format(columns))
            sys.exit(1)
        # print("Selected columns: {}".format(col_names))
        return SelectedColumns(col_names, global_col_indices, id_col_global_idx, crop_type_col_idx, CMN_ID_COL_NAME)
        

class CalibrateSafyParamsWrp(object) : 
    def __init__(self, year, parcel_id, crop_num, lai_dates, bi_vals, weather_filtered, json_parameters, d0vC, ELUEvC, SenAvC, SenBvC, LAImatC, ParametersTMP):
        self.year = year
        self.parcel_id = parcel_id
        self.crop_num = crop_num
        self.lai_dates = lai_dates
        self.bi_vals = bi_vals
        self.weather_filtered = weather_filtered
        self.json_parameters = json_parameters
        self.d0vC = d0vC
        self.ELUEvC = ELUEvC
        self.SenAvC = SenAvC
        self.SenBvC = SenBvC
        self.LAImatC = LAImatC
        self.ParametersTMP = ParametersTMP
        
def crop_type_to_crop_name(croptype) :
    crop_name = ""
    for crop in CROP_LIST:
        if croptype in cropNum[crop]:
            crop_name = deepcopy(crop)
            break
    
    return crop_name
    
def rmse(x,y):
    t = ~isnull(x+y)
#     import pdb; pdb.set_trace()
    x=x[t] ; y=y[t]
    return np.sqrt(np.sum(np.power(x - y,2))/np.sum(t))
        
def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)
        
def UpdateSafyParameters(Parameters,Pfen_MrgD=-9999, Pgro_Lue=-9999, Pfen_SenA=-9999, Pfen_SenB=-9999):
    Pout  =deepcopy(Parameters)
    if Pgro_Lue!=-9999:
        Pout['Pgro_Lue'] = Pgro_Lue     # Effective ligh-use efficiency (g.MJ-1)
    if Pfen_MrgD!=-9999:
            Pout['Pfen_MrgD'] = Pfen_MrgD      
    if Pfen_SenA!=-9999:
            Pout['Pfen_SenA'] = Pfen_SenA     # Temp. Threshold to Start Senescence 
    if Pfen_SenB!=-9999:
            Pout['Pfen_SenB'] = Pfen_SenB     # Temp. Threshold to Start Senescence 
    return Pout
        
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


def MinSAFY1(x,doyind,LAItemp,ParIn, Weather):
    Parameters = UpdateSafyParameters(ParIn,Pfen_MrgD=x[0], Pgro_Lue=x[1],\
                                      Pfen_SenA=x[2], Pfen_SenB=x[3])
    GLA,Yielddum=SafyModel(Parameters,Weather)  
    # print(Parameters)
    # print(x)
    if GLA[doyind].max()>12:
        out = 9999
    else:
        out = rmse(GLA[doyind],LAItemp)
    return out

# def MinSAFY2(x,doyind,LAItemp, ParametersTMP):
#     Parameters = UpdateSafyParameters(ParametersTMP,Pfen_MrgD=x[0], Pgro_Lue=x[1])
#     GLA,Yielddum=SafyModel(Parameters,Weather) 
#     return rmse(GLA[doyind],LAItemp)


def FindMin(Obs,Ref):
    # Obs = Mx1
    # Ref = NxM
#     import pdb;pdb.set_trace()
    minO = np.amin(Obs)
    maxR = np.amax(Ref,axis=1)
    TestIn, = np.where(maxR>=minO)
    if len(TestIn)>100:
        Ref = Ref[TestIn,:]
    s = np.shape(Ref)
    Obs = repmat(Obs.T,s[0],1)
    rmse = np.sum(np.power(Obs - Ref,2),axis=1)
    del Obs, Ref
    RmseMin    = np.amin(rmse)
    indiceMin, = np.where(rmse==RmseMin)
    RmseMin = np.sqrt(RmseMin)/s[1]
    if len(TestIn)>100:
        IndOut = TestIn[indiceMin[0]]
    else:
        IndOut = indiceMin[0]
    return IndOut,RmseMin        

def CalibrateSafy2(calibrate_safy_param_wrp):
#        self.json_parameters = json_parameters

    cropfield = calibrate_safy_param_wrp.parcel_id
    croptype = calibrate_safy_param_wrp.crop_num
    LAI   = calibrate_safy_param_wrp.bi_vals
    Date = calibrate_safy_param_wrp.lai_dates
    d0vC    = calibrate_safy_param_wrp.d0vC
    ELUEvC  = calibrate_safy_param_wrp.ELUEvC
    SenAvC  = calibrate_safy_param_wrp.SenAvC
    SenBvC  = calibrate_safy_param_wrp.SenBvC
    LAImatC = calibrate_safy_param_wrp.LAImatC
    ParametersTMP = calibrate_safy_param_wrp.ParametersTMP
    Weather = calibrate_safy_param_wrp.weather_filtered
    year = calibrate_safy_param_wrp.year

    dum = np.empty((1, 7))+np.nan
    [d0out, ELUEout,SenAout,SenBout, RmseMin, SimYield, Conf] = dum[0].tolist()
    
    # import pdb;pdb.set_trace()
    if len(Date)>0:
        if LAI[0] != -9999:
            DateOK, = np.where(Date<dt.date(year,11,30))
            if len(DateOK)>0:
                LAItemp = LAI[:DateOK[-1]+1]
                datetemp = Date[:DateOK[-1]+1]
                doyind = np.array([int(x.timetuple().tm_yday)-1 for x in datetemp])
                ind,reidual = FindMin(LAItemp,LAImatC[:, doyind])
                res = minimize(MinSAFY1, \
                    np.array([d0vC[ind],ELUEvC[ind],SenAvC[ind],SenBvC[ind]]),\
                    args=(doyind,LAItemp,ParametersTMP, Weather),  \
                    method='powell',  \
                    options=dict({'disp': False}))
                d0out = int(res['x'][0])
                ELUEout = res['x'][1]
                SenAout = res['x'][2]
                SenBout = res['x'][3]
                ParDum = UpdateSafyParameters(ParametersTMP,Pfen_MrgD=d0out, Pgro_Lue=ELUEout,Pfen_SenA=SenAout, Pfen_SenB=SenBout)
                GLA,Yielddum0=SafyModel(ParDum,Weather)
                RmseMin0 = rmse(GLA[doyind],LAItemp)
                ParDum = UpdateSafyParameters(ParametersTMP,Pfen_MrgD=d0vC[ind], Pgro_Lue=ELUEvC[ind],Pfen_SenA=SenAvC[ind], Pfen_SenB=SenBvC[ind])
                GLA,Yielddum1=SafyModel(ParDum,Weather)
                RmseMin1 = rmse(GLA[doyind],LAItemp)
                BestInd  = np.argmin((RmseMin0,RmseMin1))
                if BestInd==0 and SenAout>=0 and d0out>=1:
                    RmseMin  = RmseMin0
                    SimYield = np.max(Yielddum0)
                else:
                    BestInd=1
                    RmseMin  = RmseMin1
                    SimYield = np.max(Yielddum1)
                    d0out = d0vC[ind]
                    ELUEout = ELUEvC[ind]
                    SenAout = SenAvC[ind]
                    SenBout = SenBvC[ind]
                Conf=BestInd               
    # print([d0out,ELUEout,SenAout,SenBout,RmseMin,SimYield,Conf,croptype])
    return [cropfield,croptype, d0out,ELUEout,SenAout,SenBout,RmseMin,SimYield,Conf]

# TODO: To be optimized - these are also extracted in run_safy_lut and maybe in extract_weather_features
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
    print(all_tair_vals_arr.shape[0])
    print(all_tair_vals_arr.shape[1])

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
    fff = open(json_file, 'r')
    parameters = json.load(fff)
    print(parameters)
    fff.close()
    return parameters
    
#def execute_safy(crop_type, weather, parameters) :
    # TODO    

def get_safy_range_params(params_dir) :
    d0v={}
    ELUEv={}
    SenAv={}
    SenBv={}    
    for crop in CROP_LIST:
        OutFilePar = os.path.join(params_dir, 'SAFYLUT.Parameters_Range_'+crop+'_New.npz')
        npzfile = np.load(OutFilePar)
        d0v[crop] = npzfile['MrgDv']
        ELUEv[crop] = npzfile['LUEv']
        SenAv[crop] = npzfile['SenAv']
        SenBv[crop] = npzfile['SenBv']
    return d0v, ELUEv, SenAv, SenBv
    
def get_safy_lut_params(lut_files_dir, grid_no, parameters) :
    LAImat = {}
    ParametersTMP = {}
    for crop in CROP_LIST:
        LutFile = os.path.join(lut_files_dir, 'SafyLUT.G'+str(grid_no)+'.'+crop+'_New.npz')
        print("LutFile is {}".format(LutFile))
        if os.path.isfile(LutFile):
            npzfile=np.load(LutFile,)
            LAImat[crop]   = npzfile['LAImat'].astype(np.float32)/1000
            ParametersTMP[crop] = deepcopy(parameters[crop])
        else:
            print('Missing '+LutFile)
    return LAImat, ParametersTMP
    
def merge_lai_with_grid(lai_file, grid_file, out_file) :
    command = []
    command += ["otbcli", "Markers1CsvMerge"]
    command += ["-out", out_file]
    command += ["-il"]
    command += [lai_file, grid_file]
    command += ["-ignnodatecol", 0]

    run_command(command)
    
def FloatOrZero(value):
    try:
        return float(value)
    except:
        return 0.0
        
def handle_grid_parcels(year, grid_no, grid_parcels, lai_file_handler, all_tair_vals_arr, all_rglb_vals, d0v, ELUEv, SenAv, SenBv, json_parameters, lut_dir, writer) :
    # load also the LAI for the parcels in the grid
    lai_parcel_rows = lai_file_handler.get_lai_rows(grid_parcels)
    if len(lai_parcel_rows) == 0 :
        print ("Grid {} has no intersecting parcels ...".format(grid_no))
        return

    tair_grid_vals = all_tair_vals_arr[:, grid_no]
    rglb_grid_vals = all_rglb_vals[:, grid_no]
    #print("Values for grid {} : {}".format(grid_no, tair_grid_vals))
    
    LAImat, ParametersTMPAll = get_safy_lut_params(lut_dir, grid_no, json_parameters)
    
    weather_all = get_weather(tair_grid_vals, rglb_grid_vals)
    if len(weather_all.keys()) == 0:
        return
    
    calibrate_safy_param_wrps = [] 
    for row in lai_parcel_rows:
        parcel_id = int(row[lai_file_handler.selCols.id_col_global_idx])
        crop_num = int(float(row[lai_file_handler.selCols.crop_type_col_idx].rstrip()))
        crop_name = crop_type_to_crop_name(crop_num)
        if crop_name == "" :
            print ("Could not find crop name for crop type {}".format(crop_num))
            continue
        
        
        mean_vals = np.array([FloatOrZero(row[x]) for x in lai_file_handler.selCols.mean_indices])
        valid_pixels = np.array([FloatOrZero(row[x]) for x in lai_file_handler.selCols.valid_pix_indices])
        
        # print("Mean vals = {}".format(mean_vals))
        # print("Valid pixels vals = {}".format(valid_pixels))

        # total_pixels = cropfield_descr[selCols.total_pix_indices]
        total_pixels = valid_pixels # TODO: For now we go with this but the line above should be uncommented

        # TODO: Threshold should be configurable
        test = np.array([y>0.8*x for x,y in zip(total_pixels,valid_pixels)])
        
        if (len(test) == 0) :
            continue        
            
        # print("Test = {}".format(test))
        lai_dates       =  lai_file_handler.selCols.dates[test]
        bi_vals         =  mean_vals[test] / 1e3
        
        if len(lai_dates) == 0 or len(lai_dates) != len(bi_vals) :
            continue

        # print("Parcel id = {}".format(parcel_id))
        # print("Dates = {}".format(lai_dates))
        # print("bi_vals = {}".format(bi_vals))
        
        d0vC    = deepcopy(d0v[crop_name]  )
        ELUEvC  = deepcopy(ELUEv[crop_name])
        SenAvC  = deepcopy(SenAv[crop_name])
        SenBvC  = deepcopy(SenBv[crop_name])
        LAImatC = deepcopy(LAImat[crop_name])
        ParametersTMP = deepcopy(ParametersTMPAll[crop_name])
        
        crop_params = json_parameters[crop_name]
        weather_filtered = get_updated_weather(weather_all, crop_params)
        
        calibrate_safy_param_wrps.append(CalibrateSafyParamsWrp(year, parcel_id, crop_num, lai_dates, bi_vals, weather_filtered, 
                                                                json_parameters, d0vC, ELUEvC, SenAvC, SenBvC, LAImatC, 
                                                                ParametersTMP))

    p = Pool(cpu_count())
    OUT = p.map(partial(CalibrateSafy2), calibrate_safy_param_wrps )
    p.close()
    
    #OUT = np.array(OUT)
    #print (OUT)
    
    return OUT

def initialize_out_writer(out) : 
    out_file = open(out, "w")
    out_writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
    out_writer.writerow(OUT_HEADER)
    
    return out_writer

def main():
    parser = argparse.ArgumentParser(
        description="Extracts the weather features corresponding to the parcels provided"
    )
    parser.add_argument("-y", "--year", type=int, required=True, help="The processing year")
    parser.add_argument("-i", "--input-weather", help="Weather products",  nargs="+", required=True)
    parser.add_argument("-a", "--parcel-gridded-lai", help="Input extracted features (mean, stddev etc.) for the desired BI with the weather grid id per parcel", required=True)
    parser.add_argument("-g", "--grid-to-parcel-file", help="File containing the grid to parcel id mapping", required=True)
    parser.add_argument("-p", "--safy-params-file", help="JSON file containing the safy parameters", required=True)
    parser.add_argument("-r", "--safy-params-ranges-dir", help="Directory containing the safy parameter ranges files ", required=True)
    parser.add_argument("-l", "--lut-dir", help="Directory from where the resulted LUT files are loaded", required=True)
    parser.add_argument("-w", "--working-dir", help="Working dir", required=True)
    parser.add_argument("-o", "--out", help="File where the safy optim results will be written", required=True)
    
    args = parser.parse_args()

    lai_file_handler = LaiFileHandler(args.parcel_gridded_lai)
    
    # load safy parameters
    crop_jsons = load_safy_parameters(args.safy_params_file)

    # build or simply extract the safy range parameters
    d0v, ELUEv, SenAv, SenBv = get_safy_range_params(args.safy_params_ranges_dir)
    
    # extract weather features
    all_tair_vals_arr, all_rglb_vals = get_weather_features(args.input_weather)

    writer = initialize_out_writer(args.out)
    
    all_output = []
    with open(args.grid_to_parcel_file, 'r') as grid_to_parcel:
        # pass the file object to reader() to get the reader object
        csv_reader = csv.reader(grid_to_parcel)
        header = next(csv_reader)
        prev_grid_no = -1
        grid_parcels = []
        for row in csv_reader:
            grid_no = int(row[0])
            if prev_grid_no != grid_no:
                if len(grid_parcels) > 0 :
                    all_output += handle_grid_parcels(args.year, grid_no, grid_parcels, lai_file_handler, all_tair_vals_arr, all_rglb_vals, 
                                        d0v, ELUEv, SenAv, SenBv, crop_jsons, args.lut_dir, writer)
                    grid_parcels = []
            
            # add the parcel id
            grid_parcels.append(row[1])
            prev_grid_no = grid_no
                 
        if len(grid_parcels) > 0 :
            all_output += handle_grid_parcels(args.year, grid_no, grid_parcels, lai_file_handler, all_tair_vals_arr, all_rglb_vals, 
                    d0v, ELUEv, SenAv, SenBv, crop_jsons, args.lut_dir, writer)            
        
        all_output.sort()
        print(all_output)
        writer.writerows(all_output)
   
if __name__ == "__main__":
    main()
