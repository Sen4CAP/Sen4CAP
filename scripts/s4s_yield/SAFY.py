#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 17:38:35 2018

@author: claveriem
"""
#%%
import numpy as np
from MyTools import LaunchPGwithoutput
import os
from copy import deepcopy
# import matplotlib
# matplotlib.style.use("ggplot")
# import matplotlib.pyplot as plt


def SafyParameters(Pfen_MrgD=100, Pgro_Lue=3, Pfen_SenA=1008, Pfen_PrtB=0.0025):
    # General Parameter                                               
    ###################################################################
    Parameters = {}
    Parameters['Pgen_StrtSim'] = 1      # Day to Start the Simulation 
    Parameters['Pgen_StopSim'] = 273    # Day to Stop the Simulation
    ##################################################################
    # Crop growth Parameter
    ##################################################################
    Parameters['Pgro_R2P'] = 0.47    # Global to PAR incident radiation ratio
    Parameters['Pgro_Kex'] = 0.5     # Extinction of Radiation in Canopy [0.3-1)
    Parameters['Pgro_Lue'] = Pgro_Lue     # Effective ligh-use efficiency (g.MJ-1)
    Parameters['Pgro_Ms0'] = 4.5     # Emergence Dry Mass Value (g/m2=100 x t/ha)
    Parameters['Pgro_Sla']= 0.022   # Specific Leaf-Area 0.024 (m2 g-1)  
    Parameters['Pgro_P2G'] = 0.2 # updated from Get_SAFY_pheno_Par notebook # 0.0052  # Partition coefficient To Grain 
    ###################################################################
    # Phenological parameter
    ###################################################################
    # Day of Plant Emergence
    Parameters['Pfen_MrgD'] = Pfen_MrgD      
    # Partitioning to Leaves (after Maas, 1993)
    Parameters['Pfen_PrtA'] = 0.1573   # Make vary the origin slope of partition fn 
    Parameters['Pfen_PrtB'] = Pfen_PrtB #0.00196  # Make vary the day of max LAI (partition=0)
    # 0.0029 # updated from Get_SAFY_pheno_Par notebook #
    # Senescence function
    Parameters['Pfen_SenA'] = Pfen_SenA     # Temp. Threshold to Start Senescence 
    Parameters['Pfen_SenB'] = 6875     # Make vary the rate of Senescence 
    ###################################################################
    # Temperature Effect On Development - result in TpS=TempStress[0-1]
    ###################################################################
    Parameters['Ptfn_Tmin'] =  0  # Minimum Temperature for Plant Development (C)
    Parameters['Ptfn_Topt'] = 20  # Optimum Temperature for Plant Development (C)
    Parameters['Ptfn_Tmax'] = 37  # Maximum Temperature for Plant Development ()
    Parameters['Ptfn_TpSn'] = 2   # Make vary the length of plateau around optimum T    
    ###################################################################
    # update doy to Indice
    Parameters['Pgen_StrtSim'] = Parameters['Pgen_StrtSim']-1
    Parameters['Pgen_StopSim'] = Parameters['Pgen_StopSim']-1
    return Parameters

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
########################
### 0b) READ CLIMATE ###
########################
# import socket
# def ReadCSVWeather(Parameters,Country,grid_no,year):
#     PWD = os.getcwd()
#     root = PWD.replace('Code/SAFY','')
#     if 'geo1' in socket.gethostname():
#         root = '//geo12/homedir/'
#     dirWeather = root+'Data/Weather/MARS4CAST/CSVID/'+Country
#     Weather = {}
#     Weather['Rglb']=[]
#     Weather['Tair']=[]
#     with open(dirWeather+'/ID'+str(grid_no)+'.Y'+str(year)+'.csv', 'r') as csvfile:
#         for row in csv.reader(csvfile, delimiter=','):
#             if '20' in row[0][:2]:
#                 Weather['Tair'].append((float(row[2])+float(row[3]))/2)
#                 Weather['Rglb'].append(float(row[1])/1000.)
#     Weather['Rglb']=Weather['Rglb']\
#         [Parameters['Pgen_StrtSim']:Parameters['Pgen_StopSim']]	
#     Weather['Tair']=Weather['Tair']\
#         [Parameters['Pgen_StrtSim']:Parameters['Pgen_StopSim']]   
#     return Weather

def ReadWeather(Parameters,Country,grid_no,year):
    ScheMa=Country
    Command = 'SELECT tmin,tmax,rad from yield.'+Country+'_era5_data '+\
    "WHERE date_part('year', date)="+str(year)+\
    ' AND grid_no='+str(grid_no)+' ORDER BY date'
    # print(Command)
    table =  LaunchPGwithoutput(Command)
    table =  np.array(list(zip(*table)))
    Weather = {}
    if np.sum(table[0] == None) < 100:
        Weather['Tair'] = list((table[0]+table[1])/2)
        Weather['Rglb'] = list(table[2]/1000)
        Weather['Rglb']=Weather['Rglb']\
               [Parameters['Pgen_StrtSim']:Parameters['Pgen_StopSim']]	
        Weather['Tair']=Weather['Tair']\
               [Parameters['Pgen_StrtSim']:Parameters['Pgen_StopSim']]   
    return Weather
    
    
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
#            Day_Of_Emergence = I
        # elif I >= Parameters['Pgen_StopSim']:
        #     continue
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
#                    SMT_ANT = SMT[I]
                    Day_Of_Anthesis = I
                else:
                    DGM[I] = DGM[I-1] + Parameters['Pgro_P2G'] * DAM[I]
                # End of Vegetation Model if LAI < initial value
#                if GLA[I] < InitLAI:
#                    Day_Of_Senescence = I
    # import pdb;pdb.set_trace()
    return GLA,DGM
#%%
                    #%%
# Country = 'belgium'
# grid_no= 96097
# year= 2017
# 
# Parameters=SafyParameters(100,1.8)
# 
# Weather = ReadCSVWeather(Parameters)
# 
# GLA,DGM=SafyModel(Parameters,Weather)

#%%

#plt.plot(GLA)
# plt.plot(DGM)
