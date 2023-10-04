#!/usr/bin/env python

import argparse
import numpy as np
import pandas as pd
import sys

class Config(object):
    def __init__(self, args):
        self.lpis_csv = args.lpis_csv
        self.periods = args.periods
        self.periods_files = args.periods_files
        self.ThrdCompactS2 = args.s2_compactness_threshold
        self.group_items_cnt = args.group_items_cnt
        self.output = args.output

def period_analysis(config) : 
    lpis_csv = pd.read_csv(config.lpis_csv)
    lpis_csv.set_index('NewID', inplace=True)

    dt_all = pd.DataFrame(columns=['NewID','M1','M2','M3','M4','period'])

    markerL = [col for col in dt_all if col.startswith(('M'))]
    for i in range(len(config.periods)):
        p = config.periods[i]
        f = pd.read_csv(config.periods_files[i])
        f['M4'] = 0
        f.loc[f['CompactA'] > config.ThrdCompactS2,'M4'] = 1
        f['period'] = p
        
        dt_all = pd.concat([dt_all,f])

    newid_l = dt_all.NewID.unique()
    
    print(dt_all.to_string())

    dt_out = []
    l_marker = len(markerL)
    for i in newid_l:
        #i = 814
        dt_i = dt_all.loc[dt_all['NewID']==i]

        C_INDEX = 'NO'
        P_Hete_L = np.nan
        M1 = np.nan
        M2 = np.nan
        M3 = np.nan
        M4 = np.nan
        HoleS2_All = sum(dt_i['HoleS2']) 
        HoleS2_AllPart = sum(dt_i['HoleS2Part'])
     
        pm_period_start = min(config.periods)
        pm_period_end = max(f['period'])-config.group_items_cnt

        for p in range(pm_period_start,pm_period_end+1):
            pm = range(p,p+config.group_items_cnt)
            dt_i_m = dt_i.loc[dt_i['period'].isin(pm),markerL]
            sum_markers = dt_i_m.values.sum()
            M_sum = [sum(dt_i_m.iloc[:,0]),sum(dt_i_m.iloc[:,1]),sum(dt_i_m.iloc[:,2]),sum(dt_i_m.iloc[:,3])]
            if sum_markers == l_marker*config.group_items_cnt :
                # print('strong')
                # print(i)
                C_INDEX = 'STRONG'
                P_Hete_L = p
                M1 = M_sum[0]
                M2 = M_sum[1]
                M3 = M_sum[2]
                M4 = M_sum[3]

            elif ((sum_markers >= round(l_marker*config.group_items_cnt-(config.group_items_cnt/2))) & (C_INDEX!= 'STRONG')):
                C_INDEX = 'MODERATE'
                P_Hete_L = p
                M1 = M_sum[0]
                M2 = M_sum[1]
                M3 = M_sum[2]
                M4 = M_sum[3]
            
            elif ((sum_markers >= round(l_marker*(config.group_items_cnt-1))) & (C_INDEX not in ('STRONG','MODERATE'))):
                C_INDEX = 'WEAK'
                P_Hete_L = p
                M1 = M_sum[0]
                M2 = M_sum[1]
                M3 = M_sum[2]
                M4 = M_sum[3]
            
            elif ((sum_markers >= round((l_marker/2)*(config.group_items_cnt))) & (C_INDEX not in ('STRONG','MODERATE','WEAK'))):
                #print(f'markers sum : {dt_i_m.values.sum()} and c_index previous : {c_ind}')
                C_INDEX = 'POOR'
                P_Hete_L = p
                M1 = M_sum[0]
                M2 = M_sum[1]
                M3 = M_sum[2]
                M4 = M_sum[3]
            
        dt_out.append({
            'NewID' : i,
            'LC': lpis_csv.lc[i],
            'ShapeInd': lpis_csv.ShapeInd[i],
            'M1' : M1,
            'M2' : M2,
            'M3' : M3,
            'M4' : M4,
            'P_Hete_L' : P_Hete_L,
            'C_INDEX' : C_INDEX,
            'HoleS2' : HoleS2_All,
            'HoleS2_PM' : HoleS2_AllPart
        })
            
    dt_out = pd.DataFrame(dt_out)
    dt_out = dt_out.astype({"M1":"int","M2":"int","M3":"int","M4":"int"}, errors='ignore')
    
    dt_out.to_csv(config.output,index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Heterogeneity period analysis"
    )
    parser.add_argument("-l", "--lpis-csv", help="LPIS CSV file", required=True)
    parser.add_argument("-t", "--s2-compactness-threshold", help="Threshold of the compactness in the S2 analysis", type=float, default=1.7)    
    parser.add_argument('-p', '--periods', nargs='+', type=int, help="The list of periods")   
    parser.add_argument('-f', '--periods-files', nargs='+', help="The list of period CSV files")   
    parser.add_argument('-g', '--group-items-cnt', type=int, help="Number of periods for grouping the analysis", default=3)   
    parser.add_argument("-o", "--output", help="Output CSV file", required=True)

    args = parser.parse_args()
    
    if args.periods == 0 or args.periods_files == 0 :
        print ("No periods or period files were provided! Exiting ...")
        sys.exit(1)

    if len(args.periods) != len(args.periods_files) :
        print ("The number of periods should be equal with the number of period files! Exiting ...")
        sys.exit(2)
    
    config = Config(args)

    period_analysis(config)
    
if __name__ == "__main__":
    main()
