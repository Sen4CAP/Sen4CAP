#!/usr/bin/env python

import argparse
import numpy as np
import pandas as pd
import sys
import time

C_INDEX_NO = 0
C_INDEX_STRONG = 1
C_INDEX_MODERATE = 2
C_INDEX_WEAK = 3
C_INDEX_POOR = 4

class Config(object):
    def __init__(self, args):
        self.lpis_csv = args.lpis_csv
        self.periods = args.periods
        self.periods_files = args.periods_files
        self.ThrdCompactS2 = args.s2_compactness_threshold
        self.group_items_cnt = args.group_items_cnt
        self.output = args.output

class MarkerInfos(object):
    def __init__(self, m1, m2, m3, m4):
        self.sum_m1 = (m1)
        self.sum_m2 = (m2)
        self.sum_m3 = (m3)
        self.sum_m4 = (m4)

        self.sum_all = (m1 + m2 + m3 + m4)

    def add_markers(self, m1, m2, m3, m4) :
        self.sum_m1 = self.sum_m1 + (m1)
        self.sum_m2 = self.sum_m2 + (m2)
        self.sum_m3 = self.sum_m3 + (m3)
        self.sum_m4 = self.sum_m4 + (m4)

        self.sum_all = self.sum_m1 + self.sum_m2 + self.sum_m3 + self.sum_m4

class ParcelInfos(object):
    def __init__(self, row, pm_period_start, pm_period_end, group_items_cnt):
        self.HoleS2_All = getattr(row, 'HoleS2')
        self.HoleS2_AllPart = getattr(row, 'HoleS2Part')
        self.periods_dict = dict()
        m1_val = row.M1
        m2_val = row.M2
        m3_val = row.M3
        m4_val = row.M4
        row_period = row.period
        for p in range(pm_period_start,pm_period_end+1):
            pm = range(p, p + group_items_cnt)
            if row_period in pm :
                mi = MarkerInfos(m1_val, m2_val, m3_val, m4_val)
                self.periods_dict[p] = mi
        
    
    def add_row(self, row, pm_period_start, pm_period_end, group_items_cnt) :
        self.HoleS2_All = sum([self.HoleS2_All, row.HoleS2])
        self.HoleS2_AllPart = sum([self.HoleS2_AllPart, row.HoleS2Part])
        m1_val = row.M1
        m2_val = row.M2
        m3_val = row.M3
        m4_val = row.M4
        row_period = row.period
        for p in range(pm_period_start,pm_period_end+1):
            pm = range(p, p + group_items_cnt)
            if row_period in pm :
                if p in self.periods_dict.keys():
                    self.periods_dict[p].add_markers(m1_val, m2_val, m3_val, m4_val)
                else :
                    mi = MarkerInfos(m1_val, m2_val, m3_val, m4_val)
                    self.periods_dict[p] = mi

def get_c_index(index_n) :
    if index_n == C_INDEX_STRONG:
        return "STRONG"
    elif index_n == C_INDEX_MODERATE:
        return 'MODERATE'
    elif index_n == C_INDEX_WEAK:
        return 'WEAK'
    elif index_n == C_INDEX_POOR:
        return 'POOR'
    else:
        return 'NO'

def get_lpis_infos_maps(lpis_csv_file):
    df = pd.read_csv(lpis_csv_file, usecols = ["NewID", "ShapeInd", "lc"])
    # create a mapping from MDB ID to Decl ID 
    shapeind_dict = dict(zip(df["NewID"], df["ShapeInd"]))
    lc_dict = dict(zip(df["NewID"], df["lc"]))
    return shapeind_dict, lc_dict

def period_analysis(config) : 
    shapeind_dict, lc_dict = get_lpis_infos_maps(config.lpis_csv)
    # lpis_csv = pd.read_csv(config.lpis_csv)
    # lpis_csv.set_index('NewID', inplace=True)

    dt_all = pd.DataFrame(columns=['NewID','M1','M2','M3','M4','period'])

    markerL = [col for col in dt_all if col.startswith(('M'))]
    for i in range(len(config.periods)):
        p = config.periods[i]
        f = pd.read_csv(config.periods_files[i])
        f['M4'] = 0
        f.loc[f['CompactA'] > config.ThrdCompactS2,'M4'] = 1
        f['period'] = p
        
        dt_all = pd.concat([dt_all,f])

    dt_all = dt_all.sort_values(['NewID', 'period'])

    pm_period_start = min(config.periods)
    pm_period_end = max(f['period'])-config.group_items_cnt

    ids_dict = dict()
    for row in dt_all.itertuples():
        new_id = row.NewID
        if new_id in ids_dict.keys():
            ids_dict[new_id].add_row(row, pm_period_start, pm_period_end, config.group_items_cnt)
        else:
            ids_dict[new_id] = ParcelInfos(row, pm_period_start, pm_period_end, config.group_items_cnt)

    dt_out = []
    l_marker = len(markerL)
    sum_markers_ref = l_marker*config.group_items_cnt
    sum_markers_ref2 = round(sum_markers_ref-(config.group_items_cnt/2))
    sum_markers_ref3 = round(l_marker*(config.group_items_cnt-1))
    sum_markers_ref4 = round((l_marker/2)*(config.group_items_cnt))
    for i in ids_dict.keys():
        dt_i = ids_dict[i]

        C_INDEX = C_INDEX_NO
        P_Hete_L = np.nan
        M1 = np.nan
        M2 = np.nan
        M3 = np.nan
        M4 = np.nan
        HoleS2_All = dt_i.HoleS2_All
        HoleS2_AllPart = dt_i.HoleS2_AllPart
     
        for p in range(pm_period_start,pm_period_end+1):
            pm = range(p,p+config.group_items_cnt)
            if not (p in dt_i.periods_dict) :
                # print ("Ignoring period {} for id {} as it was not found ...".format(p, i))
                continue
            marker_infos = dt_i.periods_dict[p]
            sum_markers = marker_infos.sum_all
            if sum_markers == sum_markers_ref :
                C_INDEX = C_INDEX_STRONG
                P_Hete_L = p
                M1 = marker_infos.sum_m1
                M2 = marker_infos.sum_m2
                M3 = marker_infos.sum_m3
                M4 = marker_infos.sum_m4

            elif ((sum_markers >= sum_markers_ref2) & (C_INDEX != C_INDEX_STRONG)):
                C_INDEX = C_INDEX_MODERATE
                P_Hete_L = p
                M1 = marker_infos.sum_m1
                M2 = marker_infos.sum_m2
                M3 = marker_infos.sum_m3
                M4 = marker_infos.sum_m4
            
            elif ((sum_markers >= sum_markers_ref3) & (C_INDEX not in (C_INDEX_STRONG, C_INDEX_MODERATE))):
                C_INDEX = C_INDEX_WEAK
                P_Hete_L = p
                M1 = marker_infos.sum_m1
                M2 = marker_infos.sum_m2
                M3 = marker_infos.sum_m3
                M4 = marker_infos.sum_m4
            
            elif ((sum_markers >= sum_markers_ref4) & (C_INDEX not in (C_INDEX_STRONG, C_INDEX_MODERATE,C_INDEX_WEAK))):
                C_INDEX = C_INDEX_POOR
                P_Hete_L = p
                M1 = marker_infos.sum_m1
                M2 = marker_infos.sum_m2
                M3 = marker_infos.sum_m3
                M4 = marker_infos.sum_m4
            
        shape_ind = shapeind_dict.get(i, 0)
        lc_val = lc_dict.get(i, 0)

        dt_out.append({
            'NewID' : i,
            'LC': lc_val,
            'ShapeInd': shape_ind,
            'M1' : M1,
            'M2' : M2,
            'M3' : M3,
            'M4' : M4,
            'P_Hete_L' : P_Hete_L,
            'C_INDEX' : get_c_index(C_INDEX),
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

    time1 = time.time()
    
    period_analysis(config)

    time2 = time.time()
    print("Total execution took: {}".format(time2-time1))
    
if __name__ == "__main__":
    main()
