#!/usr/bin/env python

import argparse
import numpy as np
from osgeo import gdal,gdal_array
import glob
import pandas as pd
import time

gdal.UseExceptions()
gdal.AllRegister()

# th_diff_NDVI = 100
# n_cl = 4
# NPixClS2 = 20
# ThrdNDVIdist = 0.15
# ThrdCompactS2 = 3
n = 3 #number of images to use in the period (if month: 3 images of 10 days resampled S2)


class Config(object):
    def __init__(self, args):
        self.smooted_raster = args.smooted_raster
        self.local_conn_raster = args.local_conn_raster
        self.lpis_csv = args.lpis_csv
        self.lpis_buffered_raster = args.lpis_buffered_raster
        self.number_of_images = args.number_of_images
        self.period = args.period
        self.NPixClS1 = args.min_s1_pixels
        self.ThrdCompactS1 = args.s1_compactness_threshold
        self.PerHetero = args.percentage_heterogeneity
        self.output = args.output

class PixelInfos(object):
    def __init__(self, x, y):
        self.x = [x]
        self.y = [y]

    def add(self, x, y):
        self.x.append(x)
        self.y.append(y)

def get_parcel_pixels(lpis_buffered_raster) :
    time1 = time.time()
    
    raster = gdal.Open(lpis_buffered_raster,gdal.GA_ReadOnly)
    imgR = raster.ReadAsArray()

    dict_parcel_pixels = dict()
    x_idx = 0
    for row in imgR:
        y_idx = 0
        for pixel_val in row:
            if pixel_val != 0 :
                if pixel_val in dict_parcel_pixels.keys():
                    dict_parcel_pixels[pixel_val].add(x_idx, y_idx)
                else:
                    dict_parcel_pixels[pixel_val] = PixelInfos(x_idx, y_idx)
            y_idx = y_idx + 1
        x_idx = x_idx + 1
        
    time2 = time.time()
    print("get_parcel_pixels took: {} s" .format(time2 - time1))
        
    return dict_parcel_pixels

def check_lpis_buffered_raster(config):
    print('Start import of lpis buffered raster')
    
    Cluster_isolated = gdal.Open(config.smooted_raster,gdal.GA_ReadOnly)
    ulx, xres, xskew, uly, yskew, yres  = Cluster_isolated.GetGeoTransform()
    if xres == 20 : 
        if config.lpis_buffered_raster.endswith("_S2.tif"):
            config.lpis_buffered_raster = config.lpis_buffered_raster[:-len("_S2.tif")] + "_S1.tif"
    elif xres == 10 : 
        if config.lpis_buffered_raster.endswith("_S1.tif"):
            config.lpis_buffered_raster = config.lpis_buffered_raster[:-len("_S1.tif")] + "_S2.tif"
    else :
        print("Error: unsupported resolution {} in the S1 processing {}. Should be 10 or 20. Exiting ...".format(xres, config.smooted_raster))
        sys.exit(1)
    
    print("Input rasters resolution is {}m. Using LPIS raster {}".format(xres, config.lpis_buffered_raster))
    # ## extract cluster % of x parcels

    raster = gdal.Open(config.lpis_buffered_raster,gdal.GA_ReadOnly)
    imgR = raster.ReadAsArray()
    num_bands = raster.RasterCount
    
    all_bands_are_zero = True
    for i in range(num_bands) : 
        print("Checking band {}".format(i))
        print("NpMin = {}".format(np.min(imgR[i])))
        print("NpMax = {}".format(np.max(imgR[i])))
        if (np.min(imgR[0]) != 0 or np.max(imgR[0]) != 0) : 
            all_bands_are_zero = False
            break

    if all_bands_are_zero:
        print("All bands in input raster are zero. No need to continue and writing only file header ...")
        df_cl = pd.DataFrame(columns=['NewID','Hete','clP_1','cl_1','clP_2','cl_2','clP_3','cl_3','clP_4','cl_4','Compact','CompactA','M5','M6','M7'])
        df_clout = df_cl[['NewID','Hete','clP_1','cl_1','clP_2','cl_2','clP_3','cl_3','clP_4','cl_4','Compact','CompactA','M5','M6','M7']]
        df_clout.to_csv(config.output,index=False)        
        exit(0)

def ensure_column_exists(df, column_name, def_val):    
    if not column_name in df.columns:
        df[column_name] = def_val
    return df

def import_data(config) : 

    check_lpis_buffered_raster(config)
    
    print("Importing lpis data ...")
    # parcel identification
    area_metters_map = get_area_metters_map(config.lpis_csv)

    parcel_pixels = get_parcel_pixels(config.lpis_buffered_raster)

    Cluster_isolated = gdal.Open(config.smooted_raster,gdal.GA_ReadOnly)
    Cluster_extract = Cluster_isolated.ReadAsArray()

    Cluster_connect = gdal.Open(config.local_conn_raster,gdal.GA_ReadOnly)
    Cluster_connectE = Cluster_connect.ReadAsArray()

    # imgC = np.stack([imgR,Cluster_extract,Cluster_connectE])
    # newid_l = np.unique(imgR)
    # newid_l = newid_l[newid_l!=0]

    df_cl = pd.DataFrame(columns=['NewID','Hete'])
    df_cl_list = []

    total_parcels_no = len(parcel_pixels.keys())
    print("Having a number of {} parcels".format(total_parcels_no))
    cnt = 0
    parcels_in_perc_rng = 10*int(total_parcels_no / 100)

    for parcel_id in sorted(parcel_pixels.keys()):
        cl_i = parcel_pixels[parcel_id]
        val_poly = Cluster_extract[cl_i.x,cl_i.y]
        val_poly = val_poly.astype(np.int64)
        val_poly1 = val_poly[val_poly!=0] 
        v_countP = np.bincount(val_poly1)/len(val_poly1) 
        v_count = np.bincount(val_poly1) 
        df_cl1 = pd.DataFrame({'NewID':[parcel_id]}) 
        if sum(val_poly)>0 : 
            for c in range(1,max(val_poly1)+1): 
                df_cl1[f'clP_{c}'] = v_countP[c] 
                df_cl1[f'cl_{c}'] = v_count[c] 
        for idx in range(1,5):
            df_cl1 = ensure_column_exists(df_cl1, f'clP_{idx}', 0)
            df_cl1 = ensure_column_exists(df_cl1, f'cl_{idx}', 0)

        if len(v_count) > 0 and max(v_countP) < config.PerHetero: 
            val_Connect = Cluster_connectE[cl_i.x,cl_i.y]
            df_cl1['Compact'] = np.nanmean(val_Connect) 
            area = area_metters_map.get(parcel_id, None)
            if area is None:
                df_cl1['CompactA'] = np.nanmean(val_Connect)
            else:
                df_cl1['CompactA'] = np.nanmean(val_Connect)/ np.log(area) 
            df_cl1['Hete'] = 1 
            if sum(v_count > config.NPixClS1) >= 2: 
                df_cl1['M6'] = 1 
            else: 
                df_cl1['M6'] = 0                
        else: 
            df_cl1['Hete'] = 0 
            df_cl1['Compact'] = 0
            df_cl1['CompactA'] = 0
            df_cl1['M6'] = 0  
        
        df_cl_list.append(df_cl1)

        if (cnt % parcels_in_perc_rng) == 0:
            print("{}% parcels completed".format(10*int(cnt / parcels_in_perc_rng)))

        cnt = cnt + 1    
                
    df_results = pd.concat(df_cl_list)
    df_cl = pd.concat([df_cl, df_results])

#    print("Result: {}".format(df_cl))
    
    df_cl.loc[df_cl['Hete'].isin((0,3)),'M5'] = 0 
    df_cl.loc[df_cl['Hete'] == 1,'M5'] = 1
    df_cl.loc[df_cl['CompactA'] >= config.ThrdCompactS1,'M7'] = 1
    df_cl = df_cl.fillna(0)

    df_clout = df_cl[['NewID','Hete','clP_1','cl_1','clP_2','cl_2','clP_3','cl_3','clP_4','cl_4','Compact','CompactA','M5','M6','M7']]
    df_clout = df_clout.astype({"Hete":"int","M5":"int","M6":"int","M7":"int","cl_1":"int","cl_2":"int","cl_3":"int","cl_4":"int"}, errors='ignore')
    df_clout.to_csv(config.output,index=False)
    print(config.output)

def get_area_metters_map(lpis_csv_file):
    df = pd.read_csv(lpis_csv_file, usecols = ["NewID", "Area_meters"])
    # create a mapping from MDB ID to Decl ID 
    ret_dict = dict(zip(df["NewID"], df["Area_meters"]))
    return ret_dict


def main():
    parser = argparse.ArgumentParser(
        description="Parcels cluster analysis"
    )
    parser.add_argument("-l", "--lpis-csv", help="LPIS CSV file", required=True)
    parser.add_argument("-b", "--lpis-buffered-raster", help="LPIS buffered raster corresponding to the input image tile", required=True)
    parser.add_argument("-s", "--smooted-raster", help="Spatial smoothing resulted raster", required=True)
    parser.add_argument("-c", "--local-conn-raster", help="Spatial conectivity resulted raster", required=True)
    parser.add_argument("-n", "--number-of-images", help="Number of images to use in the period (if month: 3 images of 10 days resampled S2)", type=int, required=False, default=3)
    parser.add_argument("-p", "--period", help="Clustering period index (1 to 12 months, for example)", type=int, required=True)
    parser.add_argument("-m", "--min-s1-pixels", help="Minimum number of S2 pixels needed to take into consideration the cluster", type=int, default=20)
    parser.add_argument("-t", "--s1-compactness-threshold", help="Threshold of the compactness in the S2 analysis", type=int, default=3)
    parser.add_argument("-r", "--percentage-heterogeneity", help="Pixels percentage corresponding to the biggest cluster in the parcel", type=float, default=0.9)
    parser.add_argument("-o", "--output", help="Output CSV file", required=True)
    
    args = parser.parse_args()
    config = Config(args)

    time1 = time.time()
    
    import_data(config)
    
    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))
    
if __name__ == "__main__":
    main()
