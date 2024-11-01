#!/usr/bin/env python

import argparse
import numpy as np
from osgeo import gdal,gdal_array
import glob
import pandas as pd
import subprocess, platform, os, glob,sys
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
        self.ndvi_image = args.ndvi_image
        self.smooted_raster = args.smooted_raster
        self.local_conn_raster = args.local_conn_raster
        self.lpis_csv = args.lpis_csv
        self.lpis_buffered_raster = args.lpis_buffered_raster
        self.number_of_images = args.number_of_images
        self.period = args.period
        
        self.NPixClS2 = args.min_s2_pixels
        self.ThrdNDVIdist = args.ndvi_thr_dist
        self.ThrdCompactS2 = args.s2_compactness_threshold
        
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
    return dict_parcel_pixels

def ensure_column_exists(df, column_name, def_val):    
    if not column_name in df.columns:
        df[column_name] = def_val
    return df

def import_data(config) : 
    print("Importing data ...")

    area_meters_dict, shapeind_dict, lc_dict = get_lpis_infos_maps(config.lpis_csv)

    area = 'Area_meters'
    img_ds = gdal.Open(config.ndvi_image,gdal.GA_ReadOnly)
    num_bands = img_ds.RasterCount
    
    # parcel identification
    # lpis_csv = pd.read_csv(config.lpis_csv, usecols = ["NewID", "Area_meters", "ShapeInd", "lc"])
    # lpis_csv.set_index('NewID', inplace=True) #used to get the Area_meters

    #cluster raster 
    n = config.number_of_images
    date_l = []
    for i in range(1,n+1):
        # protection for the last period when we might not have all dates in raster
        if (i + (config.period-1)*n <= num_bands):
            date_l.append(i + (config.period-1)*n)
            
    # date_l = [i + (config.period-1)*n for i in range(1,n+1)]

    n_var = len(date_l)
    img = np.zeros((img_ds.RasterYSize,img_ds.RasterXSize,n_var),
                   gdal_array.GDALTypeCodeToNumericTypeCode(img_ds.GetRasterBand(1).DataType))

    l_var = []
    print('Start import')
    for d in range(len(date_l)):
        date = date_l[d]
        b = 0
        
        img_ds = gdal.Open(config.ndvi_image,gdal.GA_ReadOnly)

        img_dsB = img_ds.GetRasterBand(date)
        img_dsA = img_dsB.ReadAsArray()

        img[:, :, d+b] = img_dsA
        l_var.insert((b*len(date_l))+d,f'NDVI_{date}')

    l_var.insert(0,'label')
    print('imported NDVI')
    # ## extract cluster % of x parcels

    # raster = gdal.Open(config.lpis_buffered_raster,gdal.GA_ReadOnly)
    # imgR = raster.ReadAsArray()

    start = time.time()
    parcel_pixels = get_parcel_pixels(config.lpis_buffered_raster)
    end = time.time()
    print("Extracting parcels pixels took {}".format(end - start))

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
    cnt = 0
    parcels_in_perc_rng = 10*int(total_parcels_no / 100)

    for parcel_id in sorted(parcel_pixels.keys()):
        #start = time.time()
        #cl_i = np.where(imgC[0] == i)
        #end = time.time()
        #print("Stage 1 took {}".format(end - start))

        # val_poly = imgC[1,cl_i[0],cl_i[1]]
        cl_i = parcel_pixels[parcel_id]
        val_poly = Cluster_extract[cl_i.x,cl_i.y]
        val_poly = val_poly.astype(np.int64)

        # val_poly_tst = Cluster_extract[cl_i_tst.x, cl_i_tst.y]
        # val_poly_tst = val_poly_tst.astype(np.int64)
        # c = all(val_poly == val_poly_tst)

        val_poly1 = val_poly[val_poly!=0]
        v_countP = np.bincount(val_poly1)/len(val_poly1)

        v_count = np.bincount(val_poly1)
        df_cl1 = pd.DataFrame({'NewID':[parcel_id]})

        shape_ind = shapeind_dict.get(parcel_id, 0)
        df_cl1['ShapeInd'] = shape_ind
        lc_val = lc_dict.get(parcel_id, 0)
        df_cl1['LC'] = lc_val
        # df_cl1['ShapeInd'] = lpis_csv['ShapeInd'][parcel_id] 
        # df_cl1['LC'] = lpis_csv['lc'][parcel_id] 
        
        if sum(val_poly)>0 :
            for c in range(1,max(val_poly1)+1):
                df_cl1[f'clP_{c}'] = v_countP[c]
                df_cl1[f'cl_{c}'] = v_count[c]

        for idx in range(1,5):
            df_cl1 = ensure_column_exists(df_cl1, f'clP_{idx}', 0)
            df_cl1 = ensure_column_exists(df_cl1, f'cl_{idx}', 0)

        if len(v_count) == 0:
            df_cl1['Hete'] = 3
            df_cl1['distNDVI'] = 0
            
        elif max(v_countP) < config.PerHetero:

            val_Connect = Cluster_connectE[cl_i.x,cl_i.y]

            valAll_poly = img[cl_i.x,cl_i.y,:]
            valAll_poly = np.concatenate([val_poly[:, np.newaxis],valAll_poly],axis=1)
            #print(valAll_poly)
            valAll_poly = valAll_poly.astype(float)
            #valAll_poly = valAll_poly.fillna(0)
            valAll_poly[:,1:][valAll_poly[:,1:]==0] = np.nan
            c_nan = np.isnan(valAll_poly)
            cluster_i = np.unique(val_poly)

            dist = pd.DataFrame(columns=['cl_name','distNDVI'])
            valAll_poly_d = pd.DataFrame(valAll_poly,columns=l_var)
            
            #m_polyd = valAll_poly_d[ndvi_col].dropna(axis=1)
            #print('test')
            m_polydG = valAll_poly_d.groupby('label').mean().reset_index()
            v_countnoNa = np.bincount(valAll_poly_d['label'])
            nNaN = sum(np.isnan(valAll_poly[:,1:]))/len(valAll_poly)
            df_cl1['HoleS2'] = sum(nNaN==1)
            df_cl1['HoleS2Part'] = sum((nNaN>0) & (nNaN<1))

            if len(v_countnoNa)!=0:
                dist1_list = []
                for ii in range(1,max(cluster_i)+1):
                    for j in reversed(range(1,max(cluster_i)+1)):
                        if j<=ii:
                            continue
                        else:
                            dist1 = pd.DataFrame({'cl_name': [str(ii)+','+str(j)]})
                            #print(str(i)+','+str(j))
                            #print(v_count[ii])          
                            if (v_countnoNa[ii]>config.NPixClS2) & (v_countnoNa[j]>config.NPixClS2):
                                #print('after')
                                dist1['distNDVI'] = np.absolute(np.nanmean(m_polydG[m_polydG.label==ii]) - np.nanmean(m_polydG[m_polydG.label==j]))
                            else:
                                dist1['distNDVI'] = 0
                            dist1_list.append(dist1)
                
                dist1_df = pd.concat(dist1_list)
                dist = pd.concat([dist,dist1_df])
                df_cl1['distNDVI'] = round(np.nanmax(dist['distNDVI'])/1000,5)

            df_cl1['Compact'] = np.nanmean(val_Connect)
            area = area_meters_dict.get(parcel_id, 10)
            df_cl1['CompactA'] = np.nanmean(val_Connect)/ np.log(area) 

            # df_cl1['CompactA'] = (np.nanmean(val_Connect)/np.log(lpis_csv[area][parcel_id]))
            df_cl1['Hete'] = 1         
            
            if sum(v_count>config.NPixClS2) >= 2:
                df_cl1['M2'] = 1
            else :
                df_cl1['M2'] = 0

        else:
            df_cl1['Hete'] = 0
            df_cl1['distNDVI'] = 0
            df_cl1['M2'] = 0
            df_cl1['Compact'] = 0
            df_cl1['CompactA'] = 0
            df_cl1['HoleS2'] = 0
            df_cl1['HoleS2Part'] = 0
        
        df_cl_list.append(df_cl1)
        #df_cl = pd.concat([df_cl,df_cl1])

        if (parcels_in_perc_rng > 0) and (cnt % parcels_in_perc_rng) == 0:
            print("{}% parcels completed".format(10*int(cnt / parcels_in_perc_rng)))

        cnt = cnt + 1    

    df_results = pd.concat(df_cl_list)
    df_cl = pd.concat([df_cl, df_results])

    df_cl.loc[df_cl['Hete'].isin((0,3)),'M1'] = 0 
    df_cl.loc[df_cl['Hete'] == 1,'M1'] = 1
    df_cl.loc[df_cl['distNDVI'] >= config.ThrdNDVIdist,'M3'] = 1
    df_cl.loc[df_cl['CompactA'] >= config.ThrdCompactS2,'M4'] = 1
    df_cl = df_cl.fillna(0)

    df_clout = df_cl[['NewID','Hete','clP_1','cl_1','clP_2','cl_2','clP_3','cl_3','clP_4','cl_4','distNDVI','Compact','CompactA','M1','M2','M3','M4','HoleS2','HoleS2Part']]
    df_clout = df_clout.astype({"Hete":"int","M1":"int","M2":"int","M3":"int","M4":"int", "cl_1":"int","cl_2":"int","cl_3":"int","cl_4":"int", "HoleS2":"int","HoleS2Part":"int"}, errors='ignore')
    
    df_clout.to_csv(config.output,index=False)
    print(config.output)

def get_lpis_infos_maps(lpis_csv_file):
    df = pd.read_csv(lpis_csv_file, usecols = ["NewID", "Area_meters", "ShapeInd", "lc"])
    # create a mapping from MDB ID to Decl ID 
    area_meters_dict = dict(zip(df["NewID"], df["Area_meters"]))
    shapeind_dict = dict(zip(df["NewID"], df["ShapeInd"]))
    lc_dict = dict(zip(df["NewID"], df["lc"]))
    return area_meters_dict, shapeind_dict, lc_dict

def main():
    parser = argparse.ArgumentParser(
        description="Parcels cluster analysis"
    )
    parser.add_argument("-i", "--ndvi-image", help="Input image containing the resampled NDVI images", required=True)
    parser.add_argument("-l", "--lpis-csv", help="LPIS CSV file", required=True)
    parser.add_argument("-b", "--lpis-buffered-raster", help="LPIS buffered raster corresponding to the input image tile", required=True)
    parser.add_argument("-s", "--smooted-raster", help="Spatial smoothing resulted raster", required=True)
    parser.add_argument("-c", "--local-conn-raster", help="Spatial conectivity resulted raster", required=True)
    parser.add_argument("-n", "--number-of-images", help="Number of images to use in the period (if month: 3 images of 10 days resampled S2)", type=int, required=False, default=3)
    parser.add_argument("-p", "--period", help="Clustering period index (1 to 12 months, for example)", type=int, required=True)
    parser.add_argument("-m", "--min-s2-pixels", help="Minimum number of S2 pixels needed to take into consideration the cluster", type=int, default=20)
    parser.add_argument("-d", "--ndvi-thr-dist", help="Threshold of the NDVI distance calculated between clusters", type=float, default=0.17)
    parser.add_argument("-t", "--s2-compactness-threshold", help="Threshold of the compactness in the S2 analysis", type=int, default=3)
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
