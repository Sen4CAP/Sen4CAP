#!/usr/bin/env python

import argparse
import numpy as np
from sklearn import cluster, mixture
from osgeo import gdal,gdal_array
# import matplotlib.pyplot as plt
import glob
import pandas as pd
import subprocess, platform, os, glob,sys

gdal.UseExceptions()
gdal.AllRegister()

def write_geotiff(filename, arr, in_ds):
    if arr.dtype == np.float32:
        arr_type = gdal.GDT_Float32
    else:
        arr_type = gdal.GDT_Int32

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(filename, arr.shape[1], arr.shape[0], 1, arr_type, options=['COMPRESS=DEFLATE'])
    out_ds.SetProjection(in_ds.GetProjection())
    out_ds.SetGeoTransform(in_ds.GetGeoTransform())
    band = out_ds.GetRasterBand(1)
    band.WriteArray(arr)
    band.FlushCache()
    band.ComputeStatistics(False)

def kmeans_missing(X, n_clusters, max_iter=10):

    
    """Perform K-Means clustering on data with missing values.

    Args:
    X: An [n_samples, n_features] array of data to cluster.
    n_clusters: Number of clusters to form.
    max_iter: Maximum number of EM iterations to perform.

    Returns:
    labels: An [n_samples] vector of integer labels.
    centroids: An [n_clusters, n_features] array of cluster centroids.
    X_hat: Copy of X with the missing values filled in.
    """

    print("X = {}".format(str(X)))
    
    # Initialize missing values to their column means
    missing = ~np.isfinite(X)
    mu = np.nanmean(X, 0, keepdims=1)
    X_hat = np.where(missing, mu, X)
    # print("Before: {}".format(str(X_hat)))
    X_hat = np.nan_to_num(X_hat, False)
    # print("After: {}".format(str(X_hat)))

    for i in range(max_iter):
        if i > 0:
            # initialize KMeans with the previous set of centroids. this is much
            # faster and makes it easier to check convergence (since labels
            # won't be permuted on every iteration), but might be more prone to
            # getting stuck in local minima.
            cls = cluster.MiniBatchKMeans(n_clusters, init=prev_centroids)
        else:
            # do multiple random initializations in parallel
            cls = cluster.MiniBatchKMeans(n_clusters)

        # perform clustering on the filled-in data
        labels = cls.fit_predict(X_hat)
        centroids = cls.cluster_centers_

        # fill in the missing values based on their cluster centroids
        X_hat[missing] = centroids[labels][missing]

        # when the labels have stopped changing then we have converged
        if i > 0 and np.all(labels == prev_labels):
            break

        prev_labels = labels
        prev_centroids = cls.cluster_centers_

    return labels, centroids, X_hat

def do_clustering(input_images, lpis_buffered_raster, period, num_imgs, num_clusters, out_file_cluster) :
    img_ds_input_img = [s for s in input_images if "_B08_" in os.path.basename(s)]
    img_ds = gdal.Open(img_ds_input_img[0],gdal.GA_ReadOnly)
    raster = gdal.Open(lpis_buffered_raster,gdal.GA_ReadOnly)
    imgR = raster.ReadAsArray()
    num_bands = img_ds.RasterCount
    
    # for p in Period_l:
    #outputs
    # file_cluster = f'{imG_path}/L4D_{site}_{n_cl}clusters_{p}_TMP.tif'
    # file_out_post = f'{imG_path}/L4D_{site}_{n_cl}clusters_isoP_{p}.tif'
    # file_out_post2 = f'{imG_path}/L4D_{site}_{n_cl}clusters_isoP_{p}_Com.tif'
    date_l = []
    for i in range(1,num_imgs+1):
        # protection for the last period when we might not have all dates in raster
        print("Date to add: {}".format(i + (period-1)*num_imgs))
        if (i + (period-1)*num_imgs <= num_bands):
            date_l.append(i + (period-1)*num_imgs)
        
    # date_l = [i + (period-1)*num_imgs for i in range(1,num_imgs+1)]
    if len(date_l) == 0 : 
        print("Cannot compute dates list from the given parameters (num_imgs = {}, num_bands = {}, period = {})".format(num_imgs, num_bands, period))
        # write a image with only zeroes in order to avoid further steps failure. This kind of images will be ignored further in the analysis
        img = np.zeros((img_ds.RasterYSize,img_ds.RasterXSize),
                    gdal_array.GDALTypeCodeToNumericTypeCode(img_ds.GetRasterBand(1).DataType))
        print(img.shape)
        ds = gdal.Open(lpis_buffered_raster)
        write_geotiff(out_file_cluster,img,ds)
        
        sys.exit(0)

    n_var = len(input_images)*len(date_l)

    img = np.zeros((img_ds.RasterYSize,img_ds.RasterXSize,n_var),
                gdal_array.GDALTypeCodeToNumericTypeCode(img_ds.GetRasterBand(1).DataType))

    print("Input images len = {}".format(len(input_images)))
    print("date_l = {}".format(date_l))
    print("n_var = {}".format(n_var))
    
    for d in range(len(date_l)):
        date = date_l[d]
        print('imported date ' + str(date))
        for b in range(len(input_images)):
            img_ds = gdal.Open(input_images[b],gdal.GA_ReadOnly)
            if img_ds is not None: 
                # protection for last month where we can have less bands (ex. Feb)
                if date <= img_ds.RasterCount : 
                    img_dsB = img_ds.GetRasterBand(date)
                    img_dsA = img_dsB.ReadAsArray()

                    img[:, :,(d*len(input_images))+b] = img_dsA
                    print("Extracted image from file {}".format(input_images[b]))

    print('images imported')
    #test mask crop with remove
    img0 = img
    img = img0[imgR!=0,:]

    print(img)
    img = img.astype(float)
    img[img==-10000] = np.nan
    img[img==0] = np.nan
    
    print(img)

    t_missing = kmeans_missing(X = img,n_clusters=num_clusters,max_iter=10)
    
    raster = gdal.Open(lpis_buffered_raster,gdal.GA_ReadOnly)
    imgR = raster.ReadAsArray()

    t_cluster = t_missing[0]

    newshpR = (imgR.shape[0]*imgR.shape[1])
    imgR0  = imgR[:,:].reshape(newshpR)

    X_clusterOut = t_cluster +1
    imgR0[imgR0!=0]=X_clusterOut

    t_clusterE = imgR0.reshape(img_dsA.shape)

    ds = gdal.Open(lpis_buffered_raster)

    write_geotiff(out_file_cluster,t_clusterE,ds)

def main():
    parser = argparse.ArgumentParser(
        description="Parcels cluster extraction"
    )
    
    parser.add_argument("-i", "--input-images", nargs='+', help="List of input image containing the resampled images", required=True)
    parser.add_argument("-b", "--lpis-buffered-raster", help="LPIS buffered raster corresponding to the input image tile", required=True)
    parser.add_argument("-c", "--number-of-clusters", type=int, help="Number of clusters", required=False, default=4)
    parser.add_argument("-n", "--number-of-images", type=int, help="Number of images to use in the period (if month: 3 images of 10 days resampled S2)", required=False, default=3)
    parser.add_argument("-p", "--period", type=int, help="Clustering period index (1 to 12 months, for example)", required=True)
    parser.add_argument("-o", "--output", help="Output cluster file", required=True)

    args = parser.parse_args()
    
    do_clustering(args.input_images, args.lpis_buffered_raster, args.period, args.number_of_images, args.number_of_clusters, args.output)
    
if __name__ == "__main__":
    main()
