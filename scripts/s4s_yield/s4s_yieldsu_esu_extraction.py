#!/usr/bin/env python
from __future__ import print_function

import argparse
import subprocess
import glob
import os
import rasterio
import numpy as np
import pandas as pd
import sys
import pipes

class Config(object):
    def __init__(self, args):
        self.su_path = args.su_path
        self.su_unique_id = args.su_unique_id
        self.crop_type_path = args.crop_type_path
        self.tiles = args.tiles
        self.working_dir = args.working_dir
        self.out_tile_rasters = args.out_tile_rasters
        self.output = args.output
        self.tile_rasters_dict = dict()
        if len(self.out_tile_rasters) > 0 :
            if len(self.out_tile_rasters) != len(self.tiles):
                print("Output tiles rasters provided but they differ in lenght from the number of provided tiles {} <> {}".format(len(self.out_tile_rasters), len(self.tiles)))
                sys.exit(1)
            
            idx = 0
            for tile in self.tiles:
                if not tile in self.out_tile_rasters[idx]:
                    print("Output raster {} doesn't contain the tile name {} within its name !!!".format(self.out_tile_rasters[idx], tile))
                self.tile_rasters_dict[tile] = self.out_tile_rasters[idx]
                idx = idx + 1
        else :
            for tile in self.tiles:
                raster_name = 'ESU_Random_' + tile + '.tif'
                raster_path = os.path.join(self.working_dir, raster_name)
                self.tile_rasters_dict[tile] = raster_path

def run_command(args, env=None, retry=False):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)

    retries = 5 if retry else 1
    ret_res = -1
    while retries > 0:
        retries -= 1
        print(cmd_line)
        if env:
            result = subprocess.call(args, env=env)
        else:
            result = subprocess.call(args)
        
        ret_res = result
        if result != 0:
            print("Exit code: {}".format(result))
        else:
            break
            
    return ret_res

def run_command_get_output(args, env=None, retry=False):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)

    return subprocess.getoutput(cmd_line)

def get_crop_type_rasters(crop_type_path, tile):
    print("Extracting crop type rasters from {} for tile {}".format(crop_type_path, tile))
    filtering_path = "{}/VECTOR_DATA/classified_{}.tif".format(crop_type_path, tile)
    print("Using filter {}".format(filtering_path))
    ct_rasters = glob.glob(filtering_path)
    if len(ct_rasters) > 0:
        return ct_rasters[0]
    
    return ""

def parcel_extraction(config):

    #Creation of an ESU dataframe to give an SU, a Tile, a Crop and the number of pixels for each ESU created
    ESUtable=pd.DataFrame(columns=['ESUid','SUid','Tile','CropID','NbrPix'])
    ESUid=1
    for tile in config.tiles :                              # Iteration of each tiles
        s2_tile_classif_file = get_crop_type_rasters(config.crop_type_path, tile)
        if len(s2_tile_classif_file) == 0:
            print("Cannot find a classification file in {} for tile {}".format(config.crop_type_path, tile))
            continue
        print("Found classification file {}".format(s2_tile_classif_file))
        su_tile_file_name = 'SU_' + tile + '.tif'
        SUTilePath = os.path.join(config.working_dir, su_tile_file_name)
        if not os.path.exists(SUTilePath) : # Creation of the Raster presenting the SU for each S2 Tile. To be apply once (not needed to be replicated each year)
            print('### Rasterize SU of TILE ' + tile + ' ###')
            info = run_command_get_output(['gdalinfo', s2_tile_classif_file])
            
            EpsgCode = info.split('"EPSG",')[-1].split(']')[0]  # Retrieve the projection of the classif for the tile (can be different according to the tiles)
            EpsgCode = EpsgCode.replace('"', '')
            Projt = 'EPSG:' + EpsgCode
            Lowerleft = info.split("Lower Left  ( ")[-1].split(')')[0]  # Retrieve the extend of the tile for the creation of the new raster
            Upperright = info.split("Upper Right ( ")[-1].split(')')[0]

            out_shp_tile_file_name = 'SU_' + EpsgCode + '.shp'
            out_shp = os.path.join(config.working_dir, out_shp_tile_file_name)
            if not os.path.exists(out_shp):
                cmd = [ "ogr2ogr", "-t_srs", Projt, out_shp, config.su_path]
                res = run_command(cmd)
                if res != 0 : 
                    sys.exit(res)
            
#            Extendt = (str(round(float(Lowerleft.split(',')[0]))) + ' ' + 
#                      str(round(float(Lowerleft.split(',')[1]))) + ' ' + 
#                      str(round(float(Upperright.split(',')[0]))) + ' ' + 
#                      str(round(float(Upperright.split(',')[1]))))
            
            layer_name = 'SU_' + EpsgCode
            cmd = [ "gdal_rasterize", "-a", config.su_unique_id, 
                    "-l", 'SU_' + EpsgCode, 
                    "-a_nodata", "-1000", "-a_srs", Projt, 
                    "-te", 
                    round(float(Lowerleft.split(',')[0])), 
                    round(float(Lowerleft.split(',')[1])),
                    round(float(Upperright.split(',')[0])),
                    round(float(Upperright.split(',')[1])),
                    "-tr", "10", "10",
                    "-ot", "Int16", out_shp, SUTilePath]

            #cmd = [ "gdal_rasterize", "-a", "ID_2", 
            #        "-l", 'SU_' + EpsgCode, 
            #        "-a_nodata", "-1000", "-a_srs", Projt, "-te", Extendt, "-tr", "10", "10",
            #        "-ot", "Int16", out_shp, SUTilePath]

            res = run_command(cmd)
            if res != 0 : 
                sys.exit(res)
        
        # Open images 
        SU = rasterio.open(SUTilePath)                      #open the SU raster just created
        SUb = SU.read().reshape(SU.shape[0]*SU.shape[1])
        listSU=np.unique(SUb)                               # list SU id present on the tile

        Class = rasterio.open(s2_tile_classif_file).read().reshape(SU.shape[0]*SU.shape[1])    # Open the Crop Type map of the tile
        listclass=np.unique(Class)[np.unique(Class)>10]                     #list the crop present on the tile (The class of "crops" are >10, <10 is other land use)

        print('### Create ESU of TILE ' + tile + ' ###')
        ESUmapPix= np.zeros(np.shape(SUb))
        for c in listclass:
            for su in listSU:
                locESU= np.intersect1d(np.where(Class==c),np.where(SUb==su))    # Find location of each ESU on the tile
                if len(locESU)<5000:
                    continue
                ESUmapPix[np.random.choice(locESU,5000,replace=False)]=ESUid    # Select randomly 5000 pixel of each ESU
                # Write a table to record information for the aggregation at the end of the process
                print("Adding data ESUid = {}, SUid = {}, CropID = {}".format(ESUid, su, c))
                ESUtable=pd.concat([ESUtable, pd.DataFrame(data={'ESUid':[ESUid],'SUid':[su],'Tile':[tile],'CropID':[c],'NbrPix':[len(locESU)]})])   
                ESUid+=1

        raster_path = config.tile_rasters_dict[tile]
        new_dataset = rasterio.open(
            raster_path,
            'w',
            driver='GTiff',
            height=SU.shape[0],
            width=SU.shape[1],
            count=1,
            dtype=SU.dtypes[0],
            crs=SU.crs,
            transform=SU.transform,
            nodata=0
        )# Writing of the new raster containing the 5000 pixels selcted for each ESU, the "pseudo LPIS" where the feature extaction will take place 
        new_dataset.write(ESUmapPix.reshape(SU.shape[0],SU.shape[1]),1)
        new_dataset.close()
        
    # Compute the weight of each ESU in their respective SU    
    ESUtable.reset_index(inplace=True)
    ESUtable['Weight']=np.zeros(len(ESUtable))
    for i in range(len(ESUtable)):
        ESUtable['Weight'][i]=ESUtable['NbrPix'][i]/np.sum(ESUtable['NbrPix'][np.logical_and(ESUtable['SUid']==ESUtable['SUid'][i],ESUtable['CropID']==ESUtable['CropID'][i])])
    ESUtable.to_csv(config.output, index=False)


def main():
    parser = argparse.ArgumentParser(description="Yield SU Parcels Extraction")
    parser.add_argument('-c', '--crop-type-path', help="Crop type product path")
    parser.add_argument('-t', '--tiles', help="SU shapefile path", nargs="+")
    parser.add_argument('-u', '--su-path', help="SU shapefile path")
    parser.add_argument('-n', '--su-unique-id', help="SU unique id column name in the input SU shapefile",default="ID_2")
    parser.add_argument('-w', '--working-dir', help="Working dir")
    parser.add_argument('-r', '--out-tile-rasters', help="Output tiles rasters. Should have the same size as the tiles", nargs="+")
    parser.add_argument('-o', '--output', help="ESU CSV file output path")
    
    args = parser.parse_args()

    config = Config(args)
    
    parcel_extraction(config)

if __name__ == "__main__":
    main()
