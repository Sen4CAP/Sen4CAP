#!/usr/bin/env python
from __future__ import print_function

import argparse
from configparser import ConfigParser
import os
import os.path
import glob
import pipes
import psycopg2
from psycopg2.sql import SQL, Literal
import psycopg2.extras
import subprocess
from shutil import copyfile

import base64
import struct
import io
import json
import re

from osgeo import gdal,gdalconst
from osgeo import ogr
from osgeo import osr

import sys
import docker
from docker.types import LogConfig

agric_codes = [2, 21, 22, 23, 24, 211, 212, 213, 221, 222, 223, 231, 241, 242, 243, 244]

class Config(object):
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")
        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

        # work around Docker networking scheme
        if self.host == "127.0.0.1" or self.host == "::1" or self.host == "localhost":
            self.host = "172.17.0.1"

        self.site_id = args.site_id

def run_command(args, env=None):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))
    print(cmd_line)
    subprocess.call(args, env=env)

# def run_command_out_to_file(args, file_path):
#     args = list(map(str, args))
#     cmd_line = " ".join(map(pipes.quote, args))
#     print(cmd_line)
#     f = open(file_path, "w")
#     subprocess.call(args, stdout=f)

def getSiteTiles(conn, site_id):
    site_tiles = ""
    with conn.cursor() as cursor:
        query = SQL(
            """
            select tiles from site_tiles where satellite_id = 1 and site_id = {}
            """
        )
        query = query.format(Literal(site_id))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            site_tiles = row[0]
        conn.commit()
    return site_tiles

def getTileEPSGCode(conn, s2_tile):
    value = ""
    with conn.cursor() as cursor:
        query = SQL(""" select epsg_code from shape_tiles_s2 where tile_id = {} """)
        query = query.format(Literal(s2_tile))
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            value = row[0]
        conn.commit()

    return value

def getTileWKT(conn, s2_tile, epsg_code = ""):
    wkt = ""
    epsg = ""
    with conn.cursor() as cursor:
        if epsg_code == "" : 
            query = SQL(""" select st_astext(st_transform(geom, epsg_code),0), epsg_code from shape_tiles_s2 where tile_id = {} """)
            query = query.format(Literal(s2_tile))
        else :
            query = SQL(""" select st_astext(st_transform(geom, {})), epsg_code from shape_tiles_s2 where tile_id = {} """)
            query = query.format(Literal(epsg_code), Literal(s2_tile))
            
        print(query.as_string(conn))

        cursor.execute(query)
        for row in cursor:
            wkt = row[0]
            epsg = row[1]
        conn.commit()
    
    print("Tile {} -> WKT = {}, initial EPSG = {}, requested EPSG = {}".format(s2_tile, wkt, epsg, epsg_code))
    return wkt, epsg

def queryEarthSignature(start_date, end_date, wkt, output_dir, out_path) :
    # output_dir = os.path.dirname(output_dir)
    print("Querying EarthSignature ...")
    
    client = docker.from_env()
    volumes = {
        # self.input: {"bind": self.input, "mode": "ro"},
        output_dir: {"bind": output_dir, "mode": "rw"},
    }
    lc = LogConfig(type=LogConfig.types.JSON, config={
        'max-size': '1g'
    })
    
    if end_date is not None:
        msg = "{{\"n_results\": \"10\", \"start_date\": \"{}\", \"end_date\": \"{}\", \"wkt\": \"{}\" }}".format(start_date, end_date, wkt)
    else :
        msg = "{{\"n_results\": \"10\", \"start_date\": \"{}\", \"wkt\": \"{}\" }}".format(start_date, wkt)
    command = []
#    command += ["-H", "apikey: xLNnvdiAmNK0rChLk0YnUP7v"]
    # command += ["-insecure"]
    command += ["-max-msg-sz", "8000000000"]
    command += ["-d", msg]
    command += ["earthsignature.snapearth.eu:443"]
##    command += ["earthsignature.snapearth.csgroup.space:443"]
    command += ["snapearth.api.v1.DatabaseProductService.ListSegmentation"]
    # command += ["snapearth.api.v1.database.DatabaseProductService.ListSegmentation"]
    print ("Executing : docker run fullstorydev/grpcurl {}".format(" ".join(command)))
    container = client.containers.run(
        image="fullstorydev/grpcurl",
        remove=True,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        command=command,
        log_config=lc
    )
    client.close()

    f = open(out_path, "wb")
    f.write(container)
    f. close()
    
    # ------------ Old implementation -----
    # command = []
    # command += ["docker", "run", "--rm"]
    # command += [
    #     "-v",
    #     "{}:{}".format(
    #         output_dir,
    #         output_dir,
    #     ),
    # ]
    # msg = "{{\"n_results\": \"1\", \"start_date\": \"{}\", \"wkt\": \"{}\" }}".format(start_date, wkt)
    # command += ["-u", "{}:{}".format(os.getuid(), os.getgid())]
    # command += ["fullstorydev/grpcurl"]
    # command += ["-d", msg]
    # command += ["earthsignature.snapearth.eu:443"]
    # command += ["snapearth.api.v1.database.DatabaseProductService.ListSegmentation"]
    # run_command_out_to_file(command, out_path)

def filter_pixels_in_raster(in_raster, out_dir, out_raster, valid_vals) :
    client = docker.from_env()
    volumes = {
        in_raster: {"bind": in_raster, "mode": "ro"},
        out_dir: {"bind": out_dir, "mode": "rw"},
    }
    command = []
    command += ["otbcli", "BandMath", "-il", in_raster]
    command += ["-out", out_raster, "int16"]
    exp = "("
    for i in range(0, len(valid_vals)):
        if i > 0 :
            exp += " || "
        exp += "im1b1 == {} ".format(valid_vals[i])
    exp += ") ? im1b1 : 0"
    
    command += ["-exp", exp]
    client.containers.run(
        image="sen4cap/processors:3.1.0",
        remove=True,
        user=f"{os.getuid()}:{os.getgid()}",
        volumes=volumes,
        command=command,
    )
    client.close()

def decode_base64(in_file, out_file) :
    # print("Decoding base64 file {} into {}".format(in_file, out_file))
    print("Decoding base64 file ...")
    image = open(in_file, 'rb')
    image_read = image.read()
    image_64_decode = base64.decodebytes(image_read) 
    try:
        image_64_decode = base64.b64decode(image_64_decode, validate=True)
        print("Content of file {} is doubled base64 encoded. Decoded it twice ...".format(in_file))
    except Exception as e:
        print("No double Base64 encoding for file {} ...".format(in_file))
    
    image_result = open(out_file, 'wb') 
    # print("Decoded buffer has a length of {} bytes".format(len(image_64_decode)))
    # print("Decoded buffer is : ")
    # image_64_decode_io = io.BytesIO(image_64_decode)
    # my_byte = image_64_decode_io.read()
    # while my_byte != "" :
    #     # print("{} + ".format(my_byte.encode('hex')))
    #     my_byte = image_64_decode_io.read()
    #     # image_result.write(my_byte)
    # 
    image_result.write(image_64_decode)
    image_result.close()

def extract_json_files(jsonl_file, output_dir) :
    cur_out_file = ""
    json_files = []
    json_file_idx = 1
    with open(jsonl_file) as file:
        is_in_json = False
        write_file = None
        for line in file:
            line = line.rstrip("\r\n")
            # print(line)
            if line.startswith('{') :
                is_in_json = True 
                cur_out_file = os.path.join(output_dir, "response_{}.json".format(json_file_idx))
                try:
                    write_file = open(cur_out_file, "a")
                    write_file.seek(0, 0)
                    write_file.truncate()
                except IOError:
                    print("Could not open file {} !".format(cur_out_file))
                    sys.exit(1)
                json_file_idx = json_file_idx+1

            if is_in_json == True : 
                write_file.write(line)
                
            if line.endswith('}') :
                is_in_json = False
                write_file.close()
                json_files += [cur_out_file]                

    return json_files

def extract_base64_segmentation_file(json_file, out_dir, s2_tile_filter) :
    ret_files = []
    with open(json_file) as jsonFile:
        jsonObject = json.load(jsonFile)

        segmentation = jsonObject["segmentation"]
        productId = jsonObject["productId"]
        print("PRODUCT_ID = {}".format(productId))
        
        s2tile = ""
        s2_tile_re = re.compile("S2[A-D]_MSIL.*_\d{8}T\d{6}_N\d{4}_R\d{3}_T(\d{2}[A-Z]{3})_\d{8}T\d{6}(?:\.SAFE)?")
        m = s2_tile_re.search(productId)
        if m:
            s2tile = m.group(1)
            print("S2TILE = {}".format(s2tile))

        # filter and keep only the tile we are processing, in case other tiles are retrieved
        if s2tile == s2_tile_filter:
            cloudCover = jsonObject["cloudCover"]
            out_file = os.path.join(out_dir, "{}.base64".format(productId))
            with open(out_file, 'w') as f:
                f.write(segmentation)
                ret_files += [out_file]
            jsonFile.close()
        
    return ret_files
    
def create_path(dir_path) :
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def GetTileExtent(conn, s2tile):
    wkt, epsg = getTileWKT(conn, s2tile)
    print("Tile WKT = {}, EPSG = {}".format(wkt, epsg))
    geom = ogr.CreateGeometryFromWkt(wkt)
    # print("geom = {}".format(geom))
    # extent = geom.GetEnvelope()
    # print("extent = {}".format(extent))

    # print("geometries cnt = {}".format(geom.GetGeometryCount()))

    points = []
    for i in range(0, geom.GetGeometryCount()):
        g = geom.GetGeometryRef(i)
        for j in range(0, g.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            pt = g.GetPoint(j)
            points += [pt]
    return points

def minimize_tile_wkt(wkt_utm):
    print("WKT UTM = {}".format(wkt_utm))   
    geom = ogr.CreateGeometryFromWkt(wkt_utm)
    points = []
    for i in range(0, geom.GetGeometryCount()):
        g = geom.GetGeometryRef(i)
        for j in range(0, g.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            pt = g.GetPoint(j)
            x_offset = 10000 if (j % 3 == 0) else -10000
            y_offset = -10000 if (j <= 1) else 10000
            new_pt = (pt[0] + x_offset, pt[1] + y_offset)
            points += [new_pt]
    wkt_minimized = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                                                    points[0][0], points[0][1], 
                                                    points[1][0], points[1][1], 
                                                    points[2][0], points[2][1], 
                                                    points[3][0], points[3][1],
                                                    points[0][0], points[0][1])
    print("WKT minimized = {}".format(wkt_minimized))    
    return wkt_minimized

def invert_wkt_lat_lon(wkt):
    print("WKT To Be Inverted = {}".format(wkt))   
    geom = ogr.CreateGeometryFromWkt(wkt)
    points = []
    for i in range(0, geom.GetGeometryCount()):
        g = geom.GetGeometryRef(i)
        for j in range(0, g.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            pt = g.GetPoint(j)
            new_pt = (pt[1], pt[0])
            points += [new_pt]
    wkt_inverted = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                                                    points[0][0], points[0][1], 
                                                    points[1][0], points[1][1], 
                                                    points[2][0], points[2][1], 
                                                    points[3][0], points[3][1],
                                                    points[0][0], points[0][1])
    print("WKT Inverted = {}".format(wkt_inverted))    
    return wkt_inverted

def reproject_tile_wkt(wkt, src_epsg, target_epsg):
    source = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)

    target = osr.SpatialReference()
    target.ImportFromEPSG(target_epsg)
    
    polygon = ogr.CreateGeometryFromWkt(wkt)
    
    transform = osr.CoordinateTransformation(source, target)
    polygon.Transform(transform)

    ret_wkt = polygon.ExportToWkt()
    ret_wkt = invert_wkt_lat_lon(ret_wkt)
    print("WKT reprojected = {}".format(ret_wkt))  
    return ret_wkt

def do_georeference_raster(conn, s2tile, in_raster, out_raster) :
   
    print("Georeferencing segmentation file {} into {}".format(in_raster, out_raster))
    
    points = GetTileExtent(conn, s2tile)
    # print ("Tile extent: {}".format(points))
    ulx = points[0][0]
    uly = points[0][1]
    lrx = points[2][0]
    lry = points[2][1]
    tile_epsg = getTileEPSGCode(conn, s2tile)

    command = []
    command += ["gdal_translate", "-a_ullr", ulx, uly, lrx, lry]
    command += ["-ot", "Int16"]
    command += ["-a_srs", "EPSG:{}".format(tile_epsg)]
    command += [in_raster, out_raster]

    run_command(command)
    
def update_shape_codes(in_shp) :
    dataset = ogr.Open(in_shp, gdalconst.GA_Update)
    layer = dataset.GetLayer()
    feature_count = layer.GetFeatureCount()
    print("{} feature(s) found".format(feature_count)) 
    
    dataset.StartTransaction()

    crop_field_idx = layer.FindFieldIndex("CROP", False)
    if crop_field_idx != -1:
        print("Field CROP already exists!")
    else :
        layer.CreateField(ogr.FieldDefn("CROP", ogr.OFTInteger))
        crop_field_idx = layer.FindFieldIndex("CROP", False)
    
    code_field_idx = layer.FindFieldIndex("CODE", False)
    for feature in layer:
        code_val = feature.GetField("CODE")
        if code_val in agric_codes : 
            # leave the code value as it was, set only the crop value
            feature.SetField(crop_field_idx, 1)
        else :
            feature.SetField(crop_field_idx, 0)
            feature.SetField(code_field_idx, code_val)
        layer.SetFeature(feature)

    dataset.CommitTransaction()
    
def polygonize_rasters(rasters, out_file, working_dir):
    # first create a VRT with all the rasters
    vrt_file = os.path.join(working_dir, "all_tile_segmentation_rasters.vrt")
    command = []
    command += ["gdalbuildvrt", "-b", "1", "-srcnodata", "0"]
    command += [vrt_file]
    command += rasters
    run_command(command)

    out_dir = os.path.dirname(out_file)
    layer_name = os.path.splitext(os.path.basename(out_file))[0]
    
    fileList = glob.glob(os.path.join(out_dir, layer_name + ".*"))
    for filePath in fileList:
        try:
            print ("Removing already existing file {}".format(filePath))
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)
        
    # now polygonize the rasters
    command = []
    command += ["gdal_polygonize.py", vrt_file]
    command += ["-f", "ESRI Shapefile"]
    command += [out_dir, layer_name, "CODE"]
    
    run_command(command)
    
    # change the values of the codes and add also a column CROP (0/1)
    update_shape_codes(out_file)
    
def main():
    parser = argparse.ArgumentParser(
        description="Retrieves EarthSignature insitu data"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="Configuration file location",
    )
    parser.add_argument(
        "-s",
        "--site-id",
        required=True,
        help="Site id for which the file was uploaded",
    )
    parser.add_argument(
        "-w", "--working-dir", required=True, help="Working directory"
    )
    parser.add_argument(
        "-b", "--start-date", required=False, help="The start date"
    )

    parser.add_argument(
        "-e", "--end-date", required=False, help="The end date"
    )
    
    parser.add_argument(
        "-o", "--out-shp", required=True, help="The output shp file"
    )

    args = parser.parse_args()

    config = Config(args)

    with psycopg2.connect(
        host=config.host,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        # site_wkt = getSiteWKT(conn, config.site_id)
        site_tiles = getSiteTiles(conn, config.site_id)
        georef_segm_files = []
        for s2tile in site_tiles:

            print("Tile = {}".format(s2tile))
            tile_wkt_wgs84 = getTileWKT(conn, s2tile, 4326)
            tile_wkt_utm, epsg = getTileWKT(conn, s2tile)
            tile_wkt_utm = minimize_tile_wkt(tile_wkt_utm)
            tile_wkt = reproject_tile_wkt(tile_wkt_utm, epsg, 4326)
            
            s2tile_work_dir = os.path.join(args.working_dir, s2tile);
            create_path(s2tile_work_dir)
                
            # site_wkt = "MULTIPOLYGON (((1.6415459999999999 48.6570210000000003, 2.5066069999999998 48.6616240000000033, 2.5143049999999998 48.6782680000000028, 2.5817120000000000 48.8229529999999983, 2.6498390000000001 48.9676520000000011, 2.7181340000000001 49.1124610000000033, 2.7869250000000001 49.2572189999999992, 2.8559869999999998 49.4018480000000011, 2.9250820000000002 49.5464559999999992, 2.9757820000000001 49.6517830000000018, 1.6142719999999999 49.6444299999999998, 1.6415459999999999 48.6570210000000003)))"
            # site_wkt = "MULTIPOLYGON(((13.8407544343395 48.8698778299916,13.8301934869479 49.2557675230918,14.4620185534293 49.2124261409465,14.4206837170399 48.8286933623671,13.8407544343395 48.8698778299916)))"
            es_out_file_path = os.path.join(s2tile_work_dir, "earth_signature_response.txt")
            queryEarthSignature(args.start_date, args.end_date, tile_wkt, s2tile_work_dir, es_out_file_path)
            
            # Extract individual jsons from json lines received from EarthSignature
            jsons_dir = os.path.join(s2tile_work_dir, "tiles_jsons")
            create_path(jsons_dir)
            extracted_json_files = extract_json_files(es_out_file_path, jsons_dir)
            
            # extracting individual base64 segmentation files from individual json files    
            segm_files_dir = os.path.join(s2tile_work_dir, "segmentation_files")
            create_path(segm_files_dir)
            base64_seg_files = [] 
            for json_file in extracted_json_files :
                base64_seg_files += extract_base64_segmentation_file(json_file, segm_files_dir, s2tile)
            
            # extracting tiff files from the base64 json files
            segm_files = []
            for base64_seg_file in base64_seg_files :
                out_seg_file = os.path.join(segm_files_dir, "{}.tif".format(os.path.splitext(os.path.basename(base64_seg_file))[0]))
                decode_base64(base64_seg_file, out_seg_file)
                segm_files += [out_seg_file]

            # Extracting the georeferenced files
            georef_files_dir = os.path.join(s2tile_work_dir, "georeferenced_files")
            create_path(georef_files_dir)
             
            for segm_file in segm_files:
                try :
                    base_fn = os.path.splitext(os.path.basename(segm_file))[0]
                    out_georef_segm_file = os.path.join(georef_files_dir, "{}_GEOREF.tif".format(base_fn))
                    do_georeference_raster(conn, s2tile, segm_file, out_georef_segm_file)
                    out_seg_filtered_file = os.path.join(georef_files_dir, "{}_GEOREF_filtered.tif".format(base_fn))
                    filter_pixels_in_raster(out_georef_segm_file, georef_files_dir, out_seg_filtered_file, agric_codes)
                    georef_segm_files += [out_seg_filtered_file] 
                except:
                    print("Error processing segmentation file {}".format(segm_file))

        polygonize_rasters_dir = os.path.join(args.working_dir, "polygonize")
        create_path(polygonize_rasters_dir)
            
        polygonize_rasters(georef_segm_files, args.out_shp, polygonize_rasters_dir) 
        # decode_base64(base64_seg_file, "/home/cudroiu/sen2agri/scripts/earth_signature/base64_decoded_file.txt")


if __name__ == "__main__":
    main()
