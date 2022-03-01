#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor
import logging
import psycopg2
import pipes
import subprocess
from osgeo import ogr


class Command(object):
    def __init__(self, args, env, stdout):
        self.args = args
        self.env = env
        self.stdout = stdout

    def run(self):
        args = list(map(str, self.args))
        cmd_line = " ".join(map(pipes.quote, args))
        logging.debug(cmd_line)
        if self.stdout:
            with open(self.stdout, "w") as stdout:
                subprocess.call(args, env=self.env, stdout=stdout)
        else:
            subprocess.call(args, env=self.env)


conn = psycopg2.connect(dbname="postgis")

vrt_cmds = []
json_cmds = []
with conn.cursor() as cursor:
    cursor.execute(
        "select tile_id, ST_AsBinary(ST_Envelope(ST_SnapToGrid(St_Transform(geom, epsg_code), 1))), epsg_code from shape_tiles_s2"
    )
    for (tile_id, geom, epsg_code) in cursor:
        geom = ogr.CreateGeometryFromWkb(geom)
        print(geom.ExportToWkt())
        ring = geom.GetGeometryRef(0)
        ll = ring.GetPoint(0)
        ur = ring.GetPoint(2)

        vrt = tile_id + ".vrt"
        json = tile_id + ".json"
        args = ["gdalwarp"]
        args += ["-r", "average"]
        args += ["-srcnodata", "-32768", "-dstnodata", "-32768"]
        args += ["-t_srs", "EPSG:" + str(epsg_code)]
        args += ["-overwrite", "-te"]
        args += [int(ll[0]), int(ll[1])]
        args += [int(ur[0]), int(ur[1])]
        args += ["world.vrt", vrt]
        cmd = Command(args, None, None)
        vrt_cmds.append(cmd)

        args = ["gdalinfo", "-stats", "-json", vrt]
        cmd = Command(args, None, json)
        json_cmds.append(cmd)

with ThreadPoolExecutor(max_workers = 64) as executor:
    executor.map(lambda c: c.run(), vrt_cmds)
    executor.map(lambda c: c.run(), json_cmds)
