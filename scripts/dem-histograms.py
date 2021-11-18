#!/usr/bin/env python
import logging
import psycopg2
import pipes
import subprocess
import multiprocessing.dummy
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
            stdout = open(self.stdout, "w")
            subprocess.call(args, env=self.env, stdout=stdout)
        else:
            subprocess.call(args, env=self.env)


conn = psycopg2.connect(dbname="postgres")

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
        args += ["-srcnodata", "0", "-dstnodata", "0"]
        args += ["-t_srs", "EPSG:" + str(epsg_code)]
        args += ["-overwrite", "-te"]
        args += [int(ll[0]), int(ll[1])]
        args += [int(ur[0]), int(ur[1])]
        args += ["world.vrt", vrt]
        cmd = Command(args, None, None)
        vrt_cmds.append(cmd)

        args = ["gdalinfo", "-json", "-hist", vrt]
        cmd = Command(args, None, json)
        json_cmds.append(cmd)

pool = multiprocessing.dummy.Pool()
pool.map(lambda c: c.run(), vrt_cmds)
pool.map(lambda c: c.run(), json_cmds)
