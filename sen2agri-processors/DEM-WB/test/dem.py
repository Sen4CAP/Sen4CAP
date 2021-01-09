#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_____________________________________________________________________________

   Program:      Sen2Agri-Processors
   Language:     Python
   Copyright:    2015-2016, CS Romania, office@c-s.ro
   See COPYRIGHT file for details.

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
_____________________________________________________________________________

"""
try:
    import argparse
    import datetime
    import re
    import glob
    from osgeo import gdal, osr
    import logging
    import lxml.etree
    from lxml.builder import E
    import math
    import os
    from os.path import isdir, join
    import pipes
    import subprocess
    import sys
    from signal import signal, SIGINT, SIG_IGN
    import time
    from multiprocessing import Pool, TimeoutError
except Exception as e:
    print(e)

print("Starting DEM script")


try:
    from typing import List
except ModuleNotFoundError:
    pass


def GetExtent(gt, cols, rows):
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + px * gt[1] + py * gt[2]
            y = gt[3] + px * gt[4] + py * gt[5]
            ext.append([x, y])
        yarr.reverse()
    return ext


def run_command(cmd_array, log_path="", log_filename="", fake_command=False):
    start = time.time()
    cmd_array = list(map(str, cmd_array))
    cmd_str = " ".join(map(pipes.quote, cmd_array))
    logging.debug("Running command: {}".format(cmd_str))
    res = 0
    if not fake_command:
        try:
            res = subprocess.call(cmd_array, shell=False)
        except Exception as e:
            logging.error(e)
            return 1
    ok = "OK"
    nok = "NOK"
    logging.debug("Command finished {} (res = {}) in {} : {}".format((ok if res == 0 else nok), res, datetime.timedelta(seconds=(time.time() - start)), cmd_str))
    return res


def resample_dataset(src_file_name, dst_file_name, dst_spacing_x, dst_spacing_y):
    dataset = gdal.Open(src_file_name, gdal.gdalconst.GA_ReadOnly)

    src_x_size = dataset.RasterXSize
    src_y_size = dataset.RasterYSize

    src_geo_transform = dataset.GetGeoTransform()
    (ulx, uly) = (src_geo_transform[0], src_geo_transform[3])
    (lrx, lry) = (
        src_geo_transform[0] + src_geo_transform[1] * src_x_size,
        src_geo_transform[3] + src_geo_transform[5] * src_y_size,
    )

    dst_x_size = int(round((lrx - ulx) / dst_spacing_x))
    dst_y_size = int(round((lry - uly) / dst_spacing_y))

    dst_geo_transform = (
        ulx,
        dst_spacing_x,
        src_geo_transform[2],
        uly,
        src_geo_transform[4],
        dst_spacing_y,
    )

    (ulx, uly) = (dst_geo_transform[0], dst_geo_transform[3])
    (lrx, lry) = (
        dst_geo_transform[0] + dst_geo_transform[1] * dst_x_size,
        dst_geo_transform[3] + dst_geo_transform[5] * dst_y_size,
    )

    drv = gdal.GetDriverByName("GTiff")
    dest = drv.Create(dst_file_name, dst_x_size, dst_y_size, 1, gdal.GDT_Float32)
    dest.SetGeoTransform(dst_geo_transform)
    dest.SetProjection(dataset.GetProjection())
    gdal.ReprojectImage(
        dataset, dest, dataset.GetProjection(), dest.GetProjection(), gdal.GRA_Bilinear
    )


def get_raster_nodata_value(path):
    ds = gdal.Open(path, gdal.gdalconst.GA_ReadOnly)
    band = ds.GetRasterBand(1)
    return band.GetNoDataValue()


class DemBase(object):
    def __init__(self, path):
        self.path = path

    def get_nodata_value(self):
        # type: () -> int | None

        return None

    def needs_nodata_translation(self):
        # type: () -> bool

        return False

    def get_dem_tiles(self, points):
        # type: (List[float]) -> List[str]

        return []

    def get_tile_path(self, tile):
        # type: (str) -> str | None

        return None


class DemSrtm3ArcSec(DemBase):
    def __init__(self, path):
        super(DemSrtm3ArcSec, self).__init__(path)

    def get_nodata_value(self):
        # type: () -> int | None

        return -32768

    def needs_nodata_translation(self):
        # type: () -> bool

        return True

    def get_dem_tiles(self, points):
        # type: (List[float]) -> List[str]

        a_x, a_y, b_x, b_y = points
        if a_x < b_x and a_y > b_y:
            a_bb_x = int(math.floor(a_x / 5) * 5)
            a_bb_y = int(math.floor((a_y + 5) / 5) * 5)
            b_bb_x = int(math.floor((b_x + 5) / 5) * 5)
            b_bb_y = int(math.floor(b_y / 5) * 5)

            x_numbers_list = [
                (x + 180) / 5 + 1
                for x in range(min(a_bb_x, b_bb_x), max(a_bb_x, b_bb_x), 5)
            ]
            x_numbers_list_format = ["%02d" % (x,) for x in x_numbers_list]

            y_numbers_list = [
                (60 - x) / 5 for x in range(min(a_bb_y, b_bb_y), max(a_bb_y, b_bb_y), 5)
            ]
            y_numbers_list_format = ["%02d" % (x,) for x in y_numbers_list]

            srtm_zips = [
                "srtm_" + str(x) + "_" + str(y) + ".tif"
                for x in x_numbers_list_format
                for y in y_numbers_list_format
            ]

            return srtm_zips
        return []

    def get_tile_path(self, tile):
        # type: (str) -> str | None

        tile_path = os.path.join(self.path, tile)
        if os.path.exists(tile_path):
            return tile_path
        return None

    @staticmethod
    def detect(path):
        # type: (str) -> DemBase | None

        if glob.glob(os.path.join(path, "srtm*.tif")):
            return DemSrtm3ArcSec(path)
        return None


class DemSrtm1ArcSec(DemBase):
    def __init__(self, path):
        super(DemSrtm1ArcSec, self).__init__(path)

    def get_nodata_value(self):
        # type: () -> int | None

        return None

    def needs_nodata_translation(self):
        # type: () -> bool

        return True

    def get_dem_tiles(self, points):
        # type: (List[float]) -> List[str]

        a_x, a_y, b_x, b_y = points
        x_l = int(math.floor(a_x))
        x_r = int(math.floor(b_x))
        y_b = int(math.floor(b_y))
        y_t = int(math.floor(a_y))

        x_l, x_r = min(x_l, x_r), max(x_l, x_r)
        y_b, y_t = min(y_b, y_t), max(y_b, y_t)

        tiles = []
        for x in range(x_l, x_r + 1):
            for y in range(y_b, y_t + 1):
                if y > 0:
                    yp = y
                    lat = "N"
                else:
                    yp = -y
                    lat = "S"
                if x > 0:
                    xp = x
                    lon = "E"
                else:
                    xp = -x
                    lon = "W"
                tile = "{}{}{}{}".format(lat, str(yp).zfill(2), lon, str(xp).zfill(3))
                tiles.append(tile)
        return tiles

    def get_tile_path(self, tile):
        # type: (str) -> str | None

        tile_path = os.path.join(self.path, "{}.hgt".format(tile))
        if os.path.exists(tile_path):
            return tile_path
        tile_path = os.path.join(self.path, "{}.SRTMGL1.hgt.zip".format(tile))
        if os.path.exists(tile_path):
            return "/vsizip/{}/{}.hgt".format(tile_path, tile)
        return None

    @staticmethod
    def detect(path):
        # type: (str) -> DemBase | None

        if glob.glob(os.path.join(path, "*.SRTMGL1.hgt.zip")) or glob.glob(
            os.path.join(path, "*.hgt")
        ):
            return DemSrtm1ArcSec(path)
        return None


class DemAsterV3(DemBase):
    def __init__(self, path):
        super(DemAsterV3, self).__init__(path)

    def get_nodata_value(self):
        # type: () -> int | None

        return None

    def needs_nodata_translation(self):
        # type: () -> bool

        return False

    def get_dem_tiles(self, points):
        # type: (List[float]) -> List[str]

        a_x, a_y, b_x, b_y = points
        x_l = int(math.floor(a_x))
        x_r = int(math.floor(b_x))
        y_b = int(math.floor(b_y))
        y_t = int(math.floor(a_y))

        x_l, x_r = min(x_l, x_r), max(x_l, x_r)
        y_b, y_t = min(y_b, y_t), max(y_b, y_t)

        tiles = []
        for x in range(x_l, x_r + 1):
            for y in range(y_b, y_t + 1):
                if y > 0:
                    yp = y
                    lat = "N"
                else:
                    yp = -y
                    lat = "S"
                if x > 0:
                    xp = x
                    lon = "E"
                else:
                    xp = -x
                    lon = "W"
                tile = "ASTGTMV003_{}{}{}{}".format(
                    lat, str(yp).zfill(2), lon, str(xp).zfill(3)
                )
                tiles.append(tile)
        return tiles

    def get_tile_path(self, tile):
        # type: (str) -> str | None

        tile_path = os.path.join(self.path, "{}_dem.tif".format(tile))
        if os.path.exists(tile_path):
            return tile_path
        tile_path = os.path.join(self.path, "{}.zip".format(tile))
        if os.path.exists(tile_path):
            return "/vsizip/{}/{}_dem.tif".format(tile_path, tile)
        return None

    @staticmethod
    def detect(path):
        # type: (str) -> DemBase | None

        if glob.glob(os.path.join(path, "ASTGTM*.*")):
            return DemAsterV3(path)
        return None


class DemEuDem(DemBase):
    def __init__(self, path):
        super(DemEuDem, self).__init__(path)

    def get_nodata_value(self):
        # type: () -> int | None

        return None

    def needs_nodata_translation(self):
        # type: () -> bool

        return True

    def get_dem_tiles(self, points):
        # type: (List[float]) -> List[str]

        a_x, a_y, b_x, b_y = points
        wgs84_srs = osr.SpatialReference()
        wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        wgs84_srs.ImportFromEPSG(4326)
        wgs84_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        laea_srs = osr.SpatialReference()
        laea_srs.ImportFromEPSG(3035)
        transform = osr.CoordinateTransformation(wgs84_srs, laea_srs)
        pt = transform.TransformPoints([[a_x, a_y], [b_x, b_y]])
        x_l = int(pt[0][0] / 1000000)
        x_r = int(pt[1][0] / 1000000)
        y_b = int(pt[1][1] / 1000000)
        y_t = int(pt[0][1] / 1000000)

        x_l, x_r = min(x_l, x_r), max(x_l, x_r)
        y_b, y_t = min(y_b, y_t), max(y_b, y_t)

        tiles = []
        for x in range(x_l, x_r + 1):
            for y in range(y_b, y_t + 1):
                tile = "eu_dem_v11_E{}N{}.TIF".format(x * 10, y * 10)
                tiles.append(tile)
        return tiles

    def get_tile_path(self, tile):
        # type: (str) -> str | None

        tile_path = os.path.join(self.path, tile)
        if os.path.exists(tile_path):
            return tile_path
        return None

    @staticmethod
    def detect(path):
        # type: (str) -> DemBase | None

        if glob.glob(os.path.join(path, "eu_dem_*.*")):
            return DemEuDem(path)
        return None


def get_dem_type(path):
    # type: (str) -> DemBase | None

    return (
        DemSrtm3ArcSec.detect(path)
        or DemSrtm1ArcSec.detect(path)
        or DemAsterV3.detect(path)
        or DemEuDem.detect(path)
    )


def get_landsat_tile_id(image):
    m = re.match(
        r"[A-Z][A-Z]\d(\d{6})\d{4}\d{3}[A-Z]{3}\d{2}_B\d{1,2}\.TIF|[A-Z][A-Z]\d\d_[A-Z0-9]{4}_(\d{6})_\d{8}_\d{8}_\d{2}_[A-Z0-9]{2}_\w\d{1,2}\.TIF",
        image,
    )
    return m and ("L8", m.group(1) or m.group(2))


def get_sentinel2_tile_id(image):
    m = re.match(r"\w+_T(\w{5})_B\d{2}\.\w{3}|T(\w{5})_\d{8}T\d{6}_B\d{2}.\w{3}", image)
    return m and ("S2", m.group(1) or m.group(2))


def get_tile_id(image):
    name = os.path.basename(image)
    return get_landsat_tile_id(name) or get_sentinel2_tile_id(name)


def get_landsat_dir_info(name):
    m = re.match(
        r"[A-Z][A-Z]\d\d{6}(\d{4}\d{3})[A-Z]{3}\d{2}|[A-Z][A-Z]\d\d_[A-Z0-9]{4}_\d{6}_(\d{8})_\d{8}_\d{2}_[A-Z0-9]{2}",
        name,
    )
    return m and ("L8", m.group(1) or m.group(2))


def get_sentinel2_dir_info(name):
    m = re.match(r"S2\w+_(\d{8}T\d{6})\w+.SAFE", name)

    return m and ("S2", m.group(1))


def get_dir_info(dir_name):
    name = os.path.basename(dir_name)
    return get_sentinel2_dir_info(name) or get_landsat_dir_info(name)


class Context(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def format_filename(mode, output_directory, tile_id, suffix):
    filename_template = "{0}_TEST_AUX_REFDE2_{1}_0001_{2}.TIF"

    return filename_template.format(mode, tile_id, suffix)


def create_context(args):
    dir_base = args.input
    if not os.path.exists(dir_base) or not os.path.isdir(dir_base):
        logging.error("The path does not exist: {}".format(dir_base))
        sys.exit(1)

    if dir_base.rfind("/") + 1 == len(dir_base):
        dir_base = dir_base[0 : len(dir_base) - 1]
    mode, date = get_dir_info(dir_base)
    context_array = []

    images = []
    tiles_to_process = []
    if args.tiles_list is not None and len(args.tiles_list) > 0:
        tiles_to_process = args.tiles_list
    if mode == "L8":
        images.append("{}/{}_B1.TIF".format(dir_base, os.path.basename(dir_base)))
    elif mode == "S2":
        dir_base += "/GRANULE/"
        if not os.path.exists(dir_base) or not os.path.isdir(dir_base):
            logging.error("The path for Sentinel 2 (with GRANULE) does not exist ! {}".format(
                    dir_base
                ))
            return []
        tile_dirs = [
            "{}{}".format(dir_base, f)
            for f in os.listdir(dir_base)
            if isdir(join(dir_base, f))
        ]
        for tile_dir in tile_dirs:
            tile_dir += "/IMG_DATA/"
            image_band2 = glob.glob("{}*_B02.jp2".format(tile_dir))
            if len(image_band2) == 1:
                if len(tiles_to_process) > 0:
                    mode_f, tile_id = get_tile_id(image_band2[0])
                    if tile_id in tiles_to_process:
                        images.append(image_band2[0])
                else:
                    images.append(image_band2[0])
    else:
        return context_array
    for image_filename in images:
        mode, tile_id = get_tile_id(image_filename)

        dataset = gdal.Open(image_filename, gdal.gdalconst.GA_ReadOnly)

        size_x = dataset.RasterXSize
        size_y = dataset.RasterYSize

        geo_transform = dataset.GetGeoTransform()

        spacing_x = geo_transform[1]
        spacing_y = geo_transform[5]

        extent = GetExtent(geo_transform, size_x, size_y)

        source_srs = osr.SpatialReference()
        source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        source_srs.ImportFromWkt(dataset.GetProjection())
        epsg_code = source_srs.GetAttrValue("AUTHORITY", 1)
        target_srs = osr.SpatialReference()
        target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        target_srs.ImportFromEPSG(4326)

        transform = osr.CoordinateTransformation(source_srs, target_srs)
        wgs84_extent = transform.TransformPoints(extent)

        directory_template = "{0}_TEST_AUX_REFDE2_{1}_{2}_0001.DBL.DIR"
        try:
            os.makedirs(args.output)
        except FileExistsError:
            pass
        image_directory = os.path.join(
            args.output, directory_template.format(mode, tile_id, date)
        )
        try:
            os.makedirs(args.working_dir)
        except FileExistsError:
            pass
        temp_directory = os.path.join(
            args.working_dir, directory_template.format(mode, tile_id, date)
        )

        metadata_template = "{0}_TEST_AUX_REFDE2_{1}_{2}_0001.HDR"

        d = dict(
            image=image_filename,
            mode=mode,
            dem_directory=args.dem,
            swbd_directory=args.swbd,
            working_directory=args.working_dir,
            temp_directory=temp_directory,
            output=args.output,
            image_directory=image_directory,
            metadata_file=os.path.join(
                args.output, metadata_template.format(mode, tile_id, date)
            ),
            swbd_list=os.path.join(temp_directory, "swbd.txt"),
            tile_id=tile_id,
            dem_vrt=os.path.join(temp_directory, "dem.vrt"),
            dem_nodata=os.path.join(temp_directory, "dem.tif"),
            dem_coarse=os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "ALC")
            ),
            slope_degrees=os.path.join(temp_directory, "slope_degrees.tif"),
            aspect_degrees=os.path.join(temp_directory, "aspect_degrees.tif"),
            slope_coarse=os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "SLC")
            ),
            aspect_coarse=os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "ASC")
            ),
            wb=os.path.join(temp_directory, "wb.vrt"),
            water_mask=os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "MSK")
            ),
            size_x=size_x,
            size_y=size_y,
            spacing_x=spacing_x,
            spacing_y=spacing_y,
            extent=extent,
            wgs84_extent=wgs84_extent,
            epsg_code=epsg_code,
        )

        if mode == "L8":
            d["dem_r1"] = os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "ALT")
            )
            d["slope_r1"] = os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "SLP")
            )
            d["aspect_r1"] = os.path.join(
                image_directory, format_filename(mode, image_directory, tile_id, "ASP")
            )

            d["dem_r2"] = None
            d["slope_r2"] = None
            d["aspect_r2"] = None
        else:
            d["dem_r1"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "ALT_R1"),
            )
            d["dem_r2"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "ALT_R2"),
            )
            d["slope_r1"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "SLP_R1"),
            )
            d["slope_r2"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "SLP_R2"),
            )
            d["aspect_r1"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "ASP_R1"),
            )
            d["aspect_r2"] = os.path.join(
                image_directory,
                format_filename(mode, image_directory, tile_id, "ASP_R2"),
            )

        context_array.append(Context(**d))
    return context_array


def create_metadata(context):
    file_names = [
        context.dem_r1,
        context.dem_r2,
        context.dem_coarse,
        context.slope_r1,
        context.slope_r2,
        context.slope_coarse,
        context.aspect_r1,
        context.aspect_r2,
        context.aspect_coarse,
        context.water_mask,
    ]

    files = []
    index = 1
    for f in file_names:
        if f is not None:
            files.append(
                E.Packaged_DBL_File(
                    E.Relative_File_Path(os.path.relpath(f, context.output)),
                    sn=str(index),
                )
            )
            index = index + 1
    mission = "LANDSAT_8"
    if context.mode == "S2":
        mission = "SENTINEL-2_"

    return E.Earth_Explorer_Header(
        E.Fixed_Header(E.Mission(mission), E.File_Type("AUX_REFDE2")),
        E.Variable_Header(
            E.Specific_Product_Header(
                E.DBL_Organization(
                    E.List_of_Packaged_DBL_Files(*files, count=str(len(files)))
                ),
                E.Instance_Id(E.Applicable_Site_Nick_Name("PlaceholderSiteName")),
            )
        ),
        E.DEM_Information(
            E.Cartographic(
                E.Coordinate_Reference_System(E.Code(str(context.epsg_code)))
            )
        ),
    )


def process_DTM(context):
    dem = get_dem_type(context.dem_directory)
    points = [
        context.wgs84_extent[0][0],
        context.wgs84_extent[0][1],
        context.wgs84_extent[2][0],
        context.wgs84_extent[2][1],
    ]
    dtm_tiles = dem.get_dem_tiles(points)
    dem_nodata_value = dem.get_nodata_value()

    missing_tiles = []
    dtm_tiles_nodata = []
    for tile in dtm_tiles:
        tile_path = dem.get_tile_path(tile)
        if not tile_path:
            missing_tiles.append(tile)
            continue

        if (
            not dem.needs_nodata_translation()
            or get_raster_nodata_value(tile_path) == 0
        ):
            dtm_tiles_nodata.append(tile_path)
            continue

        tile_name = "{}_nodata.vrt".format(os.path.splitext(tile)[0])
        tile_nodata = os.path.join(context.working_directory, tile_name)

        command = [
            "gdalbuildvrt",
            "-q",
            "-overwrite",
            "-vrtnodata",
            "0",
        ]
        if dem_nodata_value:
            command += ["-srcnodata", str(dem_nodata_value)]
        command += [
            tile_nodata,
            tile_path,
        ]

        run_command(command)
        dtm_tiles_nodata.append(tile_nodata)

    if missing_tiles:
        logging.warning("The following DEM tiles are missing: {}".format(missing_tiles))

    run_command(
        [
            "gdalbuildvrt",
            "-q",
            "-overwrite",
            "-r",
            "cubic",
            context.dem_vrt,
        ]
        + dtm_tiles_nodata
    )
    run_command(
        [
            "gdalwarp",
            "-q",
            "-overwrite",
            "-multi",
            "-r",
            "cubic",
            "-t_srs",
            "EPSG:" + str(context.epsg_code),
            "-et",
            "0",
            "-tr",
            str(context.spacing_x),
            str(context.spacing_y),
            "-te",
            str(context.extent[1][0]),
            str(context.extent[1][1]),
            str(context.extent[3][0]),
            str(context.extent[3][1]),
            context.dem_vrt,
            context.dem_r1,
        ]
    )

    if context.dem_r2:
        # run_command(["gdal_translate",
        #              "-outsize", str(int(round(context.size_x / 2.0))), str(int(round(context.size_y
        #                                                                               / 2.0))),
        #              context.dem_r1,
        #              context.dem_r2])
        resample_dataset(context.dem_r1, context.dem_r2, 20, -20)

    # if context.mode == "L8":
    #     inv_scale = 8.0
    # else:
    #     # scale = 1.0 / 23.9737991266  # almost 1/24
    #     inv_scale = 24.0

    # run_command(["gdal_translate",
    #              "-outsize", str(int(round(context.size_x / inv_scale))), str(int(round(context.size_y /
    #                                                                                     inv_scale))),
    #              context.dem_r1,
    #              context.dem_coarse])
    resample_dataset(context.dem_r1, context.dem_coarse, 240, -240)

    run_command(
        [
            "gdaldem",
            "slope",
            "-q",
            "-compute_edges",
            context.dem_r1,
            context.slope_degrees,
        ]
    )
    run_command(
        [
            "gdaldem",
            "aspect",
            "-q",
            "-compute_edges",
            context.dem_r1,
            context.aspect_degrees,
        ]
    )

    run_command(
        [
            "gdal_translate",
            "-q",
            "-ot",
            "Int16",
            "-scale",
            "0",
            "90",
            "0",
            "157",
            context.slope_degrees,
            context.slope_r1,
        ]
    )
    run_command(
        [
            "gdal_translate",
            "-q",
            "-ot",
            "Int16",
            "-scale",
            "0",
            "368",
            "0",
            "628",
            context.aspect_degrees,
            context.aspect_r1,
        ]
    )

    if context.slope_r2:
        run_command(
            [
                "gdalwarp",
                "-q",
                "-overwrite",
                "-r",
                "cubic",
                "-tr",
                "20",
                "20",
                context.slope_r1,
                context.slope_r2,
            ]
        )

    if context.aspect_r2:
        run_command(
            [
                "gdalwarp",
                "-q",
                "-overwrite",
                "-r",
                "cubic",
                "-tr",
                "20",
                "20",
                context.aspect_r1,
                context.aspect_r2,
            ]
        )

    run_command(
        [
            "gdalwarp",
            "-q",
            "-overwrite",
            "-r",
            "cubic",
            "-tr",
            "240",
            "240",
            context.slope_r1,
            context.slope_coarse,
        ]
    )

    run_command(
        [
            "gdalwarp",
            "-q",
            "-overwrite",
            "-r",
            "cubic",
            "-tr",
            "240",
            "240",
            context.aspect_r1,
            context.aspect_coarse,
        ]
    )

    return True


def process_WB(context):
    x_l = int(math.floor(context.wgs84_extent[0][0]))
    x_r = int(math.floor(context.wgs84_extent[2][0]))
    y_b = int(math.floor(context.wgs84_extent[2][1]))
    y_t = int(math.floor(context.wgs84_extent[0][1]))
    continents = ["n", "s", "a", "e", "f", "i"]
    swbd_tiles = []
    for x in range(x_l, x_r + 1):
        for y in range(y_b, y_t + 1):
            if y > 0:
                yp = y
                lat = "n"
            else:
                yp = -y
                lat = "s"
            if x > 0:
                xp = x
                lon = "e"
            else:
                xp = -x
                lon = "w"
            tile = "{}{}{}{}".format(
                lon, str(xp).zfill(3), lat, str(yp).zfill(2)
            )
            for continent in continents:
                tile_path = os.path.join(context.swbd_directory, tile + continent + ".shp")
                if os.path.exists(tile_path):
                    swbd_tiles.append(tile_path)
                    break

    if len(swbd_tiles) == 0:
        empty_shp = os.path.join(context.swbd_directory, "empty.shp")
        swbd_tiles.append(empty_shp)

    run_command(
        [
            "ogrmerge.py",
            "-overwrite_ds",
            "-single",
            "-a_srs", "EPSG:4326",
            "-t_srs", "EPSG:{}".format(context.epsg_code),
            "-o",
            context.wb,
        ]
        + swbd_tiles
    )

    run_command(
        [
            "gdal_rasterize",
            "-ot", "Byte",
            "-burn", 1,
            "-te", context.extent[1][0], context.extent[1][1], context.extent[3][0], context.extent[3][1],
            "-tr", 240, 240,
            context.wb,
            context.water_mask,
        ]
    )


def change_extension(file, new_extension):
    return os.path.splitext(file)[0] + new_extension


def process_context(context):
    try:
        try:
            os.makedirs(context.image_directory)
        except FileExistsError:
            pass
        try:
            os.makedirs(context.temp_directory)
        except FileExistsError:
            pass
        metadata = create_metadata(context)
        with open(context.metadata_file, "wb") as f:
            lxml.etree.ElementTree(metadata).write(f, pretty_print=True)

        if not process_DTM(context):
            logging.error("Failed to process DTM")
            return
        process_WB(context)

        files = [
            context.swbd_list,
            context.dem_vrt,
            context.dem_nodata,
            context.slope_degrees,
            context.aspect_degrees,
            context.wb,
        ]

        for file in files:
            try:
                os.remove(file)
            except FileNotFoundError:
                pass

        try:
            os.rmdir(context.temp_directory)
        except Exception as e:
            logging.error("Couldn't remove the temp dir {}: {}".format(context.temp_directory, e))
    except Exception as e:
        logging.error(e)
        os._exit(1)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Creates DEM and WB data for MACCS")
    parser.add_argument("input", help="input L1C directory")
    parser.add_argument(
        "--dem", "--srtm", dest="dem", required=True, help="DEM dataset path"
    )
    parser.add_argument("--swbd", required=True, help="SWBD dataset path")
    parser.add_argument(
        "-l",
        "--tiles-list",
        required=False,
        nargs="+",
        help="If set, only these tiles will be processed",
    )
    parser.add_argument("-w", "--working-dir", required=True, help="working directory")
    parser.add_argument(
        "-p",
        "--processes-number",
        required=False,
        help="number of processed to run",
        default="3",
    )
    parser.add_argument("output", help="output location")

    args = parser.parse_args()

    return int(args.processes_number), create_context(args)


logging.basicConfig(level=logging.DEBUG)
proc_number, contexts = parse_arguments()

if len(contexts) == 0:
    logging.error("The context could not be created")
    sys.exit(1)

p = None
try:
    p = Pool(int(proc_number), lambda: signal(SIGINT, SIG_IGN))
    res = p.map_async(process_context, contexts)
    while True:
        try:
            res.get(1)
            break
        except TimeoutError:
            pass
    p.close()
except KeyboardInterrupt:
    if p:
        p.terminate()
        p.join()
