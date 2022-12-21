#!/usr/bin/env python
from __future__ import print_function

import argparse
from collections import defaultdict
import csv
from datetime import datetime, timedelta

import docker
from glob import glob
import multiprocessing
import multiprocessing.dummy
import os
import os.path
from osgeo import ogr
from osgeo import osr
from osgeo import gdal
from osgeo.gdal import gdalconst
from lxml import etree
from lxml.builder import E
import pipes
import subprocess
import sys
import tempfile


OTB_IMAGE_NAME = "sen4x/otb:7.4.0"
SEN4CAP_PROCESSORS_IMAGE_NAME = "sen4cap/processors:3.0.0"
INTERPOLATION_IMAGE_NAME = "sen4x/interpolation:0.1.0"
GDAL_IMAGE_NAME = "osgeo/gdal:ubuntu-full-3.4.1"
MISC_IMAGE_NAME = "sen4x/s4s-interim-ct:latest"


SATELLITE_ID_SENTINEL2 = 1
SATELLITE_ID_LANDSAT = 2
SATELLITE_ID_SENTINEL1 = 3

ORBIT_TYPE_ID_ASCENDING = 1
ORBIT_TYPE_ID_DESCENDING = 2

POLARIZATION_VV = "VV"
POLARIZATION_VH = "VH"

PRODUCT_TYPE_ID_BCK = 10
PRODUCT_TYPE_ID_COHE = 11


def get_satellite_name_long(satellite_id):
    if satellite_id == SATELLITE_ID_SENTINEL2:
        return "SENTINEL"
    elif satellite_id == SATELLITE_ID_LANDSAT:
        return "LANDSAT"
    raise ValueError("Unknown satellite id", satellite_id)


def get_satellite_name_short(satellite_id):
    if satellite_id == SATELLITE_ID_SENTINEL2:
        return "s2"
    elif satellite_id == SATELLITE_ID_LANDSAT:
        return "l8"
    raise ValueError("Unknown satellite id", satellite_id)


def get_satellite_resolution(satellite_id):
    if satellite_id == SATELLITE_ID_SENTINEL2:
        return 10
    elif satellite_id == SATELLITE_ID_LANDSAT:
        return 20
    raise ValueError("Unknown satellite id", satellite_id)


def get_orbit_type(orbit_type_id):
    if orbit_type_id == ORBIT_TYPE_ID_ASCENDING:
        return "ASC"
    if orbit_type_id == ORBIT_TYPE_ID_DESCENDING:
        return "DESC"
    raise ValueError("Unknown orbit type id", orbit_type_id)


def get_product_type(product_type_id):
    if product_type_id == PRODUCT_TYPE_ID_BCK:
        return "BCK"
    if product_type_id == PRODUCT_TYPE_ID_COHE:
        return "COHE"
    raise ValueError("Unknown product type id", product_type_id)


class OpticalProduct(object):
    def __init__(self, site_id, tile, dt, path):
        self.site_id = site_id
        self.tile = tile
        self.date = dt
        self.path = path


def cal(dt):
    week = int(dt.strftime("%W"))
    week = min(max(1, week), 52)
    return (dt.year, dt.month, week)


class RadarProduct(object):
    def __init__(self, dt, tile_id, orbit_type_id, polarization, product_type, path):
        self.year = dt.year
        self.month = dt.month
        self.tile_id = tile_id
        self.orbit_type_id = orbit_type_id
        self.polarization = polarization
        self.product_type = product_type
        self.path = path

        (year, month, week) = cal(dt)
        self.year = year
        self.month = month
        self.week = week


def parse_date(str):
    return datetime.strptime(str, "%Y-%m-%d").date()


def get_season_dates(start_date, end_date):
    dates = []
    while start_date <= end_date:
        dates.append(start_date)
        start_date += timedelta(days=10)
    return dates


def get_tile_hdr(tile, path):
    pat = "*_SSC_*_{}_*.HDR".format(tile)
    entries = glob(os.path.join(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        entries = glob(os.path.join(path, "*_SSC_*_{}_*.DBL.DIR/*.TIF".format(tile)))
        for raster_type in ["FRE", "CLD", "MSK", "QLT"]:
            for res in ["R1", "R2"]:
                pat = "_{}_{}.DBL.TIF".format(raster_type, res)
                ok = False
                for entry in entries:
                    if entry.endswith(pat):
                        ok = True
                        break
                if not ok:
                    print(
                        "No {} raster found for tile {} in {}".format(pat, tile, path)
                    )
                    return None
        return hdr

    pat = "*_T{}_*/*_MTD_ALL.xml".format(tile)
    entries = glob(os.path.join(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        return hdr

    pat = "MTD_MSIL2A.xml"
    entries = glob(os.path.join(path, pat))
    if len(entries) > 0:
        hdr = entries[0]
        return hdr

    print("No HDR found for tile {} in {}".format(tile, path))
    return None


def date_to_epoch_days(dt):
    unix_epoch = datetime.utcfromtimestamp(0).date()
    d = dt - unix_epoch
    return d.days


def epoch_days_to_date(days):
    unix_epoch = datetime.utcfromtimestamp(0).date()
    dt = unix_epoch + timedelta(days=days)
    return dt


def save_dates_file(path, site_id, satellite_id, dates):
    satellite_short = get_satellite_name_short(satellite_id)
    satellite_long = get_satellite_name_long(satellite_id)
    file_name = "dates-{}-{}.txt".format(site_id, satellite_short)
    file_path = os.path.join(path, file_name)
    if not os.path.exists(file_path):
        with open(file_path, "w") as file:
            for dt in dates:
                days = date_to_epoch_days(dt)
                file.write("{} {}\n".format(satellite_long, days))
    return file_path


def run_command(args, env=None, retry=False):
    args = list(map(str, args))
    cmd_line = " ".join(map(pipes.quote, args))

    retries = 5 if retry else 1
    while retries > 0:
        retries -= 1
        print(cmd_line)
        if env:
            result = subprocess.call(args, env=env)
        else:
            result = subprocess.call(args)
        if result != 0:
            print("Exit code: {}".format(result))
        else:
            break


def extract_optical_features(
    path, satellite_id, tile, products, ref, dates_file, red_edge, volumes
):
    satellite = get_satellite_name_long(satellite_id)
    # resolution = get_satellite_resolution(satellite_id)

    hdrs = []
    for product in products:
        hdr = get_tile_hdr(tile, product.path)
        if hdr:
            hdrs.append(hdr)

    if red_edge:
        mean = "mean-re-{}.csv".format(tile)
        dev = "dev-re-{}.csv".format(tile)
        count = "count-re-{}.csv".format(tile)
        resolution = 20
    else:
        mean = "mean-{}.csv".format(tile)
        dev = "dev-{}.csv".format(tile)
        count = "count-{}.csv".format(tile)
        resolution = 10

    mean = os.path.join(path, mean)
    dev = os.path.join(path, dev)
    count = os.path.join(path, count)

    env = os.environ.copy()
    env["ITK_USE_THREADPOOL"] = str(1)
    env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(4)
    env["OTB_MAX_RAM_HINT"] = str(256)

    command = []
    command += ["otbcli", "OpticalFeatures"]
    command += ["-pixsize", resolution]
    command += ["-mission", satellite]
    command += ["-ref", ref]
    command += ["-dates", dates_file]
    command += ["-outmean", mean]
    command += ["-outdev", dev]
    command += ["-outcount", count]
    if red_edge:
        command += ["-rededge", "true"]
    command += ["-il"] + hdrs
    if not os.path.exists(mean) or not os.path.exists(dev) or not os.path.exists(count):
        client = docker.from_env()
        client.containers.run(
            image=SEN4CAP_PROCESSORS_IMAGE_NAME,
            user=f"{os.getuid()}:{os.getgid()}",
            volumes=self.volumes,
            working_dir=path,
            command=command,
            environment=env,
        )


def get_lpis_map(lpis_path, resolution):
    if resolution == 10:
        pat = "*_S2.tif"
    elif resolution == 20:
        pat = "*_S1.tif"

    r = {}
    files = glob(os.path.join(lpis_path, pat))
    for file in files:
        p = os.path.basename(file)
        tile = p.rsplit("_", 2)[1]
        r[tile] = file
    return r


def paste_files(file1, file2, out):
    fd, temp = tempfile.mkstemp(".csv")
    os.close(fd)

    command = []
    command += ["sh"]
    command += ["-c", "cut -d, -f2- {} > {}".format(file2, temp)]
    run_command(command)

    command = []
    command += ["sh"]
    command += ["-c", "paste -d, {} {} >> {}".format(file1, temp, out)]
    run_command(command)

    os.remove(temp)


def process_optical(args, pool, satellite_id):
    product_map = defaultdict(lambda: defaultdict(list))
    with open(args.optical_products, "rt") as file:
        reader = csv.reader(file)
        next(reader)
        for (site_id, name, full_path, tile, created_timestamp) in reader:
            site_id = int(site_id)
            created_timestamp = datetime.fromisoformat(created_timestamp)

            product = OpticalProduct(site_id, tile, created_timestamp.date(), full_path)
            product_map[site_id][tile].append(product)

    for site_id, tiles in product_map.items():
        first_date = None
        last_date = None
        for tile, products in tiles.items():
            products.sort(key=lambda product: product.date)

            for product in products:
                if first_date is None or product.date < first_date:
                    first_date = product.date
                if last_date is None or product.date > last_date:
                    last_date = product.date

        start_date = first_date
        dates = get_season_dates(start_date, last_date)
        print(site_id, start_date, last_date)
        dates_file = save_dates_file(args.path, site_id, satellite_id, dates)

        ref_map_10m = get_lpis_map(args.lpis_path, 10)
        ref_map_20m = get_lpis_map(args.lpis_path, 20)
        work = []
        for tile, products in tiles.items():
            filtered_products = [
                product
                for product in products
                if product.date >= start_date and product.date <= last_date
            ]
            tile_ref_10m = ref_map_10m.get(tile)
            if tile_ref_10m is not None:
                work.append(
                    (
                        args.path,
                        satellite_id,
                        tile,
                        filtered_products,
                        tile_ref_10m,
                        dates_file,
                        False,
                    )
                )
            if args.re:
                tile_ref_20m = ref_map_20m.get(tile)
                if tile_ref_20m is not None:
                    work.append(
                        (
                            args.path,
                            satellite_id,
                            tile,
                            filtered_products,
                            tile_ref_20m,
                            dates_file,
                            True,
                        )
                    )

        pool.map(lambda g: extract_optical_features(*g), work)

        command = []
        command += ["merge-statistics"]
        command += ["mean.csv", "dev.csv"]
        for tile, _ in tiles.items():
            mean = "mean-{}.csv".format(tile)
            dev = "dev-{}.csv".format(tile)
            count = "count-{}.csv".format(tile)

            mean = os.path.join(args.path, mean)
            dev = os.path.join(args.path, dev)
            count = os.path.join(args.path, count)

            command += [mean, dev, count]

        run_command(command)

        if args.re:
            command = []
            command += ["merge-statistics"]
            command += ["mean-re.csv", "dev-re.csv"]
            for tile, _ in tiles.items():
                mean_re = "mean-re-{}.csv".format(tile)
                dev_re = "dev-re-{}.csv".format(tile)
                count_re = "count-re-{}.csv".format(tile)

                mean_re = os.path.join(args.path, mean_re)
                dev_re = os.path.join(args.path, dev_re)
                count_re = os.path.join(args.path, count_re)

                command += [mean_re, dev_re, count_re]

            run_command(command)

        headers_mean = "mean-headers.csv"
        headers_mean = os.path.join(args.path, headers_mean)

        headers_dev = "dev-headers.csv"
        headers_dev = os.path.join(args.path, headers_dev)

        headers_re_mean = "mean-re-headers.csv"
        headers_re_mean = os.path.join(args.path, headers_re_mean)

        headers_re_dev = "dev-re-headers.csv"
        headers_re_dev = os.path.join(args.path, headers_re_dev)

        generate_headers(
            dates_file,
            headers_mean,
            headers_dev,
            headers_re_mean,
            headers_re_dev,
            args.re,
        )

        optical_features = "optical-features.csv"
        optical_features = os.path.join(args.path, optical_features)

        if not os.path.exists(optical_features):
            paste_files(headers_mean, headers_dev, optical_features)
            paste_files("mean.csv", "dev.csv", optical_features)

        if args.re:
            optical_features_re = "optical-features-re.csv"
            optical_features_re = os.path.join(args.path, optical_features_re)

            if not os.path.exists(optical_features_re):
                paste_files(headers_re_mean, headers_re_dev, optical_features_re)
                paste_files("mean-re.csv", "dev-re.csv", optical_features_re)


class RadarGroup(object):
    def __init__(
        self, year, month, week, tile_id, orbit_type_id, polarization, product_type
    ):
        self.year = year
        self.month = month
        self.week = week
        self.tile_id = tile_id
        self.orbit_type_id = orbit_type_id
        self.polarization = polarization
        self.product_type = product_type

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (
            self.year,
            self.week,
            self.tile_id,
            self.orbit_type_id,
            self.polarization,
            self.product_type,
        )

    def format(self, site_id):
        orbit_type = get_orbit_type(self.orbit_type_id)
        product_type = get_product_type(self.product_type)
        return "SEN4CAP_L2A_PRD_S{}_W{:04}{:02}_T{}_{}_{}_{}.tif".format(
            site_id,
            self.year,
            self.week,
            self.tile_id,
            orbit_type,
            self.polarization,
            product_type,
        )

    def band_description(self):
        orbit_type = get_orbit_type(self.orbit_type_id)
        product_type = get_product_type(self.product_type)
        return "S1_W{:04}{:02}_{}_{}_{}".format(
            self.year,
            self.week,
            orbit_type,
            self.polarization,
            product_type,
        )


class BackscatterWeeklyRatioGroup(object):
    def __init__(self, year, week, tile_id, orbit_type_id):
        self.year = year
        self.week = week
        self.tile_id = tile_id
        self.orbit_type_id = orbit_type_id

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (self.year, self.week, self.tile_id, self.orbit_type_id)

    def format(self, site_id):
        orbit_type = get_orbit_type(self.orbit_type_id)
        return "SEN4CAP_L2A_PRD_S{}_W{:04}{:02}_T{}_{}_RATIO_BCK".format(
            site_id, self.year, self.week, self.tile_id, orbit_type
        )


class BackscatterBiMonthlyGroup(object):
    def __init__(self, year, month, tile_id, orbit_type_id, polarization):
        self.year = year
        self.month = month
        self.tile_id = tile_id
        self.orbit_type_id = orbit_type_id
        self.polarization = polarization

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (
            self.year,
            self.month,
            self.tile_id,
            self.orbit_type_id,
            self.polarization,
        )

    def format(self, site_id):
        orbit_type = get_orbit_type(self.orbit_type_id)
        return "SEN4CAP_L2A_PRD_S{}_M{:04}{:02}_T{}_{}_{}_BCK.tif".format(
            site_id, self.year, self.month, self.tile_id, orbit_type, self.polarization
        )

    def band_description(self, band_number):
        orbit_type = get_orbit_type(self.orbit_type_id)
        if band_number == 1:
            desc = "MEAN"
        elif band_number == 2:
            desc = "CVAR"
        return "S1_M{:04}{:02}_{}_{}_BCK_{}".format(
            self.year, self.month, orbit_type, self.polarization, desc
        )


class BackscatterRatioBiMonthlyGroup(object):
    def __init__(self, year, month, tile_id, orbit_type_id):
        self.year = year
        self.month = month
        self.tile_id = tile_id
        self.orbit_type_id = orbit_type_id

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (self.year, self.month, self.tile_id, self.orbit_type_id)

    def format(self, site_id):
        orbit_type = get_orbit_type(self.orbit_type_id)
        return "SEN4CAP_L2A_PRD_S{}_M{:04}{:02}_T{}_{}_RATIO_BCK.tif".format(
            site_id, self.year, self.month, self.tile_id, orbit_type
        )

    def band_description(self, band_number):
        orbit_type = get_orbit_type(self.orbit_type_id)
        if band_number == 1:
            desc = "MEAN"
        elif band_number == 2:
            desc = "CVAR"
        return "S1_M{:04}{:02}_{}_RATIO_BCK_{}".format(
            self.year, self.month, orbit_type, desc
        )


class BackscatterPair(object):
    def __init__(self):
        self.vv = None
        self.vh = None


class BackscatterPairs(object):
    def __init__(self):
        self.vv = []
        self.vh = []


class CoherenceMonthlyGroup(object):
    def __init__(self, year, month, tile_id, polarization):
        self.year = year
        self.month = month
        self.tile_id = tile_id
        self.polarization = polarization

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (self.year, self.month, self.tile_id, self.polarization)

    def format(self, site_id):
        return "SEN4CAP_L2A_PRD_S{}_M{:04}{:02}_T{}_{}_COHE.tif".format(
            site_id, self.year, self.month, self.tile_id, self.polarization
        )

    def band_description(self, band_number):
        if band_number == 1:
            desc = "MEAN"
        elif band_number == 2:
            desc = "Q10"
        return "S1_M{:04}{:02}_{}_COHE_{}".format(
            self.year, self.month, self.polarization, desc
        )


class CoherenceSeasonGroup(object):
    def __init__(self, tile_id, polarization):
        self.tile_id = tile_id
        self.polarization = polarization

    def __lt__(self, other):
        return self.__key() < other.__key()

    def __le__(self, other):
        return self.__key() <= other.__key()

    def __eq__(self, other):
        return self.__key() == other.__key()

    def __ne__(self, other):
        return self.__key() != other.__key()

    def __ge__(self, other):
        return self.__key() >= other.__key()

    def __gt__(self, other):
        return self.__key() > other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return (self.tile_id, self.polarization)

    def format(self, site_id):
        return "SEN4CAP_L2A_PRD_S{}_S_T{}_{}_COHE.tif".format(
            site_id, self.tile_id, self.polarization
        )

    def band_description(self):
        return "S1_S_{}_COHE_STDDEV".format(self.polarization)


def get_tile_footprints(file):
    tiles = {}
    with open(file, "rt") as file:
        reader = csv.reader(file)
        next(reader)
        for (tile_id, epsg_code, geog) in reader:
            epsg_code = int(epsg_code)
            geog = ogr.CreateGeometryFromWkt(geog)

            tiles[tile_id] = (geog, epsg_code)
    return tiles


def get_radar_products(file):
    products = []
    with open(file, "rt") as file:
        reader = csv.reader(file)
        next(reader)
        for (
            dt,
            tile_id,
            orbit_type_id,
            polarization,
            radar_product_type,
            full_path,
        ) in reader:
            dt = datetime.fromisoformat(dt)
            orbit_type_id = int(orbit_type_id)
            radar_product_type = int(radar_product_type)
            products.append(
                RadarProduct(
                    dt,
                    tile_id,
                    orbit_type_id,
                    polarization,
                    radar_product_type,
                    full_path,
                )
            )

    return products


def get_otb_extended_filename_with_tiling(file):
    output_extended = file + "?"
    output_extended += "&gdal:co:COMPRESS=DEFLATE"
    output_extended += "&gdal:co:TILED=YES"
    output_extended += "&gdal:co:BLOCKXSIZE=1024"
    output_extended += "&gdal:co:BLOCKYSIZE=1024"
    output_extended += "&streaming:type=tiled"
    output_extended += "&streaming:sizemode=height"
    output_extended += "&streaming:sizevalue=1024"
    return output_extended


def get_otb_extended_filename_skipgeom(file):
    return file + "?skipgeom=true"


def get_statistics_file_names(input):
    dir = os.path.dirname(input)
    name = os.path.splitext(os.path.basename(input))[0]

    mean = os.path.join(dir, name + "_MEAN.csv")
    dev = os.path.join(dir, name + "_DEV.csv")
    count = os.path.join(dir, name + "_COUNT.csv")

    return (mean, dev, count)


def get_statistics_invocation(input, ref):
    (mean, dev, count) = get_statistics_file_names(input)

    if os.path.exists(mean) and os.path.exists(dev) and os.path.exists(count):
        return None

    command = []
    command += ["otbcli", "ClassStatistics"]
    command += ["-in", get_otb_extended_filename_skipgeom(input)]
    command += ["-ref", ref]
    command += ["-bv", 0]
    command += ["-outmean", mean]
    command += ["-outdev", dev]
    command += ["-outcount", count]

    return command


class WeeklyComposite(object):
    def __init__(
        self,
        output,
        temp,
        tile_ref,
        xmin,
        xmax,
        ymin,
        ymax,
        force_input_epsg,
        tile_epsg_code,
        tile_extent,
        inputs,
    ):
        self.output = output
        self.temp = temp
        self.tile_ref = tile_ref
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax
        self.force_input_epsg = force_input_epsg
        self.tile_epsg_code = tile_epsg_code
        self.tile_extent = tile_extent
        self.inputs = inputs

    def run(self):
        env = os.environ.copy()
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not os.path.exists(self.output):
            self.inputs = filter(os.path.exists, self.inputs)
            inputs = [
                get_otb_extended_filename_skipgeom(input) for input in self.inputs
            ]

            command = []
            command += ["otbcli", "Composite"]
            command += ["-progress", "false"]
            command += ["-out", self.temp]
            command += ["-outputs.ulx", self.xmin]
            command += ["-outputs.uly", self.ymax]
            command += ["-outputs.lrx", self.xmax]
            command += ["-outputs.lry", self.ymin]
            command += ["-outputs.spacingx", 10]
            command += ["-outputs.spacingy", -10]
            command += ["-il"] + inputs
            command += ["-bv", 0]
            command += ["-opt.gridspacing", 240]
            run_command(command, env)

            (xmin, ymax) = self.tile_extent[0]
            (xmax, ymin) = self.tile_extent[2]

            command = []
            command += ["gdalwarp", "-q"]
            command += ["-r", "cubic"]
            if self.force_input_epsg is not None:
                command += ["-s_srs", "EPSG:{}".format(self.force_input_epsg)]
            command += ["-t_srs", "EPSG:{}".format(self.tile_epsg_code)]
            command += ["-tr", 10, 10]
            command += ["-te", xmin, ymin, xmax, ymax]
            command += [self.temp]
            command += [self.output]
            run_command(command)

            os.remove(self.temp)

            command = []
            command += ["optimize_gtiff.py"]
            command += ["--no-data", 0]
            command += [self.output]
            run_command(command, env)

        if self.tile_ref:
            command = get_statistics_invocation(self.output, self.tile_ref)
            if command:
                run_command(command, env)


class WeeklyCompositeS4S(object):
    def __init__(
        self,
        output,
        temp,
        tile_ref,
        xmin,
        xmax,
        ymin,
        ymax,
        force_input_epsg,
        tile_epsg_code,
        tile_extent,
        inputs,
        volumes,
    ):
        self.output = output
        self.temp = temp
        self.tile_ref = tile_ref
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax
        self.force_input_epsg = force_input_epsg
        self.tile_epsg_code = tile_epsg_code
        self.tile_extent = tile_extent
        self.inputs = inputs
        self.volumes = volumes

    def run(self):
        # env = os.environ.copy()
        env = {}
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not os.path.exists(self.output):
            self.inputs = filter(os.path.exists, self.inputs)
            inputs = [
                get_otb_extended_filename_skipgeom(input) for input in self.inputs
            ]

            command = []
            command += ["otbcli", "Composite2"]
            command += ["-progress", "false"]
            command += [
                "-out",
                get_otb_extended_filename_with_tiling(self.output),
                "uint16",
            ]
            command += ["-il"] + inputs
            command += ["-bv", "0"]
            # run_command(command, env)

            path = os.path.dirname(os.path.abspath(self.output))
            client = docker.from_env()
            client.containers.run(
                image=INTERPOLATION_IMAGE_NAME,
                auto_remove=True,
                user=f"{os.getuid()}:{os.getgid()}",
                volumes=self.volumes,
                working_dir=path,
                command=command,
                environment=env,
            )

            # command = []
            # command += ["optimize_gtiff.py"]
            # command += ["--no-data", 0]
            # command += [self.output]
            # run_command(command, env)

        if self.tile_ref:
            command = get_statistics_invocation(self.output, self.tile_ref)
            if command:
                run_command(command, env)


class WeeklyRatioStatistics(object):
    def __init__(self, output, vv, vh, tile_ref):
        self.output = output
        self.vv = vv
        self.vh = vh
        self.tile_ref = tile_ref

    def run(self):
        env = os.environ.copy()
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not self.tile_ref:
            return

        (mean, dev, count) = get_statistics_file_names(self.output)

        if os.path.exists(mean) and os.path.exists(dev) and os.path.exists(count):
            return

        command = []
        command += ["otbcli", "ClassStatisticsRatio"]
        command += ["-in.vv", get_otb_extended_filename_skipgeom(self.vv)]
        command += ["-in.vh", get_otb_extended_filename_skipgeom(self.vh)]
        command += ["-ref", self.tile_ref]
        command += ["-bv", 0]
        command += ["-outmean", mean]
        command += ["-outdev", dev]
        command += ["-outcount", count]
        run_command(command, env)


class BackscatterMonthlyComposite(object):
    def __init__(self, tile_ref, output, output_extended, mode, inputs, volumes):
        self.tile_ref = tile_ref
        self.output = output
        self.output_extended = output_extended
        self.mode = mode
        self.inputs = inputs
        self.volumes = volumes

    def run(self):
        # env = os.environ.copy()
        env = {}
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not os.path.exists(self.output):
            self.inputs = filter(os.path.exists, self.inputs)
            inputs = [
                get_otb_extended_filename_skipgeom(input) for input in self.inputs
            ]

            command = []
            command += [
                "otbcli",
                "BackscatterTemporalFeatures",
            ]
            command += ["-progress", "false"]
            command += ["-out", self.output_extended]
            command += ["-mode", self.mode]
            command += ["-il"] + inputs
            # run_command(command, env)

            path = os.path.dirname(os.path.abspath(self.output))
            client = docker.from_env()
            client.containers.run(
                image=SEN4CAP_PROCESSORS_IMAGE_NAME,
                auto_remove=True,
                user=f"{os.getuid()}:{os.getgid()}",
                volumes=self.volumes,
                working_dir=path,
                command=command,
                environment=env,
            )

            # command = []
            # command += ["optimize_gtiff.py"]
            # command += ["--no-data", 0]
            # command += [self.output]
            # run_command(command, env)

        if self.tile_ref:
            command = get_statistics_invocation(self.output, self.tile_ref)
            if command:
                run_command(command, env)


class CoherenceMonthlyComposite(object):
    def __init__(self, tile_ref, output, output_extended, inputs, volumes):
        self.tile_ref = tile_ref
        self.output = output
        self.output_extended = output_extended
        self.inputs = inputs
        self.volumes = volumes

    def run(self):
        # env = os.environ.copy()
        env = {}
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not os.path.exists(self.output):
            self.inputs = filter(os.path.exists, self.inputs)
            inputs = [
                get_otb_extended_filename_skipgeom(input) for input in self.inputs
            ]

            command = []
            command += ["otbcli", "CoherenceMonthlyFeatures"]
            command += ["-progress", "false"]
            command += ["-out", self.output_extended]
            command += ["-il"] + inputs
            # run_command(command, env)

            path = os.path.dirname(os.path.abspath(self.output))
            client = docker.from_env()
            client.containers.run(
                image=SEN4CAP_PROCESSORS_IMAGE_NAME,
                auto_remove=True,
                user=f"{os.getuid()}:{os.getgid()}",
                volumes=self.volumes,
                working_dir=path,
                command=command,
                environment=env,
            )

            # command = []
            # command += ["optimize_gtiff.py"]
            # command += ["--no-data", 0]
            # command += [self.output]
            # run_command(command, env)

        if self.tile_ref:
            command = get_statistics_invocation(self.output, self.tile_ref)
            if command:
                run_command(command, env)


class CoherenceSeasonComposite(object):
    def __init__(self, tile_ref, output, output_extended, inputs, volumes):
        self.tile_ref = tile_ref
        self.output = output
        self.output_extended = output_extended
        self.inputs = inputs
        self.volumes = volumes

    def run(self):
        # env = os.environ.copy()
        env = {}
        env["ITK_USE_THREADPOOL"] = str(1)
        env["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(2)
        env["OTB_MAX_RAM_HINT"] = str(1024)

        if not os.path.exists(self.output):
            self.inputs = filter(os.path.exists, self.inputs)
            inputs = [
                get_otb_extended_filename_skipgeom(input) for input in self.inputs
            ]

            command = []
            command += ["otbcli", "StandardDeviation"]
            command += ["-progress", "false"]
            command += ["-out", self.output_extended]
            command += ["-il"] + inputs
            # run_command(command, env)

            path = os.path.dirname(os.path.abspath(self.output))
            client = docker.from_env()
            client.containers.run(
                image=SEN4CAP_PROCESSORS_IMAGE_NAME,
                auto_remove=True,
                user=f"{os.getuid()}:{os.getgid()}",
                volumes=self.volumes,
                working_dir=path,
                command=command,
                environment=env,
            )

            # command = []
            # command += ["optimize_gtiff.py"]
            # command += ["--no-data", 0]
            # command += [self.output]
            # run_command(command, env)

        if self.tile_ref:
            command = get_statistics_invocation(self.output, self.tile_ref)
            if command:
                run_command(command, env)


def get_projection(file):
    ds = gdal.Open(file, gdalconst.GA_ReadOnly)
    srs = osr.SpatialReference(wkt=ds.GetProjectionRef())

    # workaround for gdal-libs-1.11.4-3 not knowing about EPSG:3035
    if srs.IsLocal() and srs.GetAuthorityCode("LOCAL_CS") == "3035":
        srs.ImportFromEPSG(3035)
        return (srs, 3035)
    else:
        return (srs, None)


def get_extent(raster):
    gt = raster.GetGeoTransform()
    (cols, rows) = raster.RasterXSize, raster.RasterYSize

    extent = []

    x = gt[0]
    y = gt[3]
    extent.append((x, y))

    x = gt[0] + rows * gt[2]
    y = gt[3] + rows * gt[5]
    extent.append((x, y))

    x = gt[0] + cols * gt[1] + rows * gt[2]
    y = gt[3] + cols * gt[4] + rows * gt[5]
    extent.append((x, y))

    x = gt[0] + cols * gt[1]
    y = gt[3] + cols * gt[4]
    extent.append((x, y))

    return extent


def process_radar(args, volumes, pool):
    tiles = get_tile_footprints(args.tile_footprints)

    products = get_radar_products(args.radar_products)
    groups = defaultdict(list)
    input_srs = None
    force_input_epsg = None
    missing_products = set()
    found_products = set()
    for product in products:
        if product.path in missing_products:
            continue
        if product.path not in found_products:
            if not os.path.exists(product.path):
                print("product {} does not exist".format(product.path))
                missing_products.add(product.path)
                continue
            else:
                found_products.add(product.path)

        if input_srs is None:
            (input_srs, force_input_epsg) = get_projection(product.path)

        group_week = (
            product.week - 1
        ) // args.radar_compositing_weeks * args.radar_compositing_weeks + 1

        group = RadarGroup(
            product.year,
            product.month,
            group_week,
            product.tile_id,
            product.orbit_type_id,
            product.polarization,
            product.product_type,
        )
        groups[group].append(product)

    if input_srs is None:
        return

    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    transform = osr.CoordinateTransformation(wgs84_srs, input_srs)
    tiles_input_srs = {}
    for (tile_id, (geog, epsg_code)) in tiles.items():
        geom = geog.Clone()
        geom.Transform(transform)
        tiles_input_srs[tile_id] = geom

    groups = sorted(list(groups.items()))
    if args.lpis_path:
        ref_map = get_lpis_map(args.lpis_path, 10)
    else:
        ref_map = None

    ref_extent_map = {}
    ref_gt_map = {}
    if args.lpis_path:
        for (tile_id, path) in ref_map.items():
            ds = gdal.Open(path, gdalconst.GA_ReadOnly)
            ref_extent_map[tile_id] = get_extent(ds)
            ref_gt_map[tile_id] = ds.GetGeoTransform()
            del ds
    else:
        for (group, products) in groups:
            if group.tile_id not in ref_extent_map:
                ds = gdal.Open(products[0].path, gdalconst.GA_ReadOnly)
                ref_extent_map[group.tile_id] = get_extent(ds)
                ref_gt_map[group.tile_id] = ds.GetGeoTransform()
                del ds

    vrt_bands = defaultdict(list)
    weekly_composites = []
    backscatter_groups = defaultdict(list)
    backscatter_ratio_weekly_groups = defaultdict(BackscatterPair)
    backscatter_ratio_bi_monthly_groups = defaultdict(BackscatterPairs)
    coherence_monthly_groups = defaultdict(list)
    coherence_season_groups = defaultdict(list)
    for (group, products) in groups:
        hdrs = []
        for product in products:
            hdrs.append(product.path)

        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            tile_extent = ref_extent_map.get(group.tile_id)
            if tile_ref is None or tile_extent is None:
                continue
        else:
            tile_ref = None
            tile_extent = ref_extent_map.get(group.tile_id)

        output = os.path.join(args.path, group.format(args.site_id))
        temp = os.path.splitext(output)[0] + "_TMP.tif"

        epsg_code = tiles[group.tile_id][1]
        if args.lpis_path:
            (xmin, xmax, ymin, ymax) = tiles_input_srs[group.tile_id].GetEnvelope()
        else:
            (xmin, ymax) = tile_extent[0]
            (xmax, ymin) = tile_extent[2]

        if args.lpis_path:
            composite = WeeklyComposite(
                output,
                temp,
                tile_ref,
                xmin,
                xmax,
                ymin,
                ymax,
                force_input_epsg,
                epsg_code,
                tile_extent,
                hdrs,
            )
        else:
            composite = WeeklyCompositeS4S(
                output,
                temp,
                tile_ref,
                xmin,
                xmax,
                ymin,
                ymax,
                force_input_epsg,
                epsg_code,
                tile_extent,
                hdrs,
                volumes,
            )
        weekly_composites.append(composite)
        tile_vrt_bands = vrt_bands[group.tile_id]
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "UInt16",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description()),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("1"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "UInt16",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)

        if group.product_type == PRODUCT_TYPE_ID_BCK:
            month = group.month
            backscatter_group_month = (month - 1) // 2 * 2 + 1

            backscatter_group = BackscatterBiMonthlyGroup(
                group.year,
                backscatter_group_month,
                group.tile_id,
                group.orbit_type_id,
                group.polarization,
            )
            backscatter_groups[backscatter_group].append(output)

            ratio_group = BackscatterRatioBiMonthlyGroup(
                group.year, backscatter_group_month, group.tile_id, group.orbit_type_id
            )
            if group.polarization == POLARIZATION_VV:
                backscatter_ratio_bi_monthly_groups[ratio_group].vv.append(output)
            elif group.polarization == POLARIZATION_VH:
                backscatter_ratio_bi_monthly_groups[ratio_group].vh.append(output)

            ratio_group = BackscatterWeeklyRatioGroup(
                group.year, group.week, group.tile_id, group.orbit_type_id
            )
            if group.polarization == POLARIZATION_VV:
                backscatter_ratio_weekly_groups[ratio_group].vv = output
            elif group.polarization == POLARIZATION_VH:
                backscatter_ratio_weekly_groups[ratio_group].vh = output
        elif group.product_type == PRODUCT_TYPE_ID_COHE:
            month = group.month

            coherence_monthly_group = CoherenceMonthlyGroup(
                group.year, month, group.tile_id, group.polarization
            )
            coherence_monthly_groups[coherence_monthly_group].append(output)

            coherence_season_group = CoherenceSeasonGroup(
                group.tile_id, group.polarization
            )
            coherence_season_groups[coherence_season_group].append(output)

    pool.map(lambda c: c.run(), weekly_composites)

    backscatter_composites = []
    backscatter_groups = sorted(list(backscatter_groups.items()))
    for (group, products) in backscatter_groups:
        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            if tile_ref is None:
                continue
        else:
            tile_ref = None

        output = os.path.join(args.path, group.format(args.site_id))
        output_extended = get_otb_extended_filename_with_tiling(output)

        composite = BackscatterMonthlyComposite(
            tile_ref, output, output_extended, "simple", products, volumes
        )
        backscatter_composites.append(composite)

        tile_vrt_bands = vrt_bands[group.tile_id]
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "Float32",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description(1)),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("1"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "Float32",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "Float32",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description(2)),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("2"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "Float32",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)

    backscater_ratio_statistics = []
    backscatter_ratio_weekly_groups = sorted(
        list(backscatter_ratio_weekly_groups.items())
    )
    for (group, pair) in backscatter_ratio_weekly_groups:
        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            if tile_ref is None:
                continue
        else:
            tile_ref = None

        if pair.vv is None or pair.vh is None:
            continue

        output = os.path.join(args.path, group.format(args.site_id))

        statistics = WeeklyRatioStatistics(output, pair.vv, pair.vh, tile_ref)
        backscater_ratio_statistics.append(statistics)

    backscatter_ratio_bi_monthly_groups = sorted(
        list(backscatter_ratio_bi_monthly_groups.items())
    )
    for (group, pair) in backscatter_ratio_bi_monthly_groups:
        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            if tile_ref is None:
                continue
        else:
            tile_ref = None

        output = os.path.join(args.path, group.format(args.site_id))
        output_extended = get_otb_extended_filename_with_tiling(output)

        pair.vv.sort()
        pair.vh.sort()

        products = []
        for (vv, vh) in zip(pair.vv, pair.vh):
            products += [vv, vh]

        if products:
            composite = BackscatterMonthlyComposite(
                tile_ref, output, output_extended, "ratio", products, volumes
            )
            backscatter_composites.append(composite)

            tile_vrt_bands = vrt_bands[group.tile_id]
            vrt_raster_band = E.VRTRasterBand(
                {
                    "dataType": "Float32",
                    "band": str(len(tile_vrt_bands) + 1),
                    # "blockXSize": str(1024),
                    # "blockYSize": str(1024),
                },
                E.Description(group.band_description(1)),
                E.SimpleSource(
                    E.SourceFileName({"relativeToVRT": "1"}, output),
                    E.SourceBand("1"),
                    E.SourceProperties(
                        {
                            "RasterXSize": str(10980),
                            "RasterYSize": str(10980),
                            "DataType": "Float32",
                            # "BlockXSize": str(1024),
                            # "BlockYSize": str(1024),
                        }
                    ),
                ),
            )
            tile_vrt_bands.append(vrt_raster_band)
            vrt_raster_band = E.VRTRasterBand(
                {
                    "dataType": "Float32",
                    "band": str(len(tile_vrt_bands) + 1),
                    # "blockXSize": str(1024),
                    # "blockYSize": str(1024),
                },
                E.Description(group.band_description(2)),
                E.SimpleSource(
                    E.SourceFileName({"relativeToVRT": "1"}, output),
                    E.SourceBand("2"),
                    E.SourceProperties(
                        {
                            "RasterXSize": str(10980),
                            "RasterYSize": str(10980),
                            "DataType": "Float32",
                            # "BlockXSize": str(1024),
                            # "BlockYSize": str(1024),
                        }
                    ),
                ),
            )
            tile_vrt_bands.append(vrt_raster_band)

    coherence_monthly_composites = []
    coherence_monthly_groups = sorted(list(coherence_monthly_groups.items()))
    for (group, products) in coherence_monthly_groups:
        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            if tile_ref is None:
                continue
        else:
            tile_ref = None

        output = os.path.join(args.path, group.format(args.site_id))
        output_extended = get_otb_extended_filename_with_tiling(output)

        composite = CoherenceMonthlyComposite(
            tile_ref, output, output_extended, products, volumes
        )
        coherence_monthly_composites.append(composite)

        tile_vrt_bands = vrt_bands[group.tile_id]
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "Float32",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description(1)),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("1"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "Float32",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "Float32",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description(2)),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("2"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "Float32",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)

    coherence_season_composites = []
    coherence_season_groups = sorted(list(coherence_season_groups.items()))
    for (group, products) in coherence_season_groups:
        if args.lpis_path:
            tile_ref = ref_map.get(group.tile_id)
            if tile_ref is None:
                continue
        else:
            tile_ref = None

        output = os.path.join(args.path, group.format(args.site_id))
        output_extended = get_otb_extended_filename_with_tiling(output)

        composite = CoherenceSeasonComposite(
            tile_ref, output, output_extended, products, volumes
        )
        coherence_season_composites.append(composite)

        tile_vrt_bands = vrt_bands[group.tile_id]
        vrt_raster_band = E.VRTRasterBand(
            {
                "dataType": "Float32",
                "band": str(len(tile_vrt_bands) + 1),
                # "blockXSize": str(1024),
                # "blockYSize": str(1024),
            },
            E.Description(group.band_description()),
            E.SimpleSource(
                E.SourceFileName({"relativeToVRT": "1"}, output),
                E.SourceBand("1"),
                E.SourceProperties(
                    {
                        "RasterXSize": str(10980),
                        "RasterYSize": str(10980),
                        "DataType": "Float32",
                        # "BlockXSize": str(1024),
                        # "BlockYSize": str(1024),
                    }
                ),
            ),
        )
        tile_vrt_bands.append(vrt_raster_band)

    pool.map(lambda c: c.run(), backscatter_composites)
    pool.map(lambda c: c.run(), backscater_ratio_statistics)
    pool.map(lambda c: c.run(), coherence_monthly_composites)
    pool.map(lambda c: c.run(), coherence_season_composites)

    for (tile_id, vrt_bands) in vrt_bands.items():
        gt = ref_gt_map[tile_id]
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(tiles[tile_id][1])
        vrt_dataset = E.VRTDataset(
            {
                "rasterXSize": str(10980),
                "rasterYSize": str(10980),
            },
            E.SRS({"dataAxisToSRSAxisMapping": "1,2"}, srs.ExportToWkt()),
            E.GeoTransform(
                "{}, {}, {}, {}, {}, {}".format(
                    gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]
                )
            ),
            # E.BlockXSize(str(1024)),
            # E.BlockYSize(str(1024)),
        )
        for vrt_band in vrt_bands:
            vrt_dataset.append(vrt_band)

        root = etree.ElementTree(vrt_dataset)
        bands_vrt = f"S1_{tile_id}.vrt"
        root.write(bands_vrt, pretty_print=True, encoding="utf-8")


def generate_headers(
    date_file, headers_mean, headers_dev, headers_re_mean, headers_re_dev, red_edge
):
    dates = []
    with open(date_file, "r") as file:
        for line in file:
            days = int(line.split()[1])
            dt = epoch_days_to_date(days)
            dates.append(dt)

    bands = [
        "b3",
        "b4",
        "b8",
        "b11",
        "ndvi",
        "ndwi",
        "brightness",
    ]

    with open(headers_mean, "w") as file:
        file.write("NewID")
        for dt in dates:
            for band in bands:
                date_string = dt.strftime("%Y_%m_%d")
                column = "XX_{}_s2_mean_{}".format(date_string, band)
                file.write("," + column)
        file.write("\n")

    with open(headers_dev, "w") as file:
        file.write("NewID")
        for dt in dates:
            for band in bands:
                date_string = dt.strftime("%Y_%m_%d")
                column = "XX_{}_s2_dev_{}".format(date_string, band)
                file.write("," + column)
        file.write("\n")

    if not red_edge:
        return

    bands = ["b5", "b6", "b7", "b12"]

    with open(headers_re_mean, "w") as file:
        file.write("NewID")
        for dt in dates:
            for band in bands:
                date_string = dt.strftime("%Y_%m_%d")
                column = "XX_{}_s2_mean_{}".format(date_string, band)
                file.write("," + column)
        file.write("\n")

    with open(headers_re_dev, "w") as file:
        file.write("NewID")
        for dt in dates:
            for band in bands:
                date_string = dt.strftime("%Y_%m_%d")
                column = "XX_{}_s2_dev_{}".format(date_string, band)
                file.write("," + column)
        file.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Crop type processor")
    parser.add_argument("-s", "--site-id", type=int, help="site ID to filter by")
    parser.add_argument(
        "-m", "--mode", required=True, choices=["optical", "sar"], help="mode"
    )
    parser.add_argument("-p", "--path", default=".", help="working path")
    parser.add_argument(
        "--lpis-path", required=False, help="path to the rasterized LPIS"
    )
    parser.add_argument("--tile-footprints", required=False, help="tile footprints CSV")
    parser.add_argument(
        "--optical-products", required=False, help="optical products CSV"
    )
    parser.add_argument("--radar-products", required=False, help="radar products CSV")
    parser.add_argument(
        "--radar-compositing-weeks",
        default=1,
        type=int,
        help="radar compositing period in weeks",
    )
    parser.add_argument("--mounts", help="paths to mount in containers", nargs="*")

    re = parser.add_mutually_exclusive_group(required=False)
    re.add_argument(
        "--re",
        help="Include red edge bands (default)",
        default=True,
        action="store_true",
    )
    re.add_argument(
        "--no-re", help="Don't include red edge bands", dest="re", action="store_false"
    )

    args = parser.parse_args()

    cpu_count = multiprocessing.cpu_count()
    if args.mode == "optical":
        if not args.optical_products:
            print("--optical-products is required with -m optical")
            sys.exit(1)
        pool = multiprocessing.dummy.Pool(cpu_count // 2)
    else:
        if not args.tile_footprints:
            print("--tile-footprints is required with -m sar")
            sys.exit(1)
        if not args.radar_products:
            print("--radar-products is required with -m sar")
            sys.exit(1)
        pool = multiprocessing.dummy.Pool(cpu_count)

    abs_out_path = os.path.abspath(args.path)
    volumes = {
        abs_out_path: {"bind": os.path.abspath(args.path), "mode": "rw"},
    }
    if args.lpis_path:
        volumes[args.lpis_path] = {"bind": args.lpis_path, "mode": "ro"}
    if args.mounts:
        for mount in args.mounts:
            volumes[mount] = {"bind": mount, "mode": "ro"}

    if args.mode == "optical":
        process_optical(args, pool, SATELLITE_ID_SENTINEL2)
    else:
        process_radar(args, volumes, pool)


if __name__ == "__main__":
    main()
