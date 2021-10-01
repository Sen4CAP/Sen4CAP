#!/usr/bin/env python
from __future__ import print_function

import argparse
import os
from osgeo import ogr
from osgeo import gdalconst
import zipfile
import tempfile

ERR_NONE = 0
ERR_IO = 1
ERR_NO_COLS = 2
ERR_UNSUPPORTED_INPUT = 3
ERR_NO_SHP_IN_ZIP = 4


def read_shp_columns(shp):
    dataSource = ogr.Open(shp, gdalconst.GA_ReadOnly)

    if dataSource is None:
        print("Could not open {}".format(shp))
        return ERR_IO

    layer = dataSource.GetLayer()
    # feature_count = layer.GetFeatureCount()
    # print("{} feature(s) found".format(feature_count))

    schema = []
    ldefn = layer.GetLayerDefn()
    for n in range(ldefn.GetFieldCount()):
        fdefn = ldefn.GetFieldDefn(n)
        name = fdefn.GetName()
        # fieldTypeCode = fdefn.GetType()
        # fieldType = fdefn.GetFieldTypeName(fieldTypeCode)
        # print("Field name = {}, fieldTypeCode= {}, fieldType = {}".format(name, fieldTypeCode, fieldType))
        schema.append(name)

    if len(schema) == 0:
        print("No column found in file {}".format(shp))
        return ERR_NO_COLS

    print("{}".format(schema))
    return ERR_NONE


def main():
    # Preserve encoding to UTF-8
    os.environ["SHAPE_ENCODING"] = "utf-8"

    parser = argparse.ArgumentParser(
        description="Extracts the field names from a shapefile"
    )
    parser.add_argument("-p", "--path", help="Input path")

    args = parser.parse_args()

    if args.path.endswith(".zip"):
        zf = zipfile.ZipFile(args.path)
        with tempfile.TemporaryDirectory() as tempdir:
            zf.extractall(tempdir)
            for file in os.listdir(tempdir):
                if file.endswith(".shp"):
                    shp = os.path.join(tempdir, file)
                    return read_shp_columns(shp)

        print("No shp found in the root of the provided archive {}".format(args.path))
        return ERR_NO_SHP_IN_ZIP

    elif args.path.endswith(".shp"):
        return read_shp_columns(args.path)
    else:
        print("Unsupported file type (should be zip or shp) {}".format(args.path))
        return ERR_UNSUPPORTED_INPUT


if __name__ == "__main__":
    main()
