#!/usr/bin/env python
import argparse
from concurrent.futures import wait, ThreadPoolExecutor
from lxml import etree
from lxml.builder import E
from osgeo import gdal
import subprocess

def get_band_element(band, index, out_index, path):
    block_size_x, block_size_y = band.GetBlockSize()
    data_type_name = gdal.GetDataTypeName(band.DataType)
    vrt_raster_band = E.VRTRasterBand(
        {
            "dataType": data_type_name,
            "band": str(out_index),
            "blockXSize": str(block_size_x),
            "blockYSize": str(block_size_y),
        },
        E.Description(band.GetDescription()),
        E.SimpleSource(
            E.SourceFileName({"relativeToVRT": "1"}, path),
            E.SourceBand(str(index)),
            E.SourceProperties(
                {
                    "RasterXSize": str(band.XSize),
                    "RasterYSize": str(band.YSize),
                    "DataType": data_type_name,
                    "BlockXSize": str(block_size_x),
                    "BlockYSize": str(block_size_y),
                }
            ),
        ),
    )
    return vrt_raster_band

def main():
    parser = argparse.ArgumentParser(
        description="Prepare input data for Broceliande"
    )
    parser.add_argument("--tiles", help="tile filter", nargs="+", required=True)
    parser.add_argument("path", help="path")
    args = parser.parse_args()

    tiles = args.tiles
    commands = []
    for tile in tiles:
        b04 = f"S2_B04_{tile}.tif"
        b08 = f"S2_B08_{tile}.tif"

        ds_b04 = gdal.Open(b04, gdal.gdalconst.GA_ReadOnly)
        ds_b08 = gdal.Open(b08, gdal.gdalconst.GA_ReadOnly)

        gt = ds_b04.GetGeoTransform()
        size_x = ds_b04.RasterXSize
        size_y = ds_b08.RasterYSize
        projection = ds_b04.GetProjection()
        band = ds_b04.GetRasterBand(1)
        block_size_x, block_size_y = band.GetBlockSize()

        vrt_dataset = E.VRTDataset(
            {
                "rasterXSize": str(size_x),
                "rasterYSize": str(size_y),
            },
            E.SRS(projection),
            E.GeoTransform(
                "{}, {}, {}, {}, {}, {}".format(
                    gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]
                )
            ),
            E.BlockXSize(str(block_size_x)),
            E.BlockYSize(str(block_size_y)),
        )

        band_count_b04 = ds_b04.RasterCount
        band_count_b08 = ds_b08.RasterCount
        band_count = min(band_count_b04, band_count_b08)
        for b in range(1, band_count + 1):
            band = ds_b04.GetRasterBand(b)
            vrt_raster_band_b04 = get_band_element(band, b, 2 * b - 1, b04)
            vrt_dataset.append(vrt_raster_band_b04)

            band = ds_b08.GetRasterBand(b)
            vrt_raster_band_b08 = get_band_element(band, b, 2 * b, b08)
            vrt_dataset.append(vrt_raster_band_b08)

        root = etree.ElementTree(vrt_dataset)
        broceliande_vrt = f"broceliande_{tile}.vrt"
        root.write(broceliande_vrt, pretty_print=True, encoding="utf-8")

        training_polygons = f"training_polygons_{tile}.shp"
        training_raster = f"training_polygons_{tile}.tif"
        validation_polygons = f"validation_polygons_{tile}.shp"
        validation_raster = f"validation_polygons_{tile}.tif"

        spacing_x, spacing_y = gt[1], gt[5]
        xmin, ymax = gt[0], gt[3]
        xmax, ymin = xmin + spacing_x * size_x, ymax + spacing_y * size_y

        # FIXME: use code_n1 from the shapefile
        training_broceliande_sql = f"""
        select classes.land_cover_class as crop_code
        from training_polygons_{tile} parcels
        join 'classes.csv'.classes on parcels.id = classes.parcel_id
        """

        validation_broceliande_sql = f"""
        select classes.land_cover_class as crop_code
        from validation_polygons_{tile} parcels
        join 'classes.csv'.classes on parcels.id = classes.parcel_id
        """

        command = []
        command += ["gdal_rasterize", "-q"]
        command += ["-sql", training_broceliande_sql]
        command += ["-a", "crop_code"]
        command += ["-a_nodata", "0"]
        command += ["-a_srs", projection]
        command += ["-te", str(xmin), str(ymin), str(xmax), str(ymax)]
        command += ["-tr", str(spacing_x), str(spacing_y)]
        command += ["-ot", "Byte"]
        command += [training_polygons, training_raster]
        commands.append(command)

        command = []
        command += ["gdal_rasterize", "-q"]
        command += ["-sql", validation_broceliande_sql]
        command += ["-a", "crop_code"]
        command += ["-a_nodata", "0"]
        command += ["-a_srs", projection]
        command += ["-te", str(xmin), str(ymin), str(xmax), str(ymax)]
        command += ["-tr", str(spacing_x), str(spacing_y)]
        command += ["-ot", "Byte"]
        command += [validation_polygons, validation_raster]
        commands.append(command)

    with ThreadPoolExecutor() as executor:
        executor.map(subprocess.call, commands)

if __name__ == "__main__":
    main()

# docker run --rm -it -v $PWD:$PWD -w $PWD -u $(id -u):$(id -g) docker.io/sen4x/gdal-py:3.3.2
# ./prepare-broceliande.py . --tiles 29TPE 29TPF 29TPG 29TPH 29TQH 30TTK 30TTL 30TTM 30TUK 30TUL 30TUM 30TUN 30TVL 30TVM 30TVN 30TWL 30TWM 30TWN
