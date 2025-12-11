#!/usr/bin/env python3

import argparse
import itertools
import json
import sys
from enum import Enum
from pathlib import Path
from urllib.parse import urljoin

import psycopg2
import psycopg2.extras
import requests
from psycopg2.sql import SQL


class ProductType(Enum):
    L2A = 1
    L3B = 3
    L1C = 7
    S1_L2A_AMP = 10
    S1_L2A_COHE = 11
    S4C_L4A = 12
    S4C_L4B = 13
    LPIS = 14
    S4C_L4C = 15
    S4C_MDB1 = 17
    S4C_MDB2 = 18
    S4C_MDB3 = 19
    S4C_MDB_L4A_OPT_MAIN = 20
    S4C_MDB_L4A_OPT_RE = 21
    S4C_MDB_L4A_SAR_MAIN = 22
    S4C_MDB_L4A_SAR_TEMP = 23
    S4C_HETEROGENEITY = 24
    FMASK = 25
    L2A_MSK = 26
    S4C_BARE_SOIL = 33
    S4C_CHANGE_DETECTION = 35
    DUMMY = 99


def read_l2a(root: Path):
    entries = list(root.glob("*"))

    for e in entries:
        if e.name == "MTD_MSIL2A.xml":
            return e

    granule = next((e for e in entries if e.name.startswith("SENTINEL2")), None)
    if not granule:
        return None

    metadata = next(granule.glob("*_MTD_ALL.xml"), None)
    return metadata


def read_l3b(root: Path):
    parts = root.name.split("_")
    metadata = root.joinpath(f"{parts[0]}_{parts[1]}_MTD_{parts[5]}.xml")
    tile = next(iter(root.glob("TILES/*")), None)
    if not tile:
        return None, None, None
    images = itertools.chain(tile.glob("IMG_DATA/*.TIF"), tile.glob("QI_DATA/*.TIF"))
    assets = {}
    for image in images:
        kind = image.name.split("_")[2]
        assets[kind] = str(image)
    parts = tile.name.split("_")
    preview = tile.joinpath(f"{parts[0]}_{parts[1]}_PVI_{parts[2]}_{parts[3]}.jpg")
    return str(metadata), assets, str(preview)


def build_item(row):
    product_type = ProductType(row["product_type_id"])

    item = {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": row["name"] if row["name"] else str(row["id"]),
        "collection": product_type.name,
        "geometry": row["geom"],
        "bbox": None,
        "properties": {},
        "assets": {},
        "links": [],
    }

    if item["geometry"]:
        coords = item["geometry"]["coordinates"][0]
        xs = [pt[0] for pt in coords]
        ys = [pt[1] for pt in coords]
        item["bbox"] = [min(xs), min(ys), max(xs), max(ys)]

    props = {}
    if row.get("created_timestamp"):
        props["datetime"] = row["created_timestamp"].isoformat()
    item["properties"] = props

    assets = {}
    product_assets = None
    preview = None
    full_path = Path(row["full_path"])
    if product_type == ProductType.L2A:
        metadata = read_l2a(full_path)
    elif product_type == ProductType.L3B:
        metadata, product_assets, preview = read_l3b(full_path)
    if metadata:
        assets["metadata"] = {
            "href": str(metadata),
            "title": "Product manifest",
            "type": "application/xml",
            "roles": ["metadata"],
        }
    if product_assets:
        for kind, image in product_assets.items():
            assets[kind] = {
                "href": str(image),
                "type": "image/tiff",
                "roles": ["data"],
            }

    quicklook_image = row.get("quicklook_image") or ""
    if preview:
        thumbnail = preview
    else:
        thumbnail = full_path.joinpath(quicklook_image)
    if thumbnail:
        assets["thumbnail"] = {
            "href": str(thumbnail),
            "type": "image/jpeg",
            "roles": ["thumbnail"],
        }
    item["assets"] = assets

    return item


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    ap.add_argument("--stac-url", help="STAC server URL")
    ap.add_argument("--product-id", type=int, help="Product filter")
    ap.add_argument("--product-type-id", type=int, help="Product type filter")
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = SQL("""
        select
            id,
            product_type_id,
            site_id,
            full_path,
            created_timestamp,
            inserted_timestamp,
            name,
            quicklook_image,
            ST_AsGeoJSON(geog) :: json AS geom
        from product
    """)

    filters = []
    params = []

    if args.product_type_id or args.product_id:
        query += SQL(" where ")
    if args.product_type_id:
        filters.append(SQL("product_type_id = %s"))
        params.append(args.product_type_id)
    if args.product_id:
        filters.append(SQL("id = %s"))
        params.append(args.product_id)
    query += SQL(" and ").join(filters)
    query += SQL(" order by id")

    cur.execute(
        query,
        params,
    )

    ok = True
    for r in cur:
        item = build_item(r)

        if args.stac_url:
            collection = item["collection"]
            url = urljoin(args.stac_url, f"collections/{collection}/items")
            response = requests.post(f"{url}", json=item)
            if not response:
                print(f"{item['id']}: {response.text}", file=sys.stderr)
                ok = False
            else:
                print(f"{url}/{item['id']}")
        else:
            with open(f"{item['id']}.json", "w") as file:
                json.dump(item, file)

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
