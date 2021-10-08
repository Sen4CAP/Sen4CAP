#!/usr/bin/env python3
import argparse
from collections import defaultdict
from configparser import ConfigParser
import logging
import psycopg2
from psycopg2.sql import SQL, Identifier, Literal
import toml

PRODUCT_TYPE_S4C_L4A = 12
PRODUCT_TYPE_S4C_L4C = 15
PRODUCT_TYPE_LPIS = 14


class Config:
    def __init__(self, args):
        parser = ConfigParser()
        parser.read([args.config_file])

        self.host = parser.get("Database", "HostName")

        # work around Docker networking scheme
        if self.host == "127.0.0.1" or self.host == "::1" or self.host == "localhost":
            self.host = "172.17.0.1"

        self.port = int(parser.get("Database", "Port", vars={"Port": "5432"}))
        self.dbname = parser.get("Database", "DatabaseName")
        self.user = parser.get("Database", "UserName")
        self.password = parser.get("Database", "Password")

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

    def get_connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}"


def get_tilesets(conn, cursor):
    tilesets = []
    logging.info("Retrieving sites")
    query = "select id, short_name from site;"
    logging.debug(query)
    cursor.execute(query)
    sites = cursor.fetchall()

    logging.info("Retrieving LPIS products")
    query = """
        select id,
                site_id,
                name,
                created_timestamp
        from product
        where product_type_id = %s;
    """
    logging.debug(query)
    cursor.execute(query, (PRODUCT_TYPE_LPIS,))
    site_lpis_products = defaultdict(list)
    for (id, site_id, name, date) in cursor:
        site_lpis_products[site_id].append((id, name, date))

    logging.info("Retrieving LPIS tables")
    query = """
        select table_name
        from information_schema.tables
        where table_schema = 'public'
            and table_name like 'decl_%';
    """
    logging.debug(query)
    cursor.execute(query)
    lpis_tables = set()
    for (table,) in cursor:
        lpis_tables.add(table)

    logging.info("Retrieving LUT tables")
    query = """
        select table_name
        from information_schema.tables
        where table_schema = 'public'
            and table_name like 'lut_%';
    """
    logging.debug(query)
    cursor.execute(query)
    lut_tables = set()
    for (table,) in cursor:
        lut_tables.add(table)

    found_lpis_tables = []
    site_lpis_tables = defaultdict(list)
    for (site_id, short_name) in sites:
        tables = site_lpis_tables[site_id]
        for (id, name, date) in site_lpis_products[site_id]:
            year = date.year
            lpis_table_name = f"decl_{short_name}_{year}"
            if lpis_table_name in lpis_tables:
                found_lpis_tables.append((lpis_table_name, name))
                tables.append((lpis_table_name, date))
        tables.sort(key=lambda t: t[1])

    lpis_table_info = {}
    for (lpis_table, lpis_product) in found_lpis_tables:
        query = SQL(
            """
            select Find_SRID('public', %s, 'wkb_geometry') as srid,
                    ST_Extent(wkb_geometry)
            from {};
            """
        ).format(Identifier(lpis_table))
        logging.debug(query)
        cursor.execute(query, (lpis_table,))
        row = cursor.fetchone()
        if not row:
            continue
        (srid, bbox) = row

        # BBOX(1 2,3 4)
        bbox = bbox[4 : len(bbox) - 1]
        (p1, p2) = bbox.split(",")
        ((x1, y1), (x2, y2)) = (p1.split(" "), p2.split(" "))
        extent = [float(x1), float(y1), float(x2), float(y2)]

        lpis_table_info[lpis_table] = (srid, extent)

        layer = {
            "name": lpis_product,
            "table_name": lpis_table,
            "geometry_field": "wkb_geometry",
            "geometry_type": "MULTIPOLYGON",
            "srid": srid,
        }
        tileset = {
            "name": lpis_product,
            "extent": extent,
            "layer": [layer],
        }
        tilesets.append(tileset)

    logging.info("Retrieving L4A products")
    query = """
        select id,
                site_id,
                name,
                created_timestamp
        from product
        where product_type_id = %s;
    """
    logging.debug(query)
    cursor.execute(query, (PRODUCT_TYPE_S4C_L4A,))
    for (id, site_id, name, date) in cursor:
        # HACK: find the latest LPIS product that's earlier than the L4A
        lpis_table = None
        for (table, lpis_date) in reversed(site_lpis_tables[site_id]):
            if lpis_date < date:
                lpis_table = table
                break
        if not lpis_table:
            continue

        lut_table = "lut" + lpis_table[4:]
        if lut_table not in lut_tables:
            continue

        info = lpis_table_info.get(lpis_table)
        if not info:
            continue
        (srid, extent) = info

        query = SQL(
            'select * from {} lpis inner join product_details_l4a l4a using ("NewID") inner join {} lut using ("ori_crop") where l4a.product_id = {}'
        ).format(Identifier(lpis_table), Identifier(lut_table), Literal(id))
        sql = query.as_string(conn)

        layer = {
            "name": name,
            "geometry_field": "wkb_geometry",
            "geometry_type": "MULTIPOLYGON",
            "srid": srid,
            "query": [{"sql": sql}],
        }
        tileset = {
            "name": name,
            "extent": extent,
            "layer": [layer],
        }
        tilesets.append(tileset)

    logging.info("Retrieving L4C products")
    query = """
        select id,
                site_id,
                name,
                created_timestamp
        from product
        where product_type_id = %s;
    """
    logging.debug(query)
    cursor.execute(query, (PRODUCT_TYPE_S4C_L4C,))
    for (id, site_id, name, date) in cursor:
        # HACK: find the latest LPIS product that's earlier than the L4C
        lpis_table = None
        for (table, lpis_date) in reversed(site_lpis_tables[site_id]):
            if lpis_date < date:
                lpis_table = table
                break
        if not lpis_table:
            continue

        lut_table = "lut" + lpis_table[4:]
        if lut_table not in lut_tables:
            continue

        info = lpis_table_info.get(lpis_table)
        if not info:
            continue
        (srid, extent) = info

        query = SQL(
            'select * from {} lpis inner join product_details_l4c l4c using ("NewID") inner join {} lut using ("ori_crop") where l4c.product_id = {}'
        ).format(Identifier(lpis_table), Identifier(lut_table), Literal(id))
        sql = query.as_string(conn)

        layer = {
            "name": name,
            "geometry_field": "wkb_geometry",
            "geometry_type": "MULTIPOLYGON",
            "srid": srid,
            "query": [{"sql": sql}],
        }
        tileset = {
            "name": name,
            "extent": extent,
            "layer": [layer],
        }
        tilesets.append(tileset)


def main():
    parser = argparse.ArgumentParser(description="Generates a t-rex configuration file")
    parser.add_argument(
        "-c",
        "--config-file",
        default="/etc/sen2agri/sen2agri.conf",
        help="configuration file location",
    )
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    parser.add_argument(
        "--stub", help="create a configuration stub", action="store_true"
    )
    parser.add_argument("output", help="output file")
    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)

    config = Config(args)

    with config.get_connection() as conn:
        with conn.cursor() as cursor:
            if args.stub:
                tilesets = []
            else:
                tilesets = get_tilesets(conn, cursor)

    t_rex = {
        "service": {"mvt": {"viewer": True}},
        "datasource": [
            {
                "dbconn": config.get_connection_string(),
                "name": "dbconn",
                "default": True,
            }
        ],
        "grid": {"predefined": "web_mercator"},
        "webserver": {
            "bind": "0.0.0.0",
            "port": 6767,
        },
        "tileset": tilesets,
    }

    with open(args.output, "wt") as f:
        toml.dump(t_rex, f)


if __name__ == "__main__":
    main()
