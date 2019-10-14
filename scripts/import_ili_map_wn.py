#!/usr/bin/env python3

import argparse
import logging
import os
import re

from psycopg2 import IntegrityError, connect

parser = argparse.ArgumentParser(
    description="Import mapping of Inter Lingual Index data to Wordnet data."
)
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
parser.add_argument(
    "-f", "--file", type=str, help="Source tab-separated file", required=True
)
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument(
    "-v",
    "--version",
    help="Prinston Wordnet version (30, 31, ...)",
    type=int,
    required=True,
)
parser.add_argument(
    "-n", "--dry-run", help="Do not do changes in the database", action="store_true"
)

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)

conn = connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    inserted_lines = 0
    skipped_lines = 0
    for line in file:
        parts = line.strip().split("\t")
        values = {"ili": parts[0], "wn": parts[1], "version": ARGS.version}

        if ARGS.dry_run:
            logging.info(f"DRY-RUN: insert values {values}")
        else:
            try:
                cur.execute(
                    f"""
                    INSERT INTO ili_map_wn (ili, wn, version)
                    VALUES (%(ili)s, %(wn)s, %(version)s)
                    """,
                    values,
                )
                conn.commit()
                inserted_lines += 1
            except IntegrityError as e:
                logging.error(e)
                conn.rollback()
                skipped_lines += 1
logging.info(f"Inserted: {inserted_lines}")
logging.info(f"Skipped: {skipped_lines}")
