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
parser.add_argument("-f", "--file", type=str, help="Source tab-separated file")
parser.add_argument(
    "-t", "--table", type=str, help="Database table name (will be truncated!)"
)
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument(
    "-n", "--dry-run", help="Do not do changes in the database", action="store_true"
)

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)

conn = connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name=%s
        LIMIT 1
        """,
        (ARGS.table,),
    )
    if cur.fetchone():
        logging.info(
            "Truncate table %s%s", ARGS.table, " (dry run)" if ARGS.dry_run else ""
        )
        if not ARGS.dry_run:
            cur.execute(f"TRUNCATE {ARGS.table}")
    else:
        logging.info(
            "Create table %s%s", ARGS.table, " (dry run)" if ARGS.dry_run else ""
        )
        if not ARGS.dry_run:
            cur.execute(
                f"""
                CREATE TABLE {ARGS.table} (
                    ili TEXT PRIMARY KEY,
                    wn TEXT,
                    extra TEXT
                )
                """
            )

    inserted_lines = 0
    skipped_lines = 0
    for line in file:
        if not line.startswith("<"):
            continue
        parts = line.strip().split("\t")
        match = re.search(r"^[^:]+:(\S+)\W+(.*)$", parts[2])
        values = {
            "ili": parts[0].strip("<>"),
            "wn": match.group(1),
            "extra": match.group(2),
        }

        if ARGS.dry_run:
            logging.info(f"DRY-RUN: insert values {values}")
        else:
            try:
                cur.execute(
                    f"""
                    INSERT INTO {ARGS.table} (ili, wn, extra)
                    VALUES (%(ili)s, %(wn)s, %(extra)s)
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
