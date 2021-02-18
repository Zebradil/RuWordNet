#!/usr/bin/env python3

import argparse
import csv
import logging
import sys

from psycopg2 import connect

parser = argparse.ArgumentParser(
    description="Import corrected morph data of RuThes entries from CSV file."
)
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
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
with conn.cursor() as cur:
    sql = """
      UPDATE v2_text_entry
         SET synt_type = %(synt_type)s,
             pos_string = %(pos_string)s
       WHERE name = %(entry_name)s
         AND (synt_type <> %(synt_type)s
          OR pos_string <> %(pos_string)s
          OR pos_string IS NULL
          OR synt_type IS NULL
         )"""

    for n, row in enumerate(csv.DictReader(sys.stdin)):

        if ARGS.dry_run:
            logging.info("DRY-RUN: update entry %s", row)
        else:
            params = {
                k: v
                for k, v in row.items()
                if k in {"pos_string", "synt_type", "entry_name"}
            }
            logging.debug("Query params: %s", params)
            cur.execute(sql, params)
            if cur.rowcount:
                logging.info("Updated: %s", row)
                conn.commit()
