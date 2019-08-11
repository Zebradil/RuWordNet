#!/usr/bin/env python3

import argparse
import logging

from psycopg2 import IntegrityError, connect

parser = argparse.ArgumentParser(description="Import Inter Lingual Index data.")
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument("-f", "--file", type=str, help="Source tab-separated file")
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument("-n", "--dry-run", help="Do not do changes in the database", action="store_true")

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)

conn = connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    inserted_lines = 0
    skipped_lines = 0
    for line in file:
        parts = line.strip().split("\t")
        values = {
            "link_type": parts[0],
            "concept_id": parts[1],
            "wn_lemma": parts[4],
            "wn_id": parts[6],
            "wn_gloss": parts[7],
        }

        if ARGS.dry_run:
            logging.info(f"DRY-RUN: insert values {values}")
        else:
            try:
                cur.execute(
                    """INSERT INTO ili (link_type, concept_id, wn_lemma, wn_id, wn_gloss)
                    VALUES (%(link_type)s, %(concept_id)s, %(wn_lemma)s, %(wn_id)s, %(wn_gloss)s)""",
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
