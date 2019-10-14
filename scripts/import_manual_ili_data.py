#!/usr/bin/env python3

import argparse
import logging
import re

import psycopg2

parser = argparse.ArgumentParser(description="Import Inter Lingual Index data.")
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
parser.add_argument("-f", "--file", type=str, help="Source tab-separated file")
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

logging.basicConfig(level=logging.WARN)

conn = psycopg2.connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    inserted_lines = 0
    skipped_lines = 0
    line_number = 0
    for line in file:
        line_number += 1
        line = line.strip()
        if line == "":
            continue
        match = re.search(
            r"^(I_S|I_NS)\s+\{(\d+)\}\s+(\w)\s+(\[[^\]]+\]\s+\[([^\]]+)\]\s+.+)\s+(\d+)$",
            line,
        )
        if match is None:
            logging.warning(f'Malformed line {line_number}: "{line}"')
            skipped_lines += 1
            continue

        parts = match.groups()
        values = {
            "link_type": parts[0],
            "concept_id": parts[5],
            "wn_lemma": parts[4],
            "wn_id": "-".join((parts[1], parts[2])),
            "wn_gloss": parts[3],
            "source": "manual",
        }

        if ARGS.dry_run:
            logging.info(f"DRY-RUN: insert values {values}")
        else:
            try:
                cur.execute(
                    """INSERT INTO ili (link_type, concept_id, wn_lemma, wn_id, wn_gloss, source)
                    VALUES (%(link_type)s, %(concept_id)s, %(wn_lemma)s, %(wn_id)s, %(wn_gloss)s, %(source)s)""",
                    values,
                )
                conn.commit()
                inserted_lines += 1
            except psycopg2.errors.ForeignKeyViolation as e:
                logging.error(line)
                logging.error(e)
                conn.rollback()
                skipped_lines += 1
            except psycopg2.errors.UniqueViolation as e:
                logging.info(e)
                conn.rollback()
                skipped_lines += 1
logging.info(f"Inserted: {inserted_lines}")
logging.info(f"Skipped: {skipped_lines}")
