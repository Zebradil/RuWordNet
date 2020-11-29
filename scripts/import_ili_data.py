#!/usr/bin/env python3

import argparse
import logging

from psycopg2 import IntegrityError, connect

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
parser.add_argument(
    "--format",
    default="auto",
    const="auto",
    nargs="?",
    choices=("verified", "auto"),
    help="Specify format of input data (default: %(default)s)",
)

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)


def parse_auto(line):
    parts = line.split("\t")
    return {
        "link_type": parts[0],
        "concept_id": parts[1],
        "wn_lemma": parts[4],
        "wn_id": parts[6],
        "wn_gloss": parts[7],
        "source": "auto",
        "approved": True,
    }


def parse_verified(line):
    parts = line.split("\t")
    concept_id = parts[0]
    if concept_id[0] == "+":
        concept_id = concept_id[1:]
        return {
            "link_type": None,
            "concept_id": concept_id,
            "wn_lemma": parts[3],
            "wn_id": parts[5],
            "wn_gloss": parts[6],
            "source": "auto",
            "approved": True,
        }
    else:
        return None


conn = connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    inserted_lines = 0
    skipped_lines = 0
    n = 0
    for line in file:
        n += 1

        line = line.strip()
        if line == "":
            continue

        if ARGS.format == "auto":
            values = parse_auto(line.strip())
        elif ARGS.format == "verified":
            values = parse_verified(line.strip())
        else:
            raise ValueError(f"Invalid format option: {ARGS.format}")

        if values is None:
            logging.info("Skipping line %s", n)
            continue

        if ARGS.dry_run:
            logging.info("DRY-RUN: insert values %s", values)
        else:
            try:
                cur.execute(
                    """INSERT INTO ili (link_type, concept_id, wn_lemma, wn_id, wn_gloss, source, approved)
                    VALUES (%(link_type)s, %(concept_id)s, %(wn_lemma)s, %(wn_id)s, %(wn_gloss)s, %(source)s, %(approved)s)""",
                    values,
                )
                conn.commit()
                inserted_lines += 1
            except IntegrityError as e:
                logging.error(e)
                conn.rollback()
                skipped_lines += 1
logging.info("Inserted: %s", inserted_lines)
logging.info("Skipped: %s", skipped_lines)
