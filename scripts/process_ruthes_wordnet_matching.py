#!/usr/bin/env python3

import argparse
import logging
import os

import psycopg2
from psycopg2 import extras

parser = argparse.ArgumentParser(
    description="Process files with matches between RuThes concepts and Wordned synsets"
)
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
parser.add_argument("-f", "--file", type=str, help="Source tab-separated file")
parser.add_argument("-o", "--out", type=str, help="Output directory")
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)

ARGS = parser.parse_args()

logging.basicConfig(level=logging.WARN)


matched_file_name = os.path.join(ARGS.out, "match_" + os.path.basename(ARGS.file))
check_file_name = os.path.join(ARGS.out, "check_" + os.path.basename(ARGS.file))

conn = psycopg2.connect(ARGS.connection_string)
with open(ARGS.file) as file, open(matched_file_name, "w") as matched_file, open(
    check_file_name, "w"
) as check_file, conn.cursor(cursor_factory=extras.DictCursor) as cur:
    sql = r"""
    SELECT
        map.wn30 as wn_id,
        c.id
    FROM ili
    JOIN wn_mapping map ON map.wn31 = ili.wn_id
    JOIN concepts c ON c.id = ili.concept_id
    WHERE source='manual'
    """
    cur.execute(sql)
    ili_manual = {row["wn_id"]: {row["id"]: True} for row in cur}

    sql = "SELECT id, name FROM concepts"
    cur.execute(sql)
    concepts = {row["id"]: row["name"] for row in cur}

    inserted_lines = 0
    skipped_lines = 0
    line_number = 0
    for line in file:
        line_number += 1
        line = line.strip()
        if line == "":
            continue
        parts = line.split("\t")
        if len(parts) != 6:
            logging.warning(f'Malformed line {line_number}: "{line}"')
            skipped_lines += 1
            continue
        concept_id = int(parts[0])
        wn_id = parts[4]

        parts.insert(1, concepts[concept_id])
        ili_item = ili_manual.get(wn_id, {}).get(concept_id, False)
        if ili_item:
            print("\t".join(parts), file=matched_file)
        else:
            print("\t".join(parts), file=check_file)
