#!/usr/bin/env python3

import argparse
import logging
import re

from psycopg2 import IntegrityError, connect

parser = argparse.ArgumentParser(description="")
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

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)

conn = connect(ARGS.connection_string)
with open(ARGS.file) as file, conn.cursor() as cur:
    miss = 0
    match = 0
    multi = 0
    n = 0

    re_concept_id = re.compile(r"\+?\d+")
    re_wn_id = re.compile(r"\d{8}-[n]")

    for line in file:
        is_verified = False
        n += 1

        line = line.strip()

        if line == "":
            continue

        # Example line:
        # +139	АЛМАЗ	500514	diamond	diamond	14834563-n	very hard native crystalline carbon valued as a gem
        # ↑ plus sign is optional, part divider is tab sign
        parts = line.split("\t")

        concept_id = parts[0]
        if re_concept_id.fullmatch(concept_id) is None:
            raise ValueError(f"Malformed concept_id on line {n}")

        if concept_id[0] == "+":
            is_verified = True
            concept_id = concept_id[1:]

        wn_id = parts[5]
        if re_wn_id.fullmatch(wn_id) is None:
            raise ValueError(f"Malformed wn_id on line {n}")

        cur.execute(
            "SELECT COUNT(*) cnt FROM ili WHERE concept_id=%s AND wn_id=%s",
            (concept_id, wn_id,),
        )
        cnt = cur.fetchone()[0]
        line = f"{cnt} {concept_id} {wn_id}"
        if cnt == 0:
            miss += 1
            logging.info("%s MISS    %s", n, line)
        elif cnt == 1:
            match += 1
            logging.info("%s MATCH   %s", n, line)
        else:
            multi += 1
            logging.info("%s MULTI/%s %s", n, cnt, line)

logging.info("Misses: %s", miss)
logging.info("Matches: %s", match)
logging.info("Multi matches: %s", match)
