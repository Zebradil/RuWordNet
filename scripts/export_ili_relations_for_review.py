# /usr/bin/env python3

import argparse
import csv
import logging
import sys

from nltk.corpus import wordnet as wn
from psycopg2 import connect, extras

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(
    description="Generates export of ili relations for review"
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

ARGS = parser.parse_args()

writer = None


def write(row):
    global writer
    if writer is None:
        writer = csv.DictWriter(
            sys.stdout, fieldnames=list(row.keys()), delimiter=",", quotechar='"'
        )
        writer.writeheader()
    writer.writerow(row)


conn = connect(ARGS.connection_string)
with conn.cursor(
    cursor_factory=extras.DictCursor
) as cur, conn.cursor() as insert_cursor:
    select_ili = """
      WITH ili_norm AS (
        SELECT
          concept_id,
          wn_id,
          wn_lemma,
          CASE WHEN source = 'auto derived' THEN source ELSE '' END source
        FROM ili
        WHERE source != 'manual'

        UNION

        SELECT
          concept_id,
          m.wn30,
          wn_lemma,
          ''
        FROM ili
          JOIN wn_mapping m ON m.wn31 = ili.wn_id
        WHERE source = 'manual'
      ),
      ili_stats_wn AS (
        SELECT
          wn_id,
          COUNT(1) cnt
        FROM ili_norm
        WHERE source = ''
        GROUP BY wn_id
      ),
      ili_stats_ruthes AS (
        SELECT
          concept_id,
          CASE WHEN substring(wn_id, '.$') = 's' THEN 'a' ELSE substring(wn_id, '.$') END pos,
          COUNT(1) cnt
        FROM ili_norm
        WHERE source = ''
        GROUP BY concept_id, pos
      )

      SELECT
        c.name,
        c.id concept_id,
        ili.wn_id,
        ili.wn_lemma
      FROM concepts c
        JOIN ili_norm ili ON ili.concept_id = c.id
      WHERE ili.source = 'auto derived'
        OR EXISTS(
          SELECT 1
          FROM ili_stats_wn
          WHERE cnt > 1
            AND wn_id = ili.wn_id
        )
        OR EXISTS(
          SELECT 1
          FROM ili_stats_ruthes
          WHERE cnt > 1
            AND concept_id = ili.concept_id
        )
      ORDER by c.name"""
    cur.execute(select_ili)
    for row in cur:
        row = dict(row)
        parts = row["wn_id"].split("-")
        row["definition"] = wn.synset_from_pos_and_offset(
            parts[1], int(parts[0])
        ).definition()
        row["wn_id"] = "{}-{}".format(parts[0], "a" if parts[1] == "s" else parts[1])
        write(row)
