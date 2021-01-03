#!/usr/bin/env python3

import argparse
import csv
import logging
import sys

import pymorphy2
from psycopg2 import connect, extras

parser = argparse.ArgumentParser(
    description="Extracts morphological info from text_entries"
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

logging.basicConfig(level=logging.INFO)


def get_pos(word) -> str:
    pos = morph.parse(word)[0].tag.POS
    if pos == "NOUN" or pos is None:
        return "N"
    if pos in {"ADJF", "PRTF", "PRTS"}:
        return "Adj"
    if pos == "INFN":
        return "V"
    logging.info("%s: %s", word, pos)
    return ""


conn = connect(ARGS.connection_string)
with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
    cur.execute(
        """
            select c2.name concept_name, t.name entry_name, t.lemma
            from (
                select t2.*
                from v2_text_entry t2
                left join text_entry t on t.id = t2.id or t.name = t2.name or t.lemma = t2.lemma
                where t.id is null
            ) t
            join v2_synonyms s on s.entry_id = t.id
            join v2_concepts c2 on c2.id = s.concept_id
            join concepts c on c.id = c2.id
            where c2.id > 0 and not is_multiword(t.name)
            order by 1, 2
        """
    )
    morph = pymorphy2.MorphAnalyzer()
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=["concept_name", "entry_name", "lemma", "POS"],
        delimiter=",",
        quotechar='"',
    )
    writer.writeheader()
    for row in cur:
        row["POS"] = get_pos(row["lemma"])
        writer.writerow(row)
