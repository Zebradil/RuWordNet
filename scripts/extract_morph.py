#!/usr/bin/env python3

import argparse
import csv
import logging
import sys
from collections import namedtuple

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

MorphData = namedtuple("MorphData", "pos_string main_word synt_type")


def get_pos(word) -> str:
    pos = morph.parse(word)[0].tag.POS  # type: ignore
    if pos == "NOUN" or pos is None:
        return "N"
    if pos in {"ADJF", "PRTF", "PRTS"}:
        return "Adj"
    if pos == "INFN":
        return "V"
    logging.info("%s: %s", word, pos)
    return ""


def get_poses(word: str) -> str:
    return " ".join(get_pos(part) for part in word.split(" "))


def get_morph_data(word) -> MorphData:
    parts = word.split(" ")
    poses = [get_pos(part) for part in parts]
    if len(parts) == 1:
        return MorphData(poses[0], word, poses[0])
    if "V" in poses:
        return MorphData(" ".join(poses), parts[poses.index("V")], "VG")
    if "N" in poses:
        return MorphData(" ".join(poses), get_main_noun(parts, poses), "NG")
    if "Adj" in poses:
        return MorphData(" ".join(poses), parts[poses.index("Adj")], "AdjG")


def get_main_noun(parts, poses) -> str:
    for n, pos in enumerate(poses):
        if pos == "N" and len(parts[n]) > 1:
            return parts[n]
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
            where c2.id > 0 and is_multiword(t.name)
            order by 1, 2
        """
    )
    morph = pymorphy2.MorphAnalyzer()
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=[
            "concept_name",
            "entry_name",
            "lemma",
            "pos_string",
            "main_word",
            "synt_type",
        ],
        delimiter=",",
        quotechar='"',
    )
    writer.writeheader()
    for row in cur:
        morph_data = get_morph_data(row["lemma"])
        if morph_data is not None:
            row["pos_string"] = morph_data.pos_string
            row["main_word"] = morph_data.main_word
            row["synt_type"] = morph_data.synt_type
        writer.writerow(row)
