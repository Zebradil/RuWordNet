import argparse
import logging
import os
import sys

from nltk.corpus import wordnet as wn
from psycopg2 import connect, errors, extras

logging.basicConfig(level="DEBUG")

logging.info("Wordnet version: %s", wn.get_version())

parser = argparse.ArgumentParser(
    description="Search possibly missed matches between RuThes concepts and Wordnet synsets"
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


def get_derived_synsets(wn_id, poses):
    def repos(pos):
        return "a" if pos == "s" else pos

    parts = wn_id.split("-")
    offset = int(parts[0])
    pos = parts[1]
    synset = wn.synset_from_pos_and_offset(pos, offset)
    for lemma in synset.lemmas():
        for derived_lemma in lemma.derivationally_related_forms():
            derived_synset = derived_lemma.synset()
            if repos(derived_synset.pos()) in poses:
                yield f"{derived_synset.offset():0>8}-{derived_synset.pos()}", derived_lemma.name()


conn = connect(ARGS.connection_string)
with conn.cursor(
    cursor_factory=extras.DictCursor
) as cur, conn.cursor() as insert_cursor:
    select_ili = """
      SELECT
        concept_id,
        c.name,
        array_agg(wn_id) wn_ids,
        array_agg(
          DISTINCT
            CASE
              WHEN s.part_of_speech = 'Adj'
              THEN 'a'
              ELSE LOWER(s.part_of_speech)
            END
        ) poses
      FROM (
        SELECT concept_id, wn_id
        FROM ili
        WHERE source != 'manual'
        UNION
        SELECT concept_id, m.wn30
        FROM ili
          JOIN wn_mapping m ON m.wn31 = ili.wn_id
        WHERE source = 'manual'
      ) t
        JOIN concepts c ON c.id = t.concept_id
        JOIN synsets s ON s.name = c.name
      GROUP BY t.concept_id, c.name"""
    cur.execute(select_ili)
    for row in cur:
        derived_synsets = set()
        wn_poses = {wn_id[-1] for wn_id in row["wn_ids"]}
        target_poses = set(row["poses"]) - wn_poses
        for wn_id in row["wn_ids"]:
            for derived_wn_id in get_derived_synsets(wn_id, target_poses):
                derived_synsets.add(derived_wn_id)
        potential_matches = set()
        for wn_id in derived_synsets:
            if wn_id[0] not in row["wn_ids"]:
                potential_matches.add(wn_id)
        if potential_matches:
            print(f"{row['name']} ({row['concept_id']}): {potential_matches}")
        for match in potential_matches:
            values = {
                "concept_id": row["concept_id"],
                "wn_lemma": match[1],
                "wn_id": match[0],
                "source": "auto derived",
            }
            try:
                insert_cursor.execute(
                    """INSERT INTO ili (concept_id, wn_lemma, wn_id, source)
                    VALUES (%(concept_id)s, %(wn_lemma)s, %(wn_id)s, %(source)s)""",
                    values,
                )
                conn.commit()
            except errors.UniqueViolation as e:
                logging.debug(values)
                logging.info(e)
                conn.rollback()
