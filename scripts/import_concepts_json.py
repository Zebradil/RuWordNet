#!/usr/bin/env python3

import argparse
import json
import logging
import sys

from psycopg2 import IntegrityError, connect

parser = argparse.ArgumentParser(description="Import RuThes data from JSON file.")
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


def extract_entities(data):
    synonyms = []
    text_entries = []

    concept = extract_concept(data)

    for synonym in data["synonyms"]:
        text_entry = extract_text_entry(synonym)
        text_entries.append(text_entry)
        synonyms.append(
            {"concept_id": concept["id"], "entry_id": text_entry["id"],}
        )

    relations = [
        extract_relation(relation, concept["id"]) for relation in data["relats"]
    ]

    return concept, text_entries, synonyms, relations


def extract_concept(data):
    return {
        "id": int(data["conceptid"].strip()),
        "name": data["conceptstr"].strip(),
        "gloss": data["shortcomments"].strip(),
        "en_name": data["conceptengstr"].strip(),
        "is_abstract": bool(int(data["isabstract"].strip())),
        "is_arguable": bool(int(data["isarguable"].strip())),
        "domainmask": int(data["domainmask"].strip()),
    }


def extract_text_entry(data):
    return {
        "id": int(data["textentryid"].strip()),
        "name": data["textentrystr"].strip(),
        "lemma": data["lementrystr"].strip(),
        "is_ambig": bool(int(data["isambig"].strip())),
        "is_arguable": bool(int(data["isarguable"].strip())),
    }


def extract_relation(data, from_id):
    return {
        "from_id": from_id,
        "to_id": int(data["conceptid"].strip()),
        "name": data["relationstr"].strip(),
        "asp": data["aspect"].strip(),
        "is_arguable": bool(int(data["isarguable"].strip())),
    }


def generate_insert(table, keys):
    return "INSERT INTO {} ({}) VALUES ({})".format(
        table, ", ".join(keys), ", ".join(f"%({key})s" for key in keys),
    )


def insert_entity(cur, table, values):
    try:
        cur.execute(generate_insert(table, list(values.keys())), values)
        conn.commit()
    except IntegrityError as e:
        logging.error(e)
        conn.rollback()


conn = connect(ARGS.connection_string)
with conn.cursor() as cur:
    n = 0
    number_of_concepts = 0
    number_of_text_entries = 0
    number_of_synonyms = 0
    number_of_relations = 0
    for line in sys.stdin:
        n += 1

        data = json.loads(line)

        concept, text_entries, synonyms, relations = extract_entities(data)

        number_of_concepts += 1
        number_of_text_entries += len(text_entries)
        number_of_synonyms += len(synonyms)
        number_of_relations += len(relations)

        if ARGS.dry_run:
            logging.info("DRY-RUN: insert concept %s", concept)
            logging.info("DRY-RUN: insert text_entries %s", text_entries)
            logging.info("DRY-RUN: insert synonyms %s", synonyms)
            logging.info("DRY-RUN: insert relations %s", relations)
        else:
            insert_entity(cur, "v2_concepts", concept)
            _ = [insert_entity(cur, "v2_text_entry", ent) for ent in text_entries]
            _ = [insert_entity(cur, "v2_synonyms", ent) for ent in synonyms]
            _ = [insert_entity(cur, "v2_relations", ent) for ent in relations]
    logging.info("Number of concepts: %s", number_of_concepts)
    logging.info("Number of text entries: %s", number_of_text_entries)
    logging.info("Number of synonyms: %s", number_of_synonyms)
    logging.info("Number of relations: %s", number_of_relations)
