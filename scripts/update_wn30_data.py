#!/usr/bin/env python3
import json
import sys

from nltk.corpus import wordnet as wn
from psycopg2 import IntegrityError, connect

dbconfig = {
    "database": "ruwordnet",
    "user": "ruwordnet",
    "password": "ruwordnet",
    "host": "127.0.0.1",
}

conn = connect(**dbconfig)
with conn.cursor() as cur:
    prepare = """
       PREPARE insert_wn AS
         INSERT INTO wn_data (id, name, definition, lemma_names, version)
           VALUES ($1, $2, $3, $4, 30)"""
    cur.execute(prepare)

    total = 0
    new = 0
    for synset in wn.all_synsets():
        total += 1
        name = synset.name()
        try:
            cur.execute(
                "EXECUTE insert_wn (%(id)s, %(name)s, %(definition)s, %(lemma_names)s)",
                {
                    "definition": synset.definition(),
                    "id": wn.ss2of(synset),
                    "lemma_names": json.dumps(synset.lemma_names()),
                    "name": name,
                },
            )
            print(f"New {name}", file=sys.stderr)
            new += 1
            conn.commit()
        except IntegrityError:
            print(f"Skip {name}", file=sys.stderr)
            conn.rollback()
    print(f"New:     {new}")
    print(f"Skipped: {total-new}")
    print(f"Total:   {total}")
