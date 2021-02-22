#!/usr/bin/env python3

import logging
import re
import sys

from psycopg2 import connect

connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)

logging.basicConfig(level=logging.INFO)

conn = connect(connection_string)
with conn.cursor() as cur:
    re_sense_lemma = re.compile(r"^([\w\s-]+)\s+\d+\s+\d+\s+\d+")

    for line in sys.stdin:
        line = line.strip()
        match = re_sense_lemma.match(line)
        lemma = match.group(1).strip()

        # Check if the text entry exists in the database
        cur.execute("SELECT 1 FROM text_entry WHERE lemma = %s LIMIT 1", (lemma,))
        if cur.fetchone() is None:
            print(f"#{line}")
            continue

        # Check verified ILI relation for the text entry
        cur.execute(
            """
            SELECT 1
            FROM text_entry t
            JOIN synonyms s ON s.entry_id = t.id
            JOIN ili ON ili.concept_id = s.concept_id
            WHERE t.lemma=%s
              AND ili.approved
            LIMIT 1
            """,
            (lemma,),
        )
        if cur.fetchone():
            print(f"+{line}")
            continue

        # Check not verified ILI relation for the text entry
        cur.execute(
            """
            SELECT 1
            FROM text_entry t
            JOIN synonyms s ON s.entry_id = t.id
            JOIN ili ON ili.concept_id = s.concept_id
            WHERE t.lemma=%s
              AND NOT ili.approved
            LIMIT 1
            """,
            (lemma,),
        )
        if cur.fetchone():
            print(f"?{line}")
            continue

        print(f"?{line}")
