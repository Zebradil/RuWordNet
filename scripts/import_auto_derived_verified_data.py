#!/usr/bin/env python3

import csv
import logging
import os
import sys

import psycopg2

TEST = os.getenv("TEST", "1") == "1"

connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)

logging.basicConfig(level=logging.WARN)


def read_stdin():
    for line in sys.stdin:
        line = line.strip()
        if line == "":
            continue
        if line.startswith('-"'):
            line = '"-' + line[2:]
        yield line


conn = psycopg2.connect(connection_string)
with conn.cursor() as cur:
    line_number = 0
    for parts in csv.reader(read_stdin()):
        line_number += 1
        if not parts:
            continue

        values = {
            "approved": not parts[0].startswith("-"),
            "concept_id": parts[1],
            "wn_id": parts[2],
        }

        try:
            if TEST:
                cur.execute(
                    """
                    SELECT count(1)
                    FROM ili
                    WHERE concept_id = %(concept_id)s
                      AND (
                        array[%(wn_id)s] <@ wn_id_variants(wn_id)
                        OR source = 'manual'
                        AND EXISTS(
                          SELECT 1
                          FROM wn_mapping
                          WHERE array[%(wn_id)s] <@ wn_id_variants(wn30)
                            AND wn_id = wn31
                        )
                      )
                    """,
                    values,
                )
                row_count = cur.fetchone()[0]
            else:
                cur.execute(
                    """
                    UPDATE ili
                    SET approved = %(approved)s
                    WHERE concept_id = %(concept_id)s
                      AND (
                        array[%(wn_id)s] <@ wn_id_variants(wn_id)
                        OR source = 'manual'
                        AND EXISTS(
                          SELECT 1
                          FROM wn_mapping
                          WHERE array[%(wn_id)s] <@ wn_id_variants(wn30)
                            AND wn_id = wn31
                        )
                      )
                    """,
                    values,
                )
                row_count = cur.rowcount
                conn.commit()
            if row_count != 1:
                print(f"{line_number}: {row_count}")
        except:
            print(f"{line_number}")
            raise
