#!/usr/bin/env python3

import logging
import re
import sys

import psycopg2
from psycopg2 import connect

logging.basicConfig(level=logging.INFO)

connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
conn = connect(connection_string)

re_line = re.compile(r"^([\w-]+)\s*\|\s*\{([\w\s,.-]+)\}$")

with conn.cursor() as cur:
    for i, line in enumerate(sys.stdin):
        line = line.strip()
        if res := re_line.match(line):
            root = res.group(1)
            words = {word.strip() for word in res.group(2).split(",") if word.strip()}
            logging.debug("%s => %s", root, words)
            for word in words:
                try:
                    cur.execute(
                        """INSERT INTO verified_roots (word, root) VALUES (%s, %s)""",
                        (word, root),
                    )
                    conn.commit()
                except psycopg2.errors.IntegrityError as e:
                    logging.error(e)
                    conn.rollback()
        else:
            logging.warning("[%s] Skipped: %s", i, line)
            sys.exit()
