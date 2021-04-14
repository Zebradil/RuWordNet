#!/usr/bin/env python3

import logging
import re
import sys
from collections import defaultdict

from psycopg2 import connect

logging.basicConfig(level=logging.INFO)

connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
conn = connect(connection_string)

good_fp = open("good_grouped_roots.txt", "w")
bad_fp = open("bad_grouped_roots.txt", "w")

re_line = re.compile(r"^([\w-]+)\s*\|\s*\{([\w\s,.-]+)\}$")

words_to_roots = defaultdict(set)
roots_to_words = defaultdict(set)
good_roots_to_words = defaultdict(set)
bad_words_data = {}
for i, line in enumerate(sys.stdin):
    line = line.strip()
    if res := re_line.match(line):
        root = res.group(1)
        words = {word.strip() for word in res.group(2).split(",") if word.strip()}
        logging.debug("%s => %s", root, words)
        for word in words:
            words_to_roots[word].add(root)
        roots_to_words[root].update(words)
    else:
        logging.warning("[%s] Skipped: %s", i, line)
        sys.exit()

cur = conn.cursor()
for word, roots in sorted(words_to_roots.items(), key=lambda x: x[0]):
    if len(roots) > 1:
        cur.execute(
            """
            SELECT array_agg(c.name) concepts
            FROM text_entry t
            JOIN synonyms s ON s.entry_id = t.id
            JOIN concepts c ON c.id = s.concept_id
            WHERE t.name = UPPER(%s)
            GROUP BY t.id""",
            (word,),
        )
        concepts = cur.fetchone()
        if concepts is None:
            logging.warning("No concepts for %s", word)
            continue
        elif len(concepts[0]) > 1:
            # bad_words_data[word] = (concepts, [roots_to_words[root] for root in roots])
            print(word, file=bad_fp)
            print(f"  {concepts[0]}", file=bad_fp)
            for root in roots:
                print(f"  {roots_to_words[root]}", file=bad_fp)
            print(file=bad_fp)
            continue
    for root in roots:
        good_roots_to_words[root].add(word)

for root, words in sorted(good_roots_to_words.items(), key=lambda x: x[0]):
    print(root, file=good_fp)
    [print(f"  {word}", file=good_fp) for word in sorted(words)]
    print(file=good_fp)
