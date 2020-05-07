#!/usr/bin/env python3

import argparse
import os
import re

from psycopg2 import connect, extras

parser = argparse.ArgumentParser(
    description="Import cause or entailment relations to RuWordNet database."
)
parser.add_argument("-s", "--source-file", type=str, help="Source csv file")
parser.add_argument(
    "--name", type=str, help="Relation name", choices=["cause", "entailment"]
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

filename = ARGS.source_file

if not os.path.isfile(filename):
    print("File not exists")
    exit()

conn = connect(ARGS.connection_string)
with open(filename) as file, conn.cursor(
    cursor_factory=extras.DictCursor
) as dict_cur, conn.cursor(cursor_factory=extras.DictCursor) as cur:
    prepare = (
        "PREPARE insert_relations AS "
        "INSERT INTO synset_relations (parent_id, child_id, name) VALUES ($1, $2, '"
        + ARGS.name
        + "')"
    )
    dict_cur.execute(prepare)

    find_synset_sql = """
SELECT
  s1.syid1 a,
  s1.syid2 b
FROM (
       SELECT
         _.synset_id syid1,
         sy2.id      syid2
       FROM (
              -- выбираем подходящие синсеты — они должны содержать ровно столько членов, сколько мы ищем
              SELECT
                synset_id,
                count(1) cnt,
                (SELECT count(*) FROM senses WHERE synset_id = s.synset_id) cnt2
              FROM senses s
              WHERE name = ANY (%s)
              GROUP BY synset_id
            ) _
         -- присоединяем кучу всего только для того, чтобы выяснить,
         -- с какими ещё синсетами есть направленные отношения ассоциации
         JOIN synsets sy ON sy.id = _.synset_id
         JOIN concepts c ON c.name = sy.name
         JOIN relations r ON r.from_id = c.id AND r.name IN ('АСЦ1', 'АСЦ2')
         JOIN concepts c2 ON c2.id = r.to_id
         JOIN synsets sy2 ON sy2.name = c2.name
       WHERE cnt = cnt2 AND cnt = %s
     ) s1
  -- теперь делаем то же самое для второго синсета
  JOIN (
         SELECT
           _.synset_id syid1,
           sy2.id      syid2
         FROM (
                -- выбираем подходящие синсеты — они должны содержать ровно столько членов, сколько мы ищем
                SELECT
                  synset_id,
                  count(1) cnt,
                  (SELECT count(*) FROM senses WHERE synset_id = s.synset_id) cnt2
                FROM senses s
                WHERE name = ANY (%s)
                GROUP BY synset_id
              ) _
           -- присоединяем кучу всего только для того, чтобы выяснить,
           -- с какими ещё синсетами есть направленные отношения ассоциации
           JOIN synsets sy ON sy.id = _.synset_id
           JOIN concepts c ON c.name = sy.name
           JOIN relations r ON r.from_id = c.id AND r.name IN ('АСЦ1', 'АСЦ2')
           JOIN concepts c2 ON c2.id = r.to_id
           JOIN synsets sy2 ON sy2.name = c2.name
         WHERE cnt = cnt2 AND cnt = %s
       ) s2
    ON s2.syid1 = s1.syid2
       AND s2.syid2 = s1.syid1"""

    re_synset = re.compile(r"^.:\s+(.*)$")

    for line_a in file:
        line_b = file.readline()

        senses_match = re_synset.search(line_a)
        if senses_match is None:
            print("Re error", line_a)
            continue

        senses_a = list(sense.strip() for sense in senses_match.group(1).split(";"))

        senses_match = re_synset.search(line_b)
        if senses_match is None:
            print("Re error", line_b)
            continue

        senses_b = list(sense.strip() for sense in senses_match.group(1).split(";"))

        dict_cur.execute(
            find_synset_sql, (senses_a, len(senses_a), senses_b, len(senses_b))
        )

        rows = dict_cur.fetchall()
        if not rows:
            print(f"Not found: {senses_a}, {senses_b}")
        elif len(rows) > 1:
            print(f"Multiple matches: {rows}, {senses_a}, {senses_b}")
        for row in rows:
            values = {"parent_id": row["a"], "child_id": row["b"]}
            try:
                cur.execute(
                    "EXECUTE insert_relations (%(parent_id)s, %(child_id)s)", values
                )
                # print("Insert", row)
            except:
                pass
                # print("Exists", row)
        conn.commit()

print("Done")
