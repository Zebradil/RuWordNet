#!/usr/bin/env python3

# -- Mark deleted relations
# UPDATE sense_relations sr
#   SET info = 'deleted'
#   WHERE name = 'composed_of'
#     AND NOT EXISTS(
#       SELECT 1
#       FROM sense_relations_revised
#       WHERE parent_id = sr.parent_id
#         AND child_id = sr.child_id
#     );

import argparse
import re

from psycopg2 import IntegrityError, connect, extras

parser = argparse.ArgumentParser(description="Import collocations report")
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
    "-t",
    "--test",
    help="Only show found relations, don't insert new relations in database",
    action="store_true",
)
parser.add_argument("file", type=argparse.FileType("r"))

ARGS = parser.parse_args()

conn = connect(ARGS.connection_string)
conn.autocommit = True


def main():
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
        word_re = re.compile(r"^Слово (.*)$")
        synset_re = re.compile(r"^Синсет \[(.*)\]$")
        no_synset_re = re.compile(r"^Без синсета$")
        collocation_re = re.compile(r"^(.*) \[(.*)\]$")

        sql = r"""
        PREPARE get_sense_id AS
          SELECT
            se.id
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
          WHERE se.name = $1
            AND sy.name = $2
        """
        cur.execute(sql)

        sql = r"""
        PREPARE add_composed_of_relation AS
        INSERT INTO sense_relations (parent_id, child_id, name, info) VALUES ($1, $2, 'composed_of', 'manual')
        """
        cur.execute(sql)

        def get_sense_id(sense_name, synset_name, n=None):
            cur.execute(
                "EXECUTE get_sense_id(%(sense)s, %(synset)s)",
                {"sense": sense_name, "synset": synset_name},
            )
            try:
                return cur.fetchone()["id"]
            except:
                print(f"Not found: [{n}] {sense_name} [{synset_name}]")
                raise

        sense_name = None
        synset_name = None
        sense_id = None

        n = 0
        for line in ARGS.file:
            n += 1
            line = line.strip()
            if line == "":
                continue

            if res := word_re.match(line):
                sense_name = res.group(1)
                synset_name = None
                sense_id = None
                continue

            if res := synset_re.match(line):
                synset_name = res.group(1)
                sense_id = get_sense_id(sense_name, synset_name, n)
                continue

            if res := no_synset_re.match(line):
                synset_name = None
                sense_id = None
                continue

            if res := collocation_re.match(line):
                if synset_name is None:
                    continue
                else:
                    collocation_id = get_sense_id(res.group(1), res.group(2), n)
                    try:
                        if not ARGS.test:
                            cur.execute(
                                "EXECUTE add_composed_of_relation(%(parent_id)s, %(child_id)s)",
                                {"parent_id": collocation_id, "child_id": sense_id},
                            )
                        print(f"New: [{n}] {sense_name} [{synset_name}] : {line}")
                    except IntegrityError:
                        pass
                    continue

            print(f"Unhandled: [{n}] {line}")


if __name__ == "__main__":
    main()
