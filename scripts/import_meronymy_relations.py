#!/usr/bin/env python3

import argparse
import os
import re

from psycopg2 import connect

# filename = 'in/add_part.txt'
# asp = 'add_part'
# relation_name = 'ЦЕЛОЕ'
#
# filename = 'in/process_steps_final.txt'
# asp = 'process_steps'
# relation_name = 'ЧАСТЬ'
#
# filename = 'in/classical_meronymy_edited.txt'
# asp = 'classical_meronymy'
# relation_name = 'ЧАСТЬ'

parser = argparse.ArgumentParser(description="Import meronymy relations to RuThes database.")
parser.add_argument("-s", "--source-file", type=str, help="Source csv file")
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument("--type", type=str, help="Relation type", choices=["ЦЕЛОЕ", "ЧАСТЬ"])
parser.add_argument(
    "--sub-type", type=str, help="Relation sub-type", choices=["add_part", "process_steps", "classical_meronymy"]
)

ARGS = parser.parse_args()

filename = ARGS.source_file
asp = ARGS.sub_type
relation_name = ARGS.type

if not os.path.isfile(filename):
    print("File not exists")
    exit()

conn = connect(ARGS.connection_string)
with open(filename) as file, conn.cursor() as cur:
    prepare = (
        "PREPARE update_relation AS "
        "UPDATE relations SET asp = $4 "
        "WHERE from_id = $1 "
        "  AND to_id = $2 "
        "  AND name = $3"
    )
    cur.execute(prepare)

    prepare = "PREPARE insert_relation AS " "INSERT INTO relations (from_id, to_id, name, asp)" "VALUES ($1, $2, $3, $4)"
    cur.execute(prepare)

    pattern = re.compile("^(\d+)[^\d]+(\d+)[^\d]+$")

    for line in file:
        line = line.strip()
        match_obj = pattern.match(line)
        if match_obj is None:
            print("NOT MATCH: ", line)
            continue
        values = {"from_id": match_obj.group(1), "to_id": match_obj.group(2), "name": relation_name, "asp": asp}

        try:
            cur.execute("EXECUTE insert_relation (%(from_id)s, %(to_id)s, %(name)s, %(asp)s)", values)
            print("Insert", line)
        except:
            conn.commit()
            cur.execute("EXECUTE update_relation (%(from_id)s, %(to_id)s, %(name)s, %(asp)s)", values)
            print("Update", line)
        finally:
            conn.commit()

print("Done")
