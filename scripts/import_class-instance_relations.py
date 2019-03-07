#!/usr/bin/env python3
import argparse
import os
import re

from psycopg2 import connect

parser = argparse.ArgumentParser(description="Import antonymy relations to RuThes database.")
parser.add_argument("-s", "--source-file", type=str, help="Source csv file")
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)

ARGS = parser.parse_args()

filename = ARGS.source_file

relation_name = "ЭКЗЕМПЛЯР"
reversible = True
reverse_relation_name = "КЛАСС"

if not os.path.isfile(filename):
    print("File not exists")
    exit()

conn = connect(ARGS.connection_string)
with open(filename) as file, conn.cursor() as cur:
    prepare = (
        "PREPARE insert_relations AS "
        "INSERT INTO relations (from_id, to_id, name) VALUES ($1, $2, '" + relation_name + "')"
    )
    if reversible:
        prepare += ", ($2, $1, '" + reverse_relation_name + "')"
    cur.execute(prepare)

    pattern = re.compile("^(\d+)[^\d]+(\d+)[^\d]+$")

    for line in file:
        line = line.strip()
        match_obj = pattern.match(line)
        if match_obj is None:
            print("NOT MATCH: ", line)
            continue
        values = {"from_id": match_obj.group(1), "to_id": match_obj.group(2)}
        try:
            cur.execute("EXECUTE insert_relations (%(from_id)s, %(to_id)s)", values)
            print("Inserted: ", line)
        except:
            print("Failed: ", line)

    conn.commit()

print("Done")
