#!/usr/bin/env python3

import argparse
import csv
import os

from psycopg2 import connect

parser = argparse.ArgumentParser(description='Import antonymy relations to RuThes database.')
parser.add_argument(
    '-s',
    '--source-file',
    type=str,
    help='Source csv file'
)
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    '-c',
    '--connection-string',
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string
)

ARGS = parser.parse_args()

filename = ARGS.source_file

relation_name = 'АНТОНИМ'
reversible = True
reverse_relation_name = relation_name

replace = False
relation_name_to_replace = None

if not os.path.isfile(filename):
    print('File not exists')
    exit()

conn = connect(ARGS.connection_string)
with open(filename, encoding='cp1251') as csvfile, conn.cursor() as cur:
    prepare = "PREPARE insert_relations AS " \
              "INSERT INTO relations (from_id, to_id, name) VALUES ($1, $2, '" + relation_name + "')"
    if reversible:
        prepare += ", ($2, $1, '" + reverse_relation_name + "')"
    cur.execute(prepare)
    reader = csv.reader(csvfile)
    for row in reader:
        if row[0] == 'id':
            continue
        values = {
            'from_id': row[0],
            'to_id': row[2],
        }
        print('Insert', row)
        cur.execute('EXECUTE insert_relations (%(from_id)s, %(to_id)s)', values)
    conn.commit()

print('Done')
