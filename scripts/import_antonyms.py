#!/usr/bin/python3
# coding=utf-8

import csv
import os

from psycopg2 import connect

filename = 'in/antonyms_final.txt'
relation_name = 'АНТОНИМ'
reversible = True
reverse_relation_name = relation_name

replace = False
relation_name_to_replace = None

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}

if not os.path.isfile(filename):
    print('File not exists')
    exit()

conn = connect(**dbconfig)
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
