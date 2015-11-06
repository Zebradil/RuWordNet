#!/usr/bin/python3
# coding=utf-8

import csv
import os

from psycopg2 import connect

filename = 'antonyms_final.txt'
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
with open(filename, encoding='cp1251') as csvfile, \
        conn.cursor() as cur:
    cur.execute("PREPARE insert_antonyms AS "
                "INSERT INTO relations (from_id, to_id, name) VALUES ($1, $2, 'АНТ'), ($2, $1, 'АНТ')")
    reader = csv.reader(csvfile)
    for row in reader:
        values = {
            'from_id': row[0],
            'to_id': row[2],
        }
        print('Insert', row)
        cur.execute('EXECUTE insert_antonyms (%(from_id)s, %(to_id)s)', values)
    conn.commit()

print('Done')
