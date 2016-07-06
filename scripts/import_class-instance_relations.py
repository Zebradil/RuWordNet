#!/usr/bin/python3
# coding=utf-8

import os
import re

from psycopg2 import connect

filename = 'in/class-instance_edited.txt'
relation_name = 'ЭКЗЕМПЛЯР'
reversible = True
reverse_relation_name = 'КЛАСС'

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
with open(filename, encoding='cp1251') as file, conn.cursor() as cur:
    prepare = "PREPARE insert_relations AS " \
              "INSERT INTO relations (from_id, to_id, name) VALUES ($1, $2, '" + relation_name + "')"
    if reversible:
        prepare += ", ($2, $1, '" + reverse_relation_name + "')"
    cur.execute(prepare)

    pattern = re.compile('^(\d+)[^\d]+(\d+)[^\d]+$')

    for line in file:
        match_obj = pattern.match(line)
        if match_obj is None:
            print('NOT MATCH: ', line)
            continue
        values = {
            'from_id': match_obj.group(1),
            'to_id': match_obj.group(2),
        }
        try:
            cur.execute('EXECUTE insert_relations (%(from_id)s, %(to_id)s)', values)
            print('Inserted: ', line)
        except:
            print('Failed: ', line)

    conn.commit()

print('Done')
