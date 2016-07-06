#!/usr/bin/python3
# coding=utf-8

import os
import re

from psycopg2 import connect

switch = 2

if switch == 0:
    filename = 'in/add_part.txt'
    asp = 'add_part'
    relation_name = 'ЦЕЛОЕ'
elif switch == 1:
    filename = 'in/process_steps_final.txt'
    asp = 'process_steps'
    relation_name = 'ЧАСТЬ'
elif switch == 2:
    filename = 'in/classical_meronymy_edited.txt'
    asp = 'classical_meronymy'
    relation_name = 'ЧАСТЬ'
else:
    exit(1)

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
    prepare = "PREPARE update_relation AS " \
              "UPDATE relations SET asp = $4 " \
              "WHERE from_id = $1 " \
              "  AND to_id = $2 " \
              "  AND name = $3"
    cur.execute(prepare)

    prepare = "PREPARE insert_relation AS " \
              "INSERT INTO relations (from_id, to_id, name, asp)" \
              "VALUES ($1, $2, $3, $4)"
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
            'name': relation_name,
            'asp': asp
        }

        try:
            cur.execute('EXECUTE insert_relation (%(from_id)s, %(to_id)s, %(name)s, %(asp)s)', values)
            print('Insert', line)
        except:
            conn.commit()
            cur.execute('EXECUTE update_relation (%(from_id)s, %(to_id)s, %(name)s, %(asp)s)', values)
            print('Update', line)
        finally:
            conn.commit()

print('Done')
