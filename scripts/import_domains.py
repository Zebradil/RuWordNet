#!/usr/bin/python3

from psycopg2 import connect

revry_filename = 'in/revry_filtered.txt'
rubrics_filename = 'in/rubrics_filtered.txt'

dbconfig = {
    'database': 'ruwordnet',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}

rubrics = {}
with open(rubrics_filename) as f:
    for line in f:
        parts = line.strip().split(' ', 1)
        if len(parts) != 2:
            continue
        rubrics.update(dict([parts]))

revry = {}
with open(revry_filename) as f:
    for line in f:
        parts = line.strip().split(' ', 1)
        if len(parts) != 2:
            continue
        revry.update(dict([reversed(parts)]))

conn = connect(**dbconfig)
with conn.cursor() as cur:
    prepare = """
       PREPARE insert_relation AS
         INSERT INTO relations (from_id, to_id, name)
           VALUES ($1, $2, 'ДОМЕН')"""
    cur.execute(prepare)

    for concept_id, domain_id in revry.items():
        if domain_id in rubrics:
            values = {
                'from_id': concept_id,
                'to_id': rubrics[domain_id],
            }
            cur.execute('EXECUTE insert_relation (%(from_id)s, %(to_id)s)', values)
        else:
            print('Not found: ', domain_id)
    conn.commit()
