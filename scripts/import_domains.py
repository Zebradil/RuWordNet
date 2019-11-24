#!/usr/bin/env python3

from psycopg2 import connect

revry_filename = "data/revry_filtered.txt"
rubrics_filename = "data/rubrics_filtered.txt"

dbconfig = {
    "database": "ruwordnet",
    "user": "ruwordnet",
    "password": "ruwordnet",
    "host": "127.0.0.1",
}

rubrics = {}
with open(rubrics_filename) as f:
    for line in f:
        parts = line.strip().split(" ", 1)
        if len(parts) != 2:
            continue
        rubrics.update(dict([parts]))

revry = {}
with open(revry_filename) as f:
    for line in f:
        parts = line.strip().split(" ", 1)
        if len(parts) != 2:
            continue
        if parts[1] not in revry:
            revry[parts[1]] = []
        revry[parts[1]].append(parts[0])

conn = connect(**dbconfig)
with conn.cursor() as cur:
    prepare = """
       PREPARE insert_relation AS
         INSERT INTO relations (from_id, to_id, name)
           VALUES ($1, $2, 'ДОМЕН')"""
    cur.execute(prepare)

    for concept_id, domains in revry.items():
        for domain_id in domains:
            if domain_id in rubrics:
                values = {"from_id": concept_id, "to_id": rubrics[domain_id]}
                cur.execute("EXECUTE insert_relation (%(from_id)s, %(to_id)s)", values)
            else:
                print("Not found: ", domain_id)
    conn.commit()
