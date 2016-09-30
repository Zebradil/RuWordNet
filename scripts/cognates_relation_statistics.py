#!/usr/bin/python3
# coding=utf-8

from psycopg2 import connect, extras

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}

conn = connect(**dbconfig)


def prepare_search_cognates(cursor):
    sql = """
        SELECT s.name, s.rel_name
        FROM (
               SELECT
                 name,
                 'synset' rel_name
               FROM senses
               WHERE id != $1
                     AND synset_id = $2
               UNION
               SELECT
                 se.name,
                 sr.name
               FROM senses se
                 INNER JOIN synset_relations sr
                   ON sr.child_id = se.synset_id
               WHERE sr.parent_id = $2
               UNION
               SELECT
                 t2.name,
                 r.name
               FROM text_entry t1
                 INNER JOIN synonyms s1 ON s1.entry_id = t1.id
                 INNER JOIN relations r ON r.from_id = s1.concept_id
                 INNER JOIN synonyms s2 ON s2.concept_id = r.to_id
                 INNER JOIN text_entry t2 ON t2.id = s2.entry_id
               WHERE t1.name = $3
             ) s
        WHERE substr(s.name, 1, 4) = substr($3, 1, 4)
          AND array_length(regexp_split_to_array(s.name, '\s+'), 1) = 1"""

    cursor.execute('PREPARE search_cognates AS ' + sql)


def main():
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            conn.cursor(cursor_factory=extras.RealDictCursor) as cur2:

        print('prepare_search_cognates', flush=True)
        prepare_search_cognates(cur2)

        print('search collocations', flush=True)
        sql = """
          SELECT
            id,
            name,
            synset_id
          FROM senses
          WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) = 1"""
        cur.execute(sql)

        print('start looping', flush=True)
        for row in cur:
            print(flush=True)
            print(row['name'], flush=True)

            params = {
                'sense_id': row['id'],
                'synset_id': row['synset_id'],
                'word': row['name']
            }
            cur2.execute('EXECUTE search_cognates(%(sense_id)s, %(synset_id)s, %(word)s)', params)
            for cognate in cur2.fetchall():
                print(cognate['rel_name'] + ': ' + cognate['name'])

    print('Done')


if __name__ == "__main__":
    main()
