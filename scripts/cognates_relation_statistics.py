#!/usr/bin/env python3
import argparse

from lxml import etree
from psycopg2 import connect, extras

parser = argparse.ArgumentParser(description='Extract derivation relations from RuThes and RuWordNet.')
parser.add_argument(
    '-o',
    '--output-file',
    type=str,
    help='In that file xml will be written',
    default=None
)
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    '-c',
    '--connection-string',
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string
)
parser.add_argument(
    '-t',
    '--test',
    help="Only show found relations, don't generate xml file",
    action='store_true'
)

ARGS = parser.parse_args()

conn = connect(ARGS.connection_string)


def prepare_search_cognates(cursor):
    sql = """
        SELECT s.name, s.rel_name
        FROM (
               SELECT
                 name,
                 'synset' rel_name,
                 'RuWordNet' source
               FROM senses
               WHERE id != $1
                     AND synset_id = $2
               UNION
               SELECT
                 se.name,
                 sr.name,
                 'RuWordNet' source
               FROM senses se
                 INNER JOIN synset_relations sr
                   ON sr.child_id = se.synset_id
               WHERE sr.parent_id = $2
               UNION
               SELECT
                 t2.name,
                 r.name,
                 'RuThes' source
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


def prepare_search_cognates_transitionally(cursor):
    sql = """
        WITH RECURSIVE tree (id, name, id_path, name_path, parent_relation_name) AS (
          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            $2 parent_relation_name
          FROM concepts
          WHERE id IN(
            SELECT concept_id
            FROM synonyms s
              INNER JOIN text_entry t
                ON t.id = s.entry_id
            WHERE t.name = $1
          )
          UNION ALL
          SELECT
            c.id,
            c.name,
            array_append(tree.id_path, c.id),
            array_append(tree.name_path, c.name),
            r.name parent_relation_name
          FROM tree
            INNER JOIN relations r
              ON r.from_id = tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
          WHERE r.name = ANY($3) AND tree.parent_relation_name = $2
        )

        SELECT t.name, tree.name_path, tree.parent_relation_name
        FROM tree
          INNER JOIN synonyms s
            ON s.concept_id = tree.id
          INNER JOIN text_entry t
            ON t.id = s.entry_id
        WHERE t.name != $1
          AND array_length(id_path, 1) > 1
          AND substr(t.name, 1, 4) = substr($1, 1, 4)
          AND array_length(regexp_split_to_array(t.name, '\s+'), 1) = 1"""
    cursor.execute('PREPARE search_cognates_transitionally AS ' + sql)


def prepare_search_sense(cursor):
    sql = """
        SELECT
          id,
          name,
          synset_id
        FROM senses
        WHERE name = $1
        ORDER BY meaning
        LIMIT 1"""

    cursor.execute('PREPARE search_sense AS ' + sql)


def main():
    test = ARGS.test
    if not test:
        # Testing if file is writeable
        open(ARGS.output_file, 'w').close()
        root = etree.Element('senses')
    else:
        root = None

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            conn.cursor(cursor_factory=extras.RealDictCursor) as cur2:

        print('prepare_search_cognates', flush=True)
        prepare_search_cognates(cur2)
        print('prepare_search_cognates_transitionally', flush=True)
        prepare_search_cognates_transitionally(cur2)

        if not test:
            print('prepare_search_sense', flush=True)
            prepare_search_sense(cur2)

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

            if not test:
                lexemes = []

            params = {
                'sense_id': row['id'],
                'synset_id': row['synset_id'],
                'word': row['name']
            }
            cur2.execute('EXECUTE search_cognates(%(sense_id)s, %(synset_id)s, %(word)s)', params)
            for cognate in cur2.fetchall():
                print(cognate['name'] + ': ' + cognate['rel_name'])
                if not test:
                    lexemes.append(cognate['name'])

            for name in ('ВЫШЕ', 'НИЖЕ', 'ЧАСТЬ', 'ЦЕЛОЕ'):
                if name == 'ВЫШЕ':
                    tail_names = ['АСЦ', 'ЧАСТЬ']
                elif name == 'ЧАСТЬ':
                    tail_names = ['АСЦ']
                else:
                    tail_names = ['']

                params = {
                    'word': row['name'],
                    'name': name,
                    'tail_names': [name] + tail_names
                }
                cur2.execute(
                    """EXECUTE search_cognates_transitionally(
                        %(word)s, %(name)s, %(tail_names)s)""", params)
                for senses_chain in cur2.fetchall():
                    chain = senses_chain['name'] + ':' + \
                            ' (' + name + ') ' + \
                            ' → '.join(senses_chain['name_path']) + \
                            ' (' + senses_chain['parent_relation_name'] + ')'
                    print(chain)
                    if not test:
                        lexemes.append(senses_chain['name'])

            if not test and lexemes:
                x_sense = etree.SubElement(root, 'sense')
                x_sense.set('name', row['name'])
                x_sense.set('id', row['id'])
                x_sense.set('synset_id', row['synset_id'])
                x_rel = etree.SubElement(x_sense, 'derived_from')
                for lexeme in set(lexemes):
                    cur2.execute('EXECUTE search_sense(%(name)s)', {'name': lexeme})
                    row_lexeme = cur2.fetchone()
                    if row_lexeme:
                        x_lexeme = etree.SubElement(x_rel, 'sense')
                        x_lexeme.set('name', row_lexeme['name'])
                        x_lexeme.set('id', row_lexeme['id'])
                        x_lexeme.set('synset_id', row_lexeme['synset_id'])

        tree = etree.ElementTree(root)
        tree.write(ARGS.output_file, encoding="utf-8", pretty_print=True)

    print('Done')


if __name__ == "__main__":
    main()
