#!/usr/bin/python3
# coding=utf-8

import getopt
import os
import sys
from subprocess import Popen, PIPE
from xml.etree import ElementTree
from psycopg2 import connect


PKG_ROOT = os.path.split(__file__)[0]
SQL_ROOT = os.path.join(PKG_ROOT, 'sql')
XML_ROOT = os.path.join(PKG_ROOT, 'xml')

conn = None

dbconfig = {
    'database': 'ruthes',
    'user': 'ruthes',
    'password': 'ruthes',
    'host': '127.0.0.1'
}


def main(argv):
    global XML_ROOT, conn

    help_str = 'Usage: {0} [-h] [--xml-root=<xml_root_directory>]'.format(os.path.split(__file__)[1])
    try:
        opts, args = getopt.getopt(argv, "h", ["xml-root="])
    except getopt.GetoptError:
        print(help_str)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_str)
            sys.exit()
        elif opt == '--xml-root':
            XML_ROOT = arg

    try:
        conn = connect(**dbconfig)
    except:
        print('I am unable to connect to the database')
        exit(1)

    init_db()
    import_data()
    create_indexes()

    print('Done')


def run_sql_file(filename):
    print('Run sql from ' + filename)
    pg_env = os.environ.copy()
    pg_env.update({'PGPASSWORD': dbconfig['password']})
    cmd_str = 'psql -U {user} -d {database} -h {host} -f {filename}'.format(filename=filename, **dbconfig)
    process = Popen(cmd_str, stdout=PIPE, stdin=PIPE, shell=True, env=pg_env)
    stdout, stderr = process.communicate()
    if stderr is None:
        print(stdout.decode())
    else:
        print(stderr.decode())
    return stderr is None


def init_db():
    print('Preparing database')
    filename = os.path.join(PKG_ROOT, 'sql', 'script_prepare_db.sql')
    run_sql_file(filename)


def import_data():
    print('Importing data from XML files to DB')

    insert_data(
        filename='concepts.xml',
        table='concepts',
        fields=['id', 'name', 'gloss', 'domain'],
        get_values=lambda item: {
            'id': item.get('id'),
            'name': item.find('name').text,
            'gloss': item.find('gloss').text,
            'domain': item.find('domain').text
        },
        get_items=lambda tree: tree.findall('concept')
    )

    insert_data(
        filename='relations.xml',
        table='relations',
        fields=['from_id', 'to_id', 'name', 'asp'],
        get_values=lambda item: {
            'from_id': item.get('from'),
            'to_id': item.get('to'),
            'name': item.get('name'),
            'asp': item.get('asp')
        },
        get_items=lambda tree: tree.findall('rel')
    )

    insert_data(
        filename='text_entry.xml',
        table='text_entry',
        fields=['id', 'name', 'lemma', 'main_word', 'synt_type', 'pos_string'],
        get_values=lambda item: {
            'id': item.get('id'),
            'name': item.find('name').text,
            'lemma': item.find('lemma').text,
            'main_word': item.find('main_word').text,
            'synt_type': item.find('synt_type').text,
            'pos_string': item.find('pos_string').text
        },
        get_items=lambda tree: tree.findall('entry')
    )

    insert_data(
        filename='synonyms.xml',
        table='synonyms',
        fields=['concept_id', 'entry_id'],
        get_values=lambda item: {
            'concept_id': item.get('concept_id'),
            'entry_id': item.get('entry_id')
        },
        get_items=lambda tree: tree.findall('entry_rel')
    )


def insert_data(filename, table, fields, get_values, get_items):
    print('Start processing ' + filename)

    logname = os.path.join(PKG_ROOT, 'log', filename + '.log')
    file = open(logname, 'w')

    fields_str = ', '.join(str(v) for v in fields)
    dollars = ', '.join('$' + str(i + 1) for i in range(len(fields)))
    placeholders = ', '.join('%({0})s'.format(f) for f in fields)

    tree = ElementTree.parse(os.path.join(XML_ROOT, filename))
    items = get_items(tree)
    count = len(items)
    print('Found {0} items'.format(count))

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(placeholders=placeholders, table=table)
    i = 0

    with conn.cursor() as cur:
        sql = 'PREPARE prepared_query_{table} AS '.format(table=table) + \
              'INSERT INTO {tbl} ({fields}) VALUES ({dollars})'\
                  .format(fields=fields_str, dollars=dollars, tbl=table)

        cur.execute(sql)

        file.write(sql + '\n\n')

        for item in items:
            values = {k: val.strip() if isinstance(val, str) else val for k, val in get_values(item).items()}
            cur.execute(sql_str, values)
            i += 1
            print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)
            file.write(str(values) + '\n')
        print()
    conn.commit()
    file.close()


def create_indexes():
    print('Creating indexes')
    filename = os.path.join(PKG_ROOT, 'sql', 'script_create_constraints.sql')
    run_sql_file(filename)


if __name__ == "__main__":
    main(sys.argv[1:])