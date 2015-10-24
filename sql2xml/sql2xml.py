import getopt
from operator import itemgetter
import os
import sys

from psycopg2 import connect, extras
from lxml import etree

PKG_ROOT = os.path.split(__file__)[0]
OUT_ROOT = os.path.join(PKG_ROOT, 'out')

conn = None

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}


def main(argv):
    global OUT_ROOT, conn

    help_str = 'Usage: {0} [-h] [--out-dir=<output_directory>]'.format(os.path.split(__file__)[1])
    try:
        opts, args = getopt.getopt(argv, "h", ["out-dir="])
    except getopt.GetoptError:
        print(help_str)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_str)
            sys.exit()
        elif opt == '--out-dir':
            OUT_ROOT = arg

    try:
        conn = connect(**dbconfig)
    except:
        print('I am unable to connect to the database')
        exit(1)

    print('Start')

    for entity in ['concepts', 'relations', 'text_entry', 'synonyms']:
        generate_xml_file(entity)

    print('Done')


def create_concepts_tree(rows):
    root = etree.Element("concepts")

    for row in sorted(rows, key=itemgetter('name')):
        doc = etree.SubElement(root, "concept")
        doc.set("id", str(row['id']))
        etree.SubElement(doc, "name").text = row['name']
        etree.SubElement(doc, "gloss").text = row['gloss']
        etree.SubElement(doc, "domain").text = row['domain']
    return root


def create_relations_tree(rows):
    root = etree.Element("relations")

    for row in rows:
        doc = etree.SubElement(root, "rel")
        doc.set("from", str(row['from_id']))
        doc.set("to", str(row['to_id']))
        doc.set("name", row['name'])
        doc.set("asp", row['asp'])
    return root


def create_synonyms_tree(rows):
    root = etree.Element("synonyms")

    for row in rows:
        doc = etree.SubElement(root, "entry_rel")
        doc.set("concept_id", str(row['concept_id']))
        doc.set("entry_id", str(row['entry_id']))
    return root


def create_text_entry_tree(rows):
    root = etree.Element("entries")

    for row in rows:
        doc = etree.SubElement(root, "entry")
        doc.set("id", str(row['id']))
        etree.SubElement(doc, "name").text = row['name']
        etree.SubElement(doc, "lemma").text = row['lemma']
        etree.SubElement(doc, "main_word").text = row['main_word']
        etree.SubElement(doc, "synt_type").text = row['synt_type']
        etree.SubElement(doc, "pos_string").text = row['pos_string']
    return root


def generate_xml_file(entity):
    if entity not in ['concepts', 'relations', 'text_entry', 'synonyms']:
        return

    filename = os.path.join(OUT_ROOT, entity + '.xml')

    if os.path.isfile(filename):
        os.remove(filename)

    print('Output file: ' + filename)

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:

        cur.execute("SELECT * FROM " + entity)
        rows = cur.fetchall()
        if entity == 'concepts':
            root = create_concepts_tree(rows)
        elif entity == 'relations':
            root = create_relations_tree(rows)
        elif entity == 'text_entry':
            root = create_text_entry_tree(rows)
        elif entity == 'synonyms':
            root = create_synonyms_tree(rows)

    tree = etree.ElementTree(root)
    tree.write(filename, encoding="utf-8", pretty_print=True)


if __name__ == "__main__":
    main(sys.argv[1:])
