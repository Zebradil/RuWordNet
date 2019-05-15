#!/usr/bin/env python3

import argparse
import os

from lxml import etree
from psycopg2 import connect, extras

PKG_ROOT = os.path.dirname(os.path.abspath(__file__))

parser = argparse.ArgumentParser(description="Generate RuThes xml files")
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument(
    "-o",
    "--output-directory",
    help="A directory where xml-files will be saved",
    default=os.path.join(PKG_ROOT, "out", "ruthes"),
)

ARGS = parser.parse_args()


def xstr(value):
    return "" if value is None else str(value)


with connect(ARGS.connection_string).cursor(
    cursor_factory=extras.RealDictCursor
) as cur:
    print("Generating text_entry.xml")
    sql = """
        SELECT
            id, name, lemma, main_word, synt_type, pos_string
        FROM text_entry
        ORDER BY id"""
    cur.execute(sql)

    root = etree.Element("entries")

    print(".. generating xml")
    for row in cur:
        entry = etree.SubElement(root, "entry")
        entry.set("id", str(row["id"]))
        etree.SubElement(entry, "name").text = xstr(row["name"])
        etree.SubElement(entry, "lemma").text = xstr(row["lemma"])
        etree.SubElement(entry, "main_word").text = xstr(row["main_word"])
        etree.SubElement(entry, "synt_type").text = xstr(row["synt_type"])
        etree.SubElement(entry, "pos_string").text = xstr(row["pos_string"])

    tree = etree.ElementTree(root)
    filename = os.path.join(ARGS.output_directory, "text_entry.xml")
    if os.path.isfile(filename):
        os.remove(filename)
    print("Output file: " + filename)
    tree.write(filename, encoding="utf-8", pretty_print=True)
