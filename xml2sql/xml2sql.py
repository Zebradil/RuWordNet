#!/usr/bin/env python3
import argparse
import os
from xml.etree import ElementTree

import psycopg2

PKG_ROOT = os.path.split(__file__)[0]

parser = argparse.ArgumentParser(description="Run import RuThes from xml to database.")
parser.add_argument(
    "-s",
    "--xml-dir",
    type=str,
    help="Source xml root directory",
    default=os.path.join(PKG_ROOT, "xml"),
)
parser.add_argument(
    "-l",
    "--log-dir",
    type=str,
    help="Log files destination",
    default=os.path.join(PKG_ROOT, "log"),
)
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
# parser.add_argument(
#     '-v',
#     dest='verbose',
#     help='verbose output',
#     action='store_true'
# )

ARGS = parser.parse_args()


def import_data():
    print("Importing data from XML files to DB")

    insert_data(
        filename="concepts.xml",
        table="concepts",
        fields=["id", "name", "gloss", "domain"],
        get_values=lambda item: {
            "id": item.get("id"),
            "name": item.find("name").text,
            "gloss": item.find("gloss").text,
            "domain": item.find("domain").text,
        },
        get_items=lambda tree: tree.findall("concept"),
    )

    insert_data(
        filename="relations.xml",
        table="relations",
        fields=["from_id", "to_id", "name", "asp"],
        get_values=lambda item: {
            "from_id": item.get("from"),
            "to_id": item.get("to"),
            "name": item.get("name"),
            "asp": item.get("asp"),
        },
        get_items=lambda tree: tree.findall("rel"),
    )

    insert_data(
        filename="text_entry.xml",
        table="text_entry",
        fields=["id", "name", "lemma", "main_word", "synt_type", "pos_string"],
        get_values=lambda item: {
            "id": item.get("id"),
            "name": item.find("name").text,
            "lemma": item.find("lemma").text,
            "main_word": item.find("main_word").text,
            "synt_type": item.find("synt_type").text,
            "pos_string": item.find("pos_string").text,
        },
        get_items=lambda tree: tree.findall("entry"),
    )

    insert_data(
        filename="synonyms.xml",
        table="synonyms",
        fields=["concept_id", "entry_id"],
        get_values=lambda item: {
            "concept_id": item.get("concept_id"),
            "entry_id": item.get("entry_id"),
        },
        get_items=lambda tree: tree.findall("entry_rel"),
    )


def insert_data(filename, table, fields, get_values, get_items):
    print("Start processing " + filename)

    logname = os.path.join(ARGS.log_dir, filename + ".log")
    file = open(logname, "w", encoding="utf-8")

    fields_str = ", ".join(str(v) for v in fields)
    dollars = ", ".join("$" + str(i + 1) for i in range(len(fields)))
    placeholders = ", ".join("%({0})s".format(f) for f in fields)

    tree = ElementTree.parse(os.path.join(ARGS.xml_dir, filename))
    items = get_items(tree)
    count = len(items)
    print("Found {0} items".format(count))

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(
        placeholders=placeholders, table=table
    )
    i = 0

    with CONN.cursor() as cur:
        sql = "PREPARE prepared_query_{table} AS ".format(
            table=table
        ) + "INSERT INTO {tbl} ({fields}) VALUES ({dollars})".format(
            fields=fields_str, dollars=dollars, tbl=table
        )

        cur.execute(sql)

        file.write(sql + "\n\n")

        for item in items:
            values = {
                k: val.strip() if isinstance(val, str) else val
                for k, val in get_values(item).items()
            }
            cur.execute(sql_str, values)
            i += 1
            print(
                "\rProgress: {0}% ({1})".format(round(i / count * 100), i),
                end="",
                flush=True,
            )
            file.write(str(values) + "\n")
        print()
    CONN.commit()
    file.close()


def check_xml_files():
    source_filenames = [
        "concepts.xml",
        "relations.xml",
        "text_entry.xml",
        "synonyms.xml",
    ]
    for filename in source_filenames:
        with open(os.path.join(ARGS.xml_dir, filename)) as f:
            pass


try:
    CONN = psycopg2.connect(ARGS.connection_string)
except psycopg2.Error as err:
    print("I'm not able to connect to the database")
    print(err)
    exit(1)

try:
    check_xml_files()
except IOError as err:
    print("Bad source files ({})".format(err))
    exit(1)

import_data()

print("Done")
