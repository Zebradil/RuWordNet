#!/usr/bin/env python3

import re

from psycopg2 import connect

conn = None
cur = None

dbconfig = {"database": "ruthes", "user": "ruwordnet", "password": "ruwordnet", "host": "127.0.0.1"}


def main():
    global conn, cur
    conn = connect(**dbconfig)
    cur = conn.cursor()
    sql = """
        PREPARE insert_entry AS
        INSERT INTO text_entry_new (id, name, lemma, synt_type, main_word, pos_string)
        VALUES ($1, $2, $3, $4, $5, $6)"""
    cur.execute(sql)

    filename = "/Users/german/tmp/ruthes/textentr_pos_edited.txt"

    word_chars = '[А-Яа-я\d\w\-",\(\)\./]'
    pp = "(\d+)\s+((?:\s{0,2}" + word_chars + ")+)\s+(?:10|20)\s+((?:\s{0,2}" + word_chars + ")+)"
    pattern0 = re.compile(pp)
    pp += "\s+([A-Za-z]+)"
    pattern1 = re.compile(pp)
    pp += "\s+([А-Яа-я\-]+)\s+([A-Za-z ]+)"
    pattern2 = re.compile(pp)

    keys = ("id", "name", "lemma", "synt_type", "main_word", "pos")

    file = open(filename, "r", -1, "Cp1251")

    i = 0

    for line in file:
        print(line)
        i += 1
        match_obj = pattern2.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups())))
            continue

        match_obj = pattern1.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups() + ("", ""))))
            continue

        match_obj = pattern0.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups() + ("", "", ""))))
            continue
        print("DOES NOT MATCH: " + line)
    print(str(i) + " rows inserted")


def insert_data(elements):
    print(elements)
    sql_str = "EXECUTE insert_entry (%(id)s, %(name)s, %(lemma)s, %(synt_type)s, %(main_word)s, %(pos)s)"
    values = {k: re.sub(r"\s+", " ", val.strip()) if isinstance(val, str) else val for k, val in elements.items()}
    cur.execute(sql_str, values)
    conn.commit()


main()
