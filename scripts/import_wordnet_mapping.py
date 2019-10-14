import json
import logging

import psycopg2


def do_import(cur, mapping, kind):
    for wn31, wn30 in mapping.items():
        pos31 = wn31[0]
        pos30 = wn30[0]
        id31 = wn31[1:].zfill(8)
        id30 = wn30[1:]
        if len(id31) != 8:
            logging.warning("31: %", id31)
        if len(id30) != 8:
            logging.warning("30: %", id30)
        values = {"wn30": f"{id30}-{pos30}", "wn31": f"{id31}-{pos31}", "kind": kind}
        try:
            cur.execute(
                """INSERT INTO wn_mapping (wn30, wn31, kind)
                VALUES (%(wn30)s, %(wn31)s, %(kind)s)""",
                values,
            )
            conn.commit()
        except psycopg2.errors.IntegrityError as e:
            logging.error(e)
            conn.rollback()


filename = "data/mapping_wordnet.json"
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
conn = psycopg2.connect(connection_string)
with open(filename) as file, conn.cursor() as cur:
    data = json.load(file)
    do_import(cur, data[0]["synset-mapping"], "strong")
    do_import(cur, data[0]["synset-weak-mapping"], "weak")
