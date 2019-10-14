import csv
import logging
import sys

from psycopg2 import connect, extras

logging.basicConfig(level=logging.INFO)

connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)


def get(arr, idx):
    try:
        return arr[idx]
    except:
        return None


writer = None


def write(row):
    global writer
    if writer is None:
        writer = csv.DictWriter(
            sys.stdout, fieldnames=list(row.keys()), delimiter=",", quotechar='"'
        )
        writer.writeheader()
    writer.writerow(row)


conn = connect(connection_string)
with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
    cur.execute("SELECT id, name FROM concepts")
    concepts = {row["id"]: row["name"] for row in cur}

    sql = r"""
    SELECT
        wn_id,
        wn_lemma,
        link_type,
        ARRAY_AGG(concept_id ORDER BY concept_id) concepts
    FROM ili
    WHERE source='auto'
    GROUP BY wn_id, wn_lemma, link_type
    ORDER BY wn_id
    """
    cur.execute(sql)
    ili_auto = {row["wn_id"]: row for row in cur}

    sql = r"""
    SELECT
        map.wn30 as wn_id,
        wn_lemma,
        link_type,
        ARRAY_AGG(concept_id ORDER BY concept_id) concepts
    FROM ili
    JOIN wn_mapping map ON map.wn31 = ili.wn_id
    WHERE source='manual'
    GROUP BY map.wn30, wn_lemma, link_type
    ORDER BY map.wn30
    """
    cur.execute(sql)
    ili_manual = {row["wn_id"]: row for row in cur}

    for wn_id in list(ili_auto.keys()) + list(ili_manual.keys()):
        ili_auto_item = get(ili_auto, wn_id)
        ili_manual_item = get(ili_manual, wn_id)
        row = {
            "same": "yes" if ili_auto_item == ili_manual_item else "",
            "wn_id": wn_id,
            "auto_lemma": get(ili_auto_item, "wn_lemma"),
            "manual_lemma": get(ili_manual_item, "wn_lemma"),
        }
        concepts_auto = ili_auto_item["concepts"] if ili_auto_item else []
        concepts_manual = ili_manual_item["concepts"] if ili_manual_item else []
        ia = 0
        im = 0
        while True:
            ca = get(concepts_auto, ia)
            cm = get(concepts_manual, im)
            if ca is not None and cm is not None:
                if ca > cm:
                    ca = None
                elif ca < cm:
                    cm = None
            elif ca is None and cm is None:
                break
            row["auto_concept_id"] = ca
            row["auto_concept_name"] = get(concepts, ca)
            row["manual_concept_id"] = cm
            row["manual_concept_name"] = get(concepts, cm)
            write(row)
            ia += 0 if ca is None else 1
            im += 0 if cm is None else 1
        write(
            {
                "same": "---",
                "wn_id": "---",
                "auto_lemma": "---",
                "manual_lemma": "---",
                "auto_concept_id": "---",
                "auto_concept_name": "---",
                "manual_concept_id": "---",
                "manual_concept_name": "---",
            }
        )
