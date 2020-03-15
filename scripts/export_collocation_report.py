#!/usr/bin/env python3

import argparse

from psycopg2 import connect, extras

parser = argparse.ArgumentParser(description="Export collocations report")
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

ARGS = parser.parse_args()

conn = connect(ARGS.connection_string)
conn.autocommit = True


def main():
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, conn.cursor(
        cursor_factory=extras.RealDictCursor
    ) as cur2:

        sql = r"""
        PREPARE search_related_collocations AS
          SELECT
            se.name,
            sy.name synset_name,
            sy2.name synset_name2
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
            INNER JOIN sense_relations sr
              ON sr.parent_id = se.id
                AND sr.name = 'composed_of'
            INNER JOIN senses se2
              ON se2.id = sr.child_id
            INNER JOIN synsets sy2
              ON sy2.id = se2.synset_id
          WHERE se2.name = $1
          ORDER BY se2.name
        """
        cur.execute(sql)

        sql = r"""
        PREPARE search_unrelated_collocations AS
          SELECT
            se.name,
            sy.name synset_name
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
            LEFT JOIN sense_relations sr
              ON sr.parent_id = se.id
                AND sr.name = 'composed_of'
                AND sr.child_id IN(
                  SELECT id
                  FROM senses
                  WHERE name = $1
                )
          WHERE is_multiword(se.name)
            AND regexp_split_to_array(se.name, '\s+') @> ARRAY[$1::text]
            AND sr.name IS NULL
          ORDER BY se.name
        """
        cur.execute(sql)

        sql = r"""
          SELECT DISTINCT name
          FROM senses
          WHERE NOT is_multiword(name)
          ORDER BY name"""
        cur.execute(sql)

        for row in cur:
            print("\nСлово {}".format(row["name"]), flush=True)

            cur2.execute(
                "EXECUTE search_related_collocations(%(word)s)", {"word": row["name"]},
            )
            rows = cur2.fetchall()
            prev_synset_name = None
            for row2 in rows:
                if prev_synset_name is None or prev_synset_name != row2["synset_name2"]:
                    prev_synset_name = row2["synset_name2"]
                    print("\n  Синсет [{}]".format(row2["synset_name2"]))
                print("    {} [{}]".format(row2["name"], row2["synset_name"]))

            cur2.execute(
                "EXECUTE search_unrelated_collocations(%(word)s)",
                {"word": row["name"]},
            )
            rows = cur2.fetchall()
            if rows:
                print("\n  Без синсета")
                for row2 in rows:
                    print("    {} [{}]".format(row2["name"], row2["synset_name"]))


if __name__ == "__main__":
    main()
