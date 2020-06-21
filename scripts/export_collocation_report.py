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

        # Search senses which participate in "composed_of" relations with a given sense/word
        sql = r"""
        PREPARE search_related_collocations AS
          SELECT
            se.name,
            sy.name synset_name
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
            INNER JOIN sense_relations sr
              ON sr.parent_id = se.id
                AND sr.name = 'composed_of'
                AND sr.child_id = $1
          ORDER BY se.name
        """
        cur.execute(sql)

        # Search senses which contain a given word and do not participate in "composed_of" relations with the given word
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
        PREPARE get_synset_name AS
          SELECT
            sy.name
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
          WHERE se.id = $1
        """
        cur.execute(sql)

        sql = r"""
          SELECT name, array_agg(id::text) ids
          FROM senses
          WHERE NOT is_multiword(name)
          GROUP BY name
          ORDER BY name"""
        cur.execute(sql)

        for row in cur:
            related_collocations = {}
            for sid in row["ids"]:
                cur2.execute(
                    "EXECUTE search_related_collocations(%(id)s)", {"id": sid},
                )
                tmp = cur2.fetchall()
                if tmp:
                    related_collocations[sid] = tmp

            cur2.execute(
                "EXECUTE search_unrelated_collocations(%(word)s)",
                {"word": row["name"]},
            )
            unrelated_collocation = cur2.fetchall()

            if related_collocations or unrelated_collocation:
                print("\nСлово {}".format(row["name"]), flush=True)
            else:
                print("\nСлово {} *".format(row["name"]), flush=True)

            for sid in row["ids"]:
                cur2.execute("EXECUTE get_synset_name(%(sense_id)s)", {"sense_id": sid})
                print("\n  Синсет [{}]".format(cur2.fetchone()["name"]))
                for row2 in related_collocations.get(sid, []):
                    print("    {} [{}]".format(row2["name"], row2["synset_name"]))

            if unrelated_collocation:
                print("\n  Без синсета")
                for row2 in unrelated_collocation:
                    print("    {} [{}]".format(row2["name"], row2["synset_name"]))


if __name__ == "__main__":
    main()
