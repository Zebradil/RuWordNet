#!/usr/bin/env python3

import argparse
import os
import re
from typing import List

from psycopg2 import connect, extras


class Matcher:
    def __init__(self, connection, source_file):
        self.connection = connection
        self.source_file = source_file
        self.re_synset = re.compile(r"^.:\s+(.*)$")

    def run(self):
        with open(self.source_file) as file, self.connection.cursor(
            cursor_factory=extras.DictCursor
        ) as dict_cur:

            for line_a in file:
                senses_a = self.extract_senses(line_a.strip())
                senses_b = self.extract_senses(file.readline().strip())

                result_a = self.search_matches(dict_cur, senses_a)
                result_b = self.search_matches(dict_cur, senses_b)

                if result_a or result_b:
                    print("A: {}".format("; ".join(senses_a)))
                    if result_a:
                        print(result_a, end="")
                    print("B: {}".format("; ".join(senses_b)))
                    if result_b:
                        print(result_b, end="")
                    print()

    def extract_senses(self, line: str) -> List[str]:
        line = line.strip()
        senses_match = self.re_synset.search(line)
        if senses_match is None:
            raise ValueError(f"Can't parse value: {line}")

        return sorted(sense.strip() for sense in senses_match.group(1).split(";"))

    @classmethod
    def search_matches(cls, cur, senses: List[str]) -> str:
        result = ""
        exact_matches = cls.search_exact_matches(cur, senses)
        if not exact_matches:
            loose_matches = cls.search_loose_matches(cur, senses)
            if loose_matches:
                result += "    Точных совпадений не найдено, неточные совпадения:\n"
                for match in loose_matches:
                    result += "    ({}) {}: {} | {}\n".format(
                        len(set(senses).intersection(set(match["senses"]))),
                        match["part_of_speech"],
                        match["name"],
                        "; ".join(match["senses"]),
                    )

            else:
                result += "    Совпадений не найдено\n"
        elif len(exact_matches) > 1:
            result += "    Несколько точных совпадений:\n"
            for match in exact_matches:
                result += "    {}: {} | {}\n".format(
                    match["part_of_speech"], match["name"], "; ".join(match["senses"]),
                )
        return result

    @staticmethod
    def search_exact_matches(cur, senses: List[str]):
        cur.execute(
            """
            WITH extended_synsets(name, part_of_speech, senses) AS (
                SELECT
                    sy.name,
                    sy.part_of_speech,
                    array_agg(se.name ORDER BY se.name) senses
                FROM synsets sy
                JOIN senses se ON se.synset_id = sy.id
                GROUP BY sy.name, sy.part_of_speech
            )
            SELECT *
            FROM extended_synsets
            WHERE senses @> ARRAY[%s] AND senses <@ ARRAY[%s]
            """,
            (senses, senses),
        )
        return cur.fetchall()

    @staticmethod
    def search_loose_matches(cur, senses: List[str]):
        cur.execute(
            """
            WITH extended_synsets(name, part_of_speech, senses) AS (
                SELECT
                    sy.name,
                    sy.part_of_speech,
                    array_agg(se.name ORDER BY se.name) senses
                FROM synsets sy
                JOIN senses se ON se.synset_id = sy.id
                GROUP BY sy.name, sy.part_of_speech
            )
            SELECT *
            FROM extended_synsets
            WHERE senses && ARRAY[%s]
            """,
            (senses,),
        )
        return sorted(
            cur,
            key=lambda x: len(set(x["senses"]).intersection(set(senses))),
            reverse=True,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Search matches for synsets from cause-entailment relations files."
    )
    parser.add_argument("-s", "--source-file", type=str, help="Source csv file")
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

    filename = ARGS.source_file

    if not os.path.isfile(filename):
        print("File not exists")
        exit()

    conn = connect(ARGS.connection_string)
    Matcher(conn, filename).run()
    print("Done")


if __name__ == "__main__":
    main()
