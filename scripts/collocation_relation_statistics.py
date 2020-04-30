#!/usr/bin/env python3

import argparse
import sys
from collections import defaultdict
from typing import List, Optional, Tuple

from psycopg2 import IntegrityError, connect, extras
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Extract collocation composition information from RuThes and RuWordNet."
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
parser.add_argument(
    "-t",
    "--test",
    help="Only show found relations, don't generate xml file",
    action="store_true",
)
parser.add_argument(
    "--without-matches",
    help="Print collocations without matched components to stdout",
    action="store_true",
)

ARGS = parser.parse_args()

blacklist = [
    "И",
    "ДА",
    "ЖЕ",
    "ТО",
    "ИЛИ",
    "КАК",
    "РАЗ",
    "ТАК",
    "ЧТО",
    "ЛИШЬ",
    "БУДТО",
    "ПОСЛЕ",
    "ТОЧНО",
    "ЧТОБЫ",
    "СЛОВНО",
    "Д",
    "Ж",
    "И",
    "О",
    "С",
    "Ф",
    "Х",
    "В",
    "И",
    "К",
    "О",
    "С",
    "У",
    "Х",
    "А-ЛЯ",
    "ВО",
    "ДО",
    "ЗА",
    "ИЗ",
    "КО",
    "НА",
    "ОБ",
    "ОТ",
    "ПО",
    "СО",
    "ИЗ-ПОД",
    "БЕЗ",
    "ВНЕ",
    "ДЛЯ",
    "ИЗО",
    "НАД",
    "ОТО",
    "ПОД",
    "ПРИ",
    "ПРО",
    "ЧТО",
    "ВЫШЕ",
    "МИМО",
    "РАДИ",
    "СЕБЯ",
    "ВНИЗУ",
    "МЕЖДУ",
    "ПЕРЕД",
    "ПОСЛЕ",
    "САМЫЙ",
    "СВЕРХ",
    "СЗАДИ",
    "СНИЗУ",
    "СРЕДИ",
    "ЧЕРЕЗ",
    "ВМЕСТО",
    "ВНУТРИ",
    "ВНУТРЬ",
    "ВОКРУГ",
    "ВПЕРЕД",
    "НАСЧЕТ",
    "ПОЗАДИ",
    "ПРОТИВ",
    "СВЕРХУ",
    "СКВОЗЬ",
    "СЛОВНО",
    "ВПЕРЕДИ",
    "ИЗНУТРИ",
    "СДЕЛАТЬ",
    "СПЕРЕДИ",
    "СТОРОНА",
    "НАПРОТИВ",
    "СОГЛАСНО",
    "ОТНОСИТЕЛЬНО",
    "О",
    "Я",
    "НА",
    "ОН",
    "ТЫ",
    "ВСЕ",
    "ЕГО",
    "КТО",
    "МОЙ",
    "НАШ",
    "ОБА",
    "ОНИ",
    "САМ",
    "ТАК",
    "ТОМ",
    "ТОТ",
    "ЧТО",
    "ЧЕЙ-ТО",
    "КТО-НИБУДЬ",
    "ВЕСЬ",
    "ВСЕЙ",
    "ИНОЙ",
    "ОДИН",
    "СВОЙ",
    "СЕБЯ",
    "ЭТОТ",
    "ВЕСТИ",
    "КАКОЙ",
    "НИКТО",
    "НИЧЕЙ",
    "ПЛОХО",
    "САМЫЙ",
    "СОБОЙ",
    "КАКОЙ-ТО",
    "ВСЯКИЙ",
    "ДАННЫЙ",
    "ДРУГОЙ",
    "КАЖДЫЙ",
    "МНОГИЙ",
    "МНОГОЕ",
    "НЕЧЕГО",
    "НИЧЕГО",
    "НЕКОТОРЫЙ",
    "НЕПОХОЖИЙ",
    "ОСТАЛЬНОЙ",
    "СТАРАТЕЛЬНО",
    "БЫ",
    "ЖЕ",
    "НЕ",
    "НИ",
    "ТО",
    "ВОН",
    "ЕЩЕ",
    "НЕТ",
    "УЖЕ",
    "СЕБЯ",
    "ТОГО",
    "ВСЕГО",
    "ДОБРО",
    "ПРОСТО",
    "ХОРОШО",
]

conn = connect(ARGS.connection_string)
conn.autocommit = True


def prepare_search_sense_query(cursor):
    sql = """
        SELECT
          se.id,
          se.name,
          se.synset_id
        FROM senses se
          INNER JOIN synsets sy
            ON sy.id = se.synset_id
        WHERE se.lemma = $1
          AND sy.name = $2"""

    cursor.execute("PREPARE search_sense AS " + sql)


def prepare_rwn_word_relation_query(cursor):
    sql = """
      -- 1: synset_id (of the current collocation)
      -- 2: word (particular word in the collocation)
      -- Searching for collocations in rwn relations neighborhood

      -- Search for a sense with the same lemma as input word
      -- through a one step down relation
      SELECT
        sr.name,
        (SELECT name FROM synsets WHERE id = se.synset_id) synset_name
      FROM senses se
        INNER JOIN synset_relations sr
          ON sr.child_id = se.synset_id
      WHERE sr.parent_id = $1
        AND se.lemma = $2

      UNION ALL

      -- Search in the same synset
      SELECT
        'synset',
        (SELECT name FROM synsets WHERE id = synset_id) synset_name
      FROM senses
      WHERE lemma = $2
        AND synset_id = $1
      LIMIT 1"""
    cursor.execute("PREPARE select_rwn_word_relation AS " + sql)


def prepare_rwn_relation_query(cursor):
    sql = r"""
        SELECT
          synset_id,
          array_agg(name) senses,
          rel_name
        FROM
          (
            SELECT
              se.synset_id,
              se.name,
              'derivational_to' rel_name
            FROM senses se
              INNER JOIN synset_relations sr
                ON sr.child_id = se.synset_id
            WHERE sr.parent_id = $1
                  AND sr.name = 'POS-synonymy'
                  AND array_length(regexp_split_to_array(se.lemma, '\s+'), 1) = 1
            UNION ALL
            SELECT
              synset_id,
              name,
              'synonym_to'
            FROM senses
            WHERE synset_id = $1
                  AND array_length(regexp_split_to_array(lemma, '\s+'), 1) = 1
          ) t
        GROUP BY synset_id, rel_name
        ORDER BY rel_name"""
    cursor.execute("PREPARE select_rwn_relation AS " + sql)


def prepare_ruthes_relation_query(cursor):
    sql = """
      -- 1: word (particular word in the current collocation)
      -- 2: synset_name (synset_name of the collocation)
      -- Search for a source word in RuThes relations neighborhood one step down

      SELECT
        r.name,
        r.asp,
        c.name as synset_name
      FROM text_entry t
        INNER JOIN synonyms s
          ON s.entry_id = t.id
        INNER JOIN concepts c
          ON c.id = s.concept_id
        INNER JOIN relations r
          ON r.to_id = s.concept_id
        INNER JOIN concepts c2
          ON c2.id = r.from_id
      WHERE t.lemma = $1
        AND c2.name = $2
      LIMIT 1"""
    cursor.execute("PREPARE select_ruthes_relation AS " + sql)


def prepare_transitional_relation_query(cursor):
    sql = """
      -- 1: word (particular word in the current collocation)
      -- 2: names (relation names to start with)
      -- 3: tail_names (relation names to propagate with)
      -- 4: synset_name (name of the corresponding to the current collocation synset)
      -- Search a source word recursively through RuThes relations.

        WITH RECURSIVE tree (id, name, id_path, name_path, relation_path) AS (

          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            ARRAY[]::text[] relation_path
          FROM concepts
            WHERE name = $4

          UNION ALL

          SELECT
            c.id,
            c.name,
            array_append(tree.id_path, c.id),
            array_append(tree.name_path, c.name),
            array_append(tree.relation_path, r.name)
          FROM tree
            INNER JOIN relations r
              ON r.from_id = tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
          WHERE r.name = ANY($3)
            -- last relation in the path should be on of the allowed
            AND (
              tree.relation_path[array_upper(tree.relation_path, 1)] = ANY($2)
              OR array_upper(tree.relation_path, 1) IS NULL
            )
        )

        SELECT tree.*
        FROM tree
          INNER JOIN synonyms s
            ON s.concept_id = tree.id
          INNER JOIN text_entry t
            ON t.id = s.entry_id
        WHERE t.lemma = $1
        LIMIT 1"""
    cursor.execute("PREPARE select_transited_relation AS " + sql)


def prepare_bitransitional_relation_query(cursor):
    sql = """
      -- 1: word (particular word in the current collocation)
      -- 2: name (relation name to start with)
      -- 3: tail_names (relation names to propagate with)
      -- 4: synset_name (name of the corresponding to the current collocation synset)
      -- Search a source word recursively through RuThes relations.

        WITH RECURSIVE collocation_tree (id, name, id_path, name_path, parent_relation_name) AS (
          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            $2 parent_relation_name
          FROM concepts
            WHERE name = $4

          UNION ALL

          SELECT
            c.id,
            c.name,
            array_append(collocation_tree.id_path, c.id),
            array_append(collocation_tree.name_path, c.name),
            r.name parent_relation_name
          FROM collocation_tree
            INNER JOIN relations r
              ON r.from_id = collocation_tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
          WHERE r.name = ANY($3) AND collocation_tree.parent_relation_name = $2
        ),

        member_tree (id, name, id_path, name_path, parent_relation_name) AS (
          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            $2 parent_relation_name
          FROM concepts
            WHERE id IN(
              SELECT s.concept_id
              FROM synonyms s
                JOIN text_entry t ON t.id = s.entry_id
              WHERE t.lemma = $1
            )

          UNION ALL

          SELECT
            c.id,
            c.name,
            array_append(member_tree.id_path, c.id),
            array_append(member_tree.name_path, c.name),
            r.name parent_relation_name
          FROM member_tree
            INNER JOIN relations r
              ON r.from_id = member_tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
          WHERE r.name = ANY($3) AND member_tree.parent_relation_name = $2
        )

        SELECT --t.*,
            t.name,
            t.name_path,
            t.parent_relation_name,
            mt.name name1,
            mt.name_path name_path1,
            mt.parent_relation_name parent_relation_name1
        FROM collocation_tree t
          INNER JOIN member_tree mt
            ON mt.id = t.id
        ORDER BY array_length(t.id_path, 1), array_length(mt.id_path, 1)
        LIMIT 10"""
    cursor.execute("PREPARE select_bitransited_relation AS " + sql)


def prepare_sense_existence_check_query(cursor):
    sql = """
      SELECT
        (SELECT count(1) FROM senses WHERE lemma = $1) sense,
        (SELECT count(1) FROM text_entry WHERE lemma = $1) entry"""
    cursor.execute("PREPARE check_sense_existence AS " + sql)


def make_insert_query(table, fields, cur):
    fields_str = ", ".join(str(v) for v in fields)
    dollars = ", ".join("$" + str(i + 1) for i in range(len(fields)))
    placeholders = ", ".join("%({0})s".format(f) for f in fields)

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(
        placeholders=placeholders, table=table
    )

    sql = "PREPARE prepared_query_{table} AS ".format(
        table=table
    ) + "INSERT INTO {tbl} ({fields}) VALUES ({dollars})".format(
        fields=fields_str, dollars=dollars, tbl=table
    )

    cur.execute(sql)
    return sql_str


def main():
    test = ARGS.test
    without_matches = ARGS.without_matches

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, conn.cursor(
        cursor_factory=extras.RealDictCursor
    ) as cur2:

        print("prepare_rwn_word_relation_query", flush=True)
        prepare_rwn_word_relation_query(cur2)
        print("prepare_rwn_relation_query", flush=True)
        prepare_rwn_relation_query(cur2)
        print("prepare_ruthes_relation_query", flush=True)
        prepare_ruthes_relation_query(cur2)
        print("prepare_transitional_relation_query", flush=True)
        prepare_transitional_relation_query(cur2)
        print("prepare_bitransitional_relation_query", flush=True)
        prepare_bitransitional_relation_query(cur2)
        print("prepare_sense_existence_check_query", flush=True)
        prepare_sense_existence_check_query(cur2)

        if not test:
            print("prepare_search_sense_query", flush=True)
            prepare_search_sense_query(cur2)
            insert_relation_sql = make_insert_query(
                "sense_relations", ("parent_id", "child_id", "name"), cur
            )

        print("search collocations", flush=True)
        sql = r"""
          SELECT
            se.id,
            se.name,
            se.lemma,
            se.synset_id,
            sy.name synset_name
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
          WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) > 1
          ORDER BY se.name"""
        cur.execute(sql)
        # Fetch all right away to have total count of rows for tqdm progressbar
        rows = cur.fetchall()

        counters = {
            "collocations": 0,
            "collocationsNoRelations": 0,
            "collocationsAllRelations": 0,
            "noRelation": 0,
            "wordPresented": 0,
            "relations": defaultdict(int),
        }

        # Approximate algorithm:
        # 1. Find multiword sense
        # 2. For each word not in the blacklist search one source word:
        #    1. among all words with single meaning
        #    2. in RWN relations (one step up + in the same synset)
        #    3. in RuThes relations
        #    4. in RuThes relations transitionally

        print("start looping")
        for row in tqdm(rows, file=sys.stdout):
            print(flush=True, file=sys.stderr)
            print("{} ({}):".format(row["name"], row["synset_name"]), file=sys.stderr)
            counters["collocations"] += 1

            words_with_relations = 0
            checked_words = 0

            words = row["lemma"].split()
            detailed_words = []
            word_results = []
            for word in words:
                if word in blacklist:
                    continue
                checked_words += 1
                synset_name = None
                relation_name = None
                result = "нет"

                cur2.execute("EXECUTE check_sense_existence(%(word)s)", {"word": word})
                res = cur2.fetchone()
                sense_count = int(res["sense"])
                entries_count = int(res["entry"])

                if sense_count or entries_count:
                    synset_name, relation_name, result = search_everywhere(
                        cur2, word, row["synset_id"], row["synset_name"], row["lemma"]
                    )

                    if synset_name is None:
                        result = "нет"
                        counters["noRelation"] += 1
                        counters["wordPresented"] += 1
                        existence_strings = []
                        if sense_count:
                            existence_strings.append("есть в РуТез")
                        if entries_count:
                            existence_strings.append("есть в RWN")
                        result += " (" + (", ".join(existence_strings)) + ")"
                    else:
                        detailed_words.append((word, synset_name))
                        counters["relations"][relation_name] += 1
                        words_with_relations += 1

                print(f"{word} — {result}", file=sys.stderr)
                word_results.append(f"{word} — {result}")

            if words_with_relations > 0:
                counters["collocationsAllRelations"] += 1
                if not test:
                    params = {"parent_id": row["id"], "name": "composed_of"}
                    for word, synset_name in detailed_words:
                        cur2.execute(
                            "EXECUTE search_sense(%(word)s, %(synset_name)s)",
                            {"word": word, "synset_name": synset_name},
                        )
                        row_lexeme = cur2.fetchone()
                        if row_lexeme:
                            try:
                                cur2.execute(
                                    insert_relation_sql,
                                    {"child_id": row_lexeme["id"], **params},
                                )
                            except IntegrityError:
                                # Бывают словосочетания, образованные из одного слова (МАТЬ → МАТЬ МАТЕРИ)
                                pass
                        else:
                            print(
                                f"Лексема не найдена: {word} ({synset_name})",
                                file=sys.stderr,
                            )
                        #     return

            elif without_matches:
                counters["collocationsNoRelations"] += 1
                print(flush=True)
                print("{} ({}):".format(row["name"], row["synset_name"]))
                print("\n".join(word_results))

                if test:
                    params = {"synset_id": row["synset_id"]}
                    cur2.execute("EXECUTE select_rwn_relation(%(synset_id)s)", params)
                    for rel_row in cur2:
                        print(
                            " -- "
                            + rel_row["rel_name"]
                            + ": "
                            + ", ".join(rel_row["senses"])
                        )

        print(flush=True)
        print("Словосочетаний: " + str(counters["collocations"]))
        print("    — со всеми связями: " + str(counters["collocationsAllRelations"]))
        print("    — без связей: " + str(counters["collocationsNoRelations"]))
        print()
        print(
            "Слов без отношений: "
            + str(counters["noRelation"])
            + " ("
            + str(counters["wordPresented"])
            + " слов представлены в тезаурусе)"
        )
        print("Количество связей:")
        for relation, count in counters["relations"].items():
            print(relation + " — " + str(count))

    print("Done", flush=True)


def search_everywhere(
    cur: extras.DictCursorBase,
    word: str,
    c_synset_id: str,
    c_synset_name: str,
    c_lemma: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    synset_name, relation_name = search_in_rwn(cur, word, c_synset_id)
    if synset_name is not None:
        return synset_name, relation_name, relation_name

    synset_name, relation_name = search_in_ruthes(cur, word, c_lemma, c_synset_name)
    if synset_name is not None:
        return synset_name, relation_name, relation_name

    synset_name, relation_name, extra = search_in_ruthes_transitionally(
        cur, word, c_synset_name
    )
    if synset_name is not None:
        return synset_name, relation_name, extra

    # synset_name, relation_name, extra = search_in_ruthes_bitransitionally(cur, word, c_synset_name)
    # if synset_name is not None:
    # return synset_name, relation_name, extra

    return None, None, None


def search_in_rwn(
    cur: extras.DictCursorBase, word: str, synset_id: str
) -> Tuple[Optional[str], Optional[str]]:
    params = {"word": word}
    cur.execute(
        """
        SELECT sy.name
        FROM senses se
        JOIN synsets sy ON sy.id = se.synset_id
        WHERE se.lemma = %(word)s
        """,
        params,
    )
    res = cur.fetchall()
    if len(res) == 1:
        return (res[0]["name"], "single")

    params = {"synset_id": synset_id, "word": word}
    cur.execute("EXECUTE select_rwn_word_relation(%(synset_id)s, %(word)s)", params)
    res = cur.fetchone()
    return (res["synset_name"], res["name"]) if res else (None, None)


def search_in_ruthes(
    cur: extras.DictCursorBase, word: str, collocation_lemma: str, synset_name: str
) -> Tuple[Optional[str], Optional[str]]:
    params = {"word": word}
    cur.execute(
        """
        SELECT c.name
        FROM text_entry t
          INNER JOIN synonyms s
            ON s.entry_id = t.id
          INNER JOIN concepts c
            ON c.id = s.concept_id
        WHERE t.lemma = %(word)s
        """,
        params,
    )
    res = cur.fetchall()
    if len(res) == 1:
        return (res[0]["name"], "single")

    params = {"word": word, "synset_name": synset_name}
    cur.execute("EXECUTE select_ruthes_relation(%(word)s, %(synset_name)s)", params)
    res = cur.fetchone()
    return (res["synset_name"], res["name"]) if res else (None, None)


def search_in_ruthes_transitionally(
    cur: extras.DictCursorBase, word: str, synset_name: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    chain = None
    relation_name = "ВЫВОД"
    for i in range(2):
        if i == 0:
            names = ["ВЫШЕ", "ЦЕЛОЕ"]
            tail_names = ["АСЦ", "АСЦ1", "АСЦ2", "ЧАСТЬ"]
        else:
            names = ["НИЖЕ", "ЧАСТЬ"]
            tail_names = ["АСЦ", "АСЦ1", "АСЦ2", "ЧАСТЬ"]

        params = {
            "word": word,
            "names": names,
            "tail_names": names + tail_names,
            "synset_name": synset_name,
        }
        cur.execute(
            """EXECUTE select_transited_relation(
                %(word)s, %(names)s, %(tail_names)s, %(synset_name)s)""",
            params,
        )
        senses_chain = cur.fetchone()
        if senses_chain is not None:
            chain = print_chain(
                senses_chain["name_path"], senses_chain["relation_path"]
            )
            break

    return (
        (senses_chain["name"], relation_name, f"{relation_name} {chain}")
        if senses_chain is not None
        else (None, None, None)
    )


def print_chain(concepts: List[str], relations: List[str]):
    chain = ""
    i = 0
    for name in concepts:
        chain += name
        if i < len(relations):
            chain += " -{}→ ".format(relations[i])
        i += 1
    return chain


def search_in_ruthes_bitransitionally(
    cur: extras.DictCursorBase, word: str, synset_name: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    chain = None
    relation_name = "ВЫВОД"
    for i in range(2):
        if i == 0:
            names = ["ВЫШЕ", "ЦЕЛОЕ"]
            tail_names = ["АСЦ", "АСЦ1", "АСЦ2", "ЧАСТЬ"]
        else:
            names = ["НИЖЕ", "ЧАСТЬ"]
            tail_names = ["АСЦ", "АСЦ1", "АСЦ2", "ЧАСТЬ"]

        params = {
            "word": word,
            "names": names,
            "tail_names": names + tail_names,
            "synset_name": synset_name,
        }
        cur.execute(
            """EXECUTE select_bitransited_relation(
                %(word)s, %(name)s, %(tail_names)s, %(synset_name)s)""",
            params,
        )
        senses_chain = cur.fetchone()
        if senses_chain is not None:
            print_extraction(senses_chain)
            break

    return (
        (senses_chain["name"], relation_name, f"{relation_name} {chain}")
        if senses_chain is not None
        else (None, None, None)
    )


def print_extraction(data):
    print("\n      V\n".join([f"    {n}" for n in data["name_path"]]), file=sys.stderr)
    print("      A", file=sys.stderr)
    print(
        "\n      A\n".join(reversed([f"    {n}" for n in data["name_path1"][:-1]])),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
