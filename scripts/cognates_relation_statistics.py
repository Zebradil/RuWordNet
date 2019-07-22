#!/usr/bin/env python3

# pylint: disable=C0103
# pylint: disable=C0111

import argparse
import logging
import multiprocessing
import os
import sys
from queue import Queue
from threading import Thread
from typing import Dict, List

from psycopg2 import IntegrityError, connect, extras
from tqdm import tqdm

logging.basicConfig(level="INFO", format="%(word)-31s %(name)-4s %(seq)-3s %(message)s")

PKG_ROOT = os.path.dirname(os.path.abspath(__file__))
PREDEFINED_COGNATES_FILE = os.path.join(PKG_ROOT, "predefined_cognates.txt")

parser = argparse.ArgumentParser(description="Extract derivation relations from RuThes and RuWordNet.")
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)
parser.add_argument(
    "-t", "--test", help="Only show found relations, don't insert new relations in database", action="store_true"
)

ARGS = parser.parse_args()

prefixes = [
    "АНТИ",
    "АРХИ",
    "БЕЗ",
    "БЕС",
    "БИ",
    "ВЗ",
    "ВЗО",
    "ВИЦЕ",
    "ВНЕ",
    "ВНУТРИ",
    "ВО",
    "ВОЗ",
    "ВОЗО",
    "ВОС",
    "ВС",
    "ВСЕ",
    "ВЫ",
    "ГИПЕР",
    "ДЕ",
    "ДЕЗ",
    "ДИС",
    "ДО",
    "ЗА",
    "ИЗ",
    "ИЗО",
    "ИМ",
    "ИНТЕР",
    "ИР",
    "ИС",
    "ИСПОД",
    "КВАЗИ",
    "КОЕ",
    "КОЙ",
    "КОНТР",
    "МАКРО",
    "МЕЖ",
    "МЕЖДО",
    "МЕЖДУ",
    "МИКРО",
    "НА",
    "НАД",
    "НАДО",
    "НАИ",
    "НЕ",
    "НЕБЕЗ",
    "НЕБЕС",
    "НЕДО",
    "НИ",
    "НИЗ",
    "НИЗО",
    "НИС",
    "ОБ",
    "ОБЕЗ",
    "ОБЕР",
    "ОБЕС",
    "ОБО",
    "ОКОЛО",
    "ОТ",
    "ОТО",
    "ПА",
    "ПЕРЕ",
    "ПО",
    "ПОД",
    "ПОД",
    "ПОЗА",
    "ПОСЛЕ",
    "ПОСТ",
    "ПРА",
    "ПРЕ",
    "ПРЕД",
    "ПРЕДИ",
    "ПРЕДО",
    "ПРИ",
    "ПРО",
    "ПРОТИВО",
    "ПРОТО",
    "ПСЕВДО",
    "РАЗ",
    "РАЗО",
    "РАС",
    "РЕ",
    "РОЗ",
    "РОС",
    "САМО",
    "СВЕРХ",
    "СО",
    "СРЕДИ",
    "СУ",
    "СУБ",
    "СУПЕР",
    "СЫЗ",
    "ТРАНС",
    "ТРЕ",
    "УЛЬТРА",
    "ЧЕРЕЗ",
    "ЧЕРЕС",
    "ЧРЕЗ",
    "ЭКЗО",
    "ЭКС",
    "ЭКСТРА",
]
prefixes.sort(key=len, reverse=True)

roots_groups = (
    ("БР", "БИР"),  # +
    ("БИВ", "БИТ"),  # +
    ("БЫВ", "БЫТ"),  # +
    ("ВЕР", "ВОР"),  # +
    ("ГН", "ГОН"),  # +
    ("ДАВ", "ДАТ", "ДАЧ"),  # +
    ("ЛИВ", "ЛИТ"),  # +
    ("ЛЕЧ", "ЛЕГ"),  # +
    ("ЧЕС", "ЧЕТ"),  # +
    ("КАЖ", "КАЗ"),  # +
    ("ЖИВ", "ЖИТ"),  # +
    ("ПИВ", "ПИТ"),  # +
    ("ЛЕП", "ЛИП"),  # +
    ("МЕЩ", "МЕСТ"),  # +
    ("МЫСЛ", "МЫШЛ"),  # +
    ("МЯТ", "МИН"),  # +
    ("РАЖ", "РАЗ"),  # +
    ("РОД", "РОЖ"),  # +
    ("СЫТ", "СЫЩ"),  # +
    ("СИД", "СИЖ"),  # +
    ("СОБ", "САБ"),  # +
    ("СКОЛ", "СКАЛ"),  # +
    ("СКОБ", "СКАБ"),  # +
    ("ХОД", "ХОЖ"),  # +
    ("НЕС", "НОС", "НОШ"),  # +
    ("ТЯГ", "ТЯН", "ТЯЖ"),  # +
    ("ДЛЕ", "ДЛИ"),  # +
    ("ОБИЖ", "ОБИД"),  # +
    ("СМОТР", "СМАТР"),  # +
    ("ГОР", "ГАР"),
    ("КЛОН", "КЛАН"),
    ("ТВОР", "ТВАР"),
    ("ЗОР", "ЗАР"),
    ("ПЛАВ", "ПЛОВ"),
    ("ЛАГ", "ЛОЖ"),
    ("РАСТ", "РАЩ", "РОС"),
    ("КАС", "КОС"),
    ("СКАК", "СКОЧ"),
    ("БИР", "БЕР"),
    ("ДИР", "ДЕР"),
    ("МИР", "МЕР"),
    ("ТИР", "ТЕР"),
    ("ПИР", "ПЕР"),
    ("ЖИГ", "ЖЕГ"),
    ("СТИЛ", "СТЕЛ"),
    ("БЛИСТ", "БЛЕСТ"),
    ("ЧИТ", "ЧЕТ"),
    ("МОК", "МОЧ", "МАК"),
    ("РАВН", "РОВН"),
)

prefix_exceptions = (
    "НЕРВ",
    "ОБИЖ",
    "ОБИД",
    "ПОЧТ",
    "ОТВЕТ",
    "ОТВЕЧ",
    "ПОЛН",
    "РОСТ",
    "РОССИ",
    "ПРАВ",
    "НИЩЕ",
    "ТРЕН",
    "ТРЕВОЖ",
    "ТРЕВОГ",
    "ЗАВИД",
    "ЗАВИСТ",
)

predefined_cognates = {}
with open(PREDEFINED_COGNATES_FILE, "r") as f:
    for line in f:
        word1, word2 = line.strip().split(" ")
        if word1 not in predefined_cognates:
            predefined_cognates[word1] = []
        if word2 not in predefined_cognates:
            predefined_cognates[word2] = []
        predefined_cognates[word1].append(word2)
        predefined_cognates[word2].append(word1)


def prepare_search_cognates(cursor):
    sql = r"""
        -- 1: sense_id
        -- 2: synset_id
        -- 3: word
        -- 4: synset_name
        -- Поиск слов-претендентов на общий корень. Поиск выполняется с учётом значений слов.

        SELECT s.name, s.rel_name, s.synset_name
        FROM (
            -- Проверка слов-синонимов
               SELECT
                 name,
                 $4 synset_name,
                 'synset' rel_name,
                 'RuWordNet' source
               FROM senses se
               WHERE id != $1
                 AND synset_id = $2

               UNION

            -- Проверка слов в связанных синсетах (только один шаг по иерархии вниз)
               SELECT
                 se.name,
                 (SELECT name FROM synsets WHERE id = se.synset_id) synset_name,
                 sr.name,
                 'RuWordNet' source
               FROM senses se
                 INNER JOIN synset_relations sr
                   ON sr.child_id = se.synset_id
               WHERE sr.parent_id = $2

               UNION

            -- Проверка текстовых входов из связанных концептов РуТез (только один шаг по иерархии вниз)
               SELECT
                 t2.name,
                 (SELECT name FROM concepts WHERE id = s2.concept_id) synset_name,
                 r.name,
                 'RuThes' source
               FROM text_entry t1
                 INNER JOIN synonyms s1 ON s1.entry_id = t1.id
                 INNER JOIN concepts c ON c.id = s1.concept_id
                 INNER JOIN relations r ON r.from_id = s1.concept_id
                 INNER JOIN synonyms s2 ON s2.concept_id = r.to_id
                 INNER JOIN text_entry t2 ON t2.id = s2.entry_id
               WHERE t1.name = $3
                 -- Отсечение многозначных текстовых входов
                 -- Поиск происходит по конкретному значению, которое определяется через synsets.id/concept.id
                 AND c.name = $4
             ) s
        WHERE substr(s.name, 1, 4) = substr($3, 1, 4)
          AND array_length(regexp_split_to_array(s.name, '\s+'), 1) = 1"""

    cursor.execute("PREPARE search_cognates AS " + sql)


def prepare_search_cognates_transitionally(cursor):
    sql = r"""
        -- 1: word
        -- 2: names (relation names to start with)
        -- 3: tail_names (relation names to propagate with)
        -- 4: synset_name
        -- Рекурсивный поиск слов-претендентов на общий корень. Поиск выполняется с учётом значений слов.

        WITH RECURSIVE tree (id, name, id_path, name_path, relation_path) AS (

        -- Поиск начинается от заданного значения (synset_name)
          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            ARRAY[]::text[] relation_path
          FROM concepts
          WHERE name = $4

          UNION ALL

        -- Спускаться по иерархии понятий можно на неограниченную по связям определённого типа (name)
        -- и далее на один шаг по связи из дополнительного списка (tail_names)
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

        -- Далее поиск выполняется по текстовым входам понятий из под-дерева, найденного в рекурсивной части
        SELECT
          t.name,
          tree.name synset_name,
          tree.name_path,
          tree.relation_path
        FROM tree
          INNER JOIN synonyms s
            ON s.concept_id = tree.id
          INNER JOIN text_entry t
            ON t.id = s.entry_id
        WHERE t.name != $1
          AND array_length(id_path, 1) > 1
          -- AND substr(t.name, 1, 4) = substr($1, 1, 4)
          AND array_length(regexp_split_to_array(t.name, '\s+'), 1) = 1"""
    cursor.execute("PREPARE search_cognates_transitionally AS " + sql)


def prepare_search_sense(cursor):
    sql = """
        SELECT
          se.id,
          se.name,
          se.synset_id
        FROM senses se
          INNER JOIN synsets sy
            ON sy.id = se.synset_id
        WHERE se.name = $1
          AND sy.name = $2"""

    cursor.execute("PREPARE search_sense AS " + sql)


def make_insert_query(table, fields, cur):
    fields_str = ", ".join(str(v) for v in fields)
    dollars = ", ".join("$" + str(i + 1) for i in range(len(fields)))
    placeholders = ", ".join("%({0})s".format(f) for f in fields)

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(placeholders=placeholders, table=table)

    sql = "PREPARE prepared_query_{table} AS ".format(
        table=table
    ) + "INSERT INTO {tbl} ({fields}) VALUES ({dollars})".format(fields=fields_str, dollars=dollars, tbl=table)

    cur.execute(sql)
    return sql_str


cached_results = {}


def cache_result(func):
    def get_key_and_needle(word1, word2):
        if word1 > word2:
            return word1, word2
        else:
            return word2, word1

    def cache_result_inner(word1, word2):
        key, needle = get_key_and_needle(word1, word2)
        if key in cached_results:
            if needle in cached_results[key]:
                # print('got from cache')
                return cached_results[key][needle]
        else:
            cached_results[key] = {}
        result = func(word1, word2)
        cached_results[key][needle] = result
        # print('added to cache')
        return result

    return cache_result_inner


@cache_result
def is_cognates(word1, word2):
    # print("checking words: {} {}".format(word1, word2))
    if word1 == word2:
        # print("same word")
        return False
    if word1 in predefined_cognates:
        if word2 in predefined_cognates[word1]:
            # print("from predefined list")
            return True
    words1 = remove_prefixes(word1)
    words2 = remove_prefixes(word2)
    for sub1 in words1:
        for sub2 in words2:
            if check_substrings(sub1, sub2):
                # print("are cognates: {} {}".format(word1, word2))
                return True
    # print("aren't cognates: {} {}".format(word1, word2))
    return False


def check_substrings(word1, word2):
    match_len = min(len(word1), len(word2), 3)
    # print("words after processing: {} {}".format(word1[:match_len], word2[:match_len]))
    if word1[:match_len] == word2[:match_len]:
        # print("beginnings are equal")
        return True
    for root in get_roots_group(word1):
        if word2.find(root) == 0:
            # print("root is found {}".format(root))
            return True
    return False


def get_roots_group(word):
    for group in roots_groups:
        for root in group:
            if word.find(root) == 0:
                return group
    return []


def remove_prefixes(word):
    for exception in prefix_exceptions:
        if word.find(exception) == 0:
            return [word]
    forms = []
    for prefix in prefixes:
        if word.startswith(prefix):
            trimmed = word.replace(prefix, "", 1)
            if len(trimmed) >= 3 and set(trimmed) & set("АОИЕЁЭЫУЮЯ"):
                forms.append(trimmed)
    return forms if forms else [word]


def main():
    conn = connect(ARGS.connection_string)
    conn.autocommit = True

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:

        print("search collocations", flush=True)
        sql = r"""
          SELECT
            se.id,
            se.name,
            se.synset_id,
            sy.name synset_name
          FROM senses se
            INNER JOIN synsets sy
              ON sy.id = se.synset_id
          WHERE array_length(regexp_split_to_array(se.lemma, '\s+'), 1) = 1"""
        cur.execute(sql)

        workers_count = multiprocessing.cpu_count() - 1
        queue = Queue(workers_count * 10)
        for i in range(workers_count):
            worker = Worker()
            worker.set(queue, ARGS.connection_string, logging.getLogger(f"w-{i}"), ARGS.test)
            worker.daemon = True
            worker.start()

        print("start looping", flush=True)
        rows = cur.fetchall()
        for row in tqdm(rows, file=sys.stdout):
            queue.put(row)

        queue.join()

        # TODO shutdown workers?

    print("Done")


class Worker(Thread):
    def set(self, queue: Queue, connection_string: str, logger: logging.Logger, is_test=False) -> None:
        self.queue = queue
        self.connection_string = connection_string
        self.logger = logger
        self.is_test = is_test

    def conn_up(self) -> None:
        self.conn = connect(self.connection_string)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(cursor_factory=extras.RealDictCursor)

        self.logger.debug("prepare_search_cognates")
        prepare_search_cognates(self.cursor)
        self.logger.debug("prepare_search_cognates_transitionally")
        prepare_search_cognates_transitionally(self.cursor)

        if not self.is_test:
            self.logger.debug("prepare_search_sense")
            prepare_search_sense(self.cursor)
            self.insert_relation_sql = make_insert_query(
                "sense_relations", ("parent_id", "child_id", "name"), self.cursor
            )

    def conn_down(self) -> None:
        self.cursor.close()
        self.conn.close()

    def run(self) -> None:
        self.conn_up()
        while True:
            task = self.queue.get()
            if task is None:
                self.conn_down()
                break
            self.process(task)
            self.queue.task_done()

    def process(self, row: Dict[str, str]) -> None:
        def e(word):
            e.counter += 1
            return {"word": word, "seq": e.counter}

        e.counter = 0
        test = self.is_test
        cur2 = self.cursor
        # print(flush=True)
        self.logger.info("{} ({})".format(row["name"], row["synset_name"]), extra=e(row["name"]))

        if not test:
            lexemes = []

        params = {
            "sense_id": row["id"],
            "synset_id": row["synset_id"],
            "word": row["name"],
            "synset_name": row["synset_name"],
        }
        cur2.execute("EXECUTE search_cognates(%(sense_id)s, %(synset_id)s, %(word)s, %(synset_name)s)", params)
        for cognate in cur2.fetchall():
            if is_cognates(row["name"], cognate["name"]):
                self.logger.info("    " + cognate["name"] + ": " + cognate["rel_name"], extra=e(row["name"]))
                if not test:
                    lexemes.append((cognate["name"], cognate["synset_name"]))

        for i in range(2):
            if i == 0:
                names = ["ВЫШЕ", "ЦЕЛОЕ"]
                tail_names = ["АСЦ", "ЧАСТЬ"]
            else:
                names = ["НИЖЕ", "ЧАСТЬ"]
                tail_names = ["АСЦ"]

            params = {
                "word": row["name"],
                "names": names,
                "tail_names": names + tail_names,
                "synset_name": row["synset_name"],
            }
            cur2.execute(
                """EXECUTE search_cognates_transitionally(
                    %(word)s, %(names)s, %(tail_names)s, %(synset_name)s)""",
                params,
            )
            for senses_chain in cur2.fetchall():
                if is_cognates(row["name"], senses_chain["name"]):
                    chain = build_chain(senses_chain["name_path"], senses_chain["relation_path"])
                    self.logger.info("    " + senses_chain["name"] + ":" + chain, extra=e(row["name"]))
                    if not test:
                        lexemes.append((senses_chain["name"], senses_chain["synset_name"]))

        if not test and lexemes:
            params = {"parent_id": row["id"], "name": "derived_from"}
            for lexeme in set(lexemes):
                cur2.execute(
                    "EXECUTE search_sense(%(name)s, %(synset_name)s)", {"name": lexeme[0], "synset_name": lexeme[1]}
                )
                row_lexeme = cur2.fetchone()
                if row_lexeme:
                    try:
                        cur2.execute(self.insert_relation_sql, {"child_id": row_lexeme["id"], **params})
                    except IntegrityError:
                        # Если такое отношение уже есть, не останавливаем выполнение
                        pass


def build_chain(concepts: List[str], relations: List[str]):
    chain = ""
    i = 0
    for name in concepts:
        chain += name
        if i in relations:
            chain += " -{}→ ".format(relations[i])
    return chain


if __name__ == "__main__":
    main()
