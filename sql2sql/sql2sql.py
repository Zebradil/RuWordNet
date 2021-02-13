#!/usr/bin/env python3

import argparse
import logging
import os

from nltk.tree import ParentedTree
from psycopg2 import connect, errors, extras

PKG_ROOT = os.path.split(__file__)[0]

logging.basicConfig(level="INFO")

conn = None

POS_TYPE_MAP = {
    "N": {"N", "NG", "NGprep", "PrepG"},
    "V": {"V", "VG", "VGprep", "Prdc"},
    "Adj": {"Adj", "AdjG", "AdjGprep"},
}

# все типы текстовых входов, которые можно экспортировать
ALL_TYPES = set().union(*POS_TYPE_MAP.values())


def main():
    global conn

    parser = argparse.ArgumentParser(description="Convert RuThes to RuWordNet")

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

    parser.add_argument("-n", "--dry-run", action="store_true")

    ARGS = parser.parse_args()

    conn = connect(ARGS.connection_string)

    logging.info("Start")
    transform_ruthes_to_ruwordnet(ARGS.dry_run)
    logging.info("Done")


def transform_ruthes_to_ruwordnet(dry_run):

    inserter = SoftInserter(conn)

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:

        logging.info("Finding entries...")

        sql = """
          SELECT
            c.id   c_id,
            c.name c_name,
            c.gloss,
            t.id,
            t.name,
            t.lemma,
            t.main_word,
            t.pos_string,
            t.synt_type,

            -- Все понятия, связанные с текущим текстовым входом (многозначный текстовый вход)
            array_remove(
                array_agg(DISTINCT s2.concept_id),
                NULL
            )      concept_ids,

            -- Все текстовые входы, связанные с текущим понятием (синонимы)
            array_remove(
                array_agg(DISTINCT s3.entry_id),
                NULL
            )      entry_ids

          -- Связка "текстовый вход - понятие"
          FROM synonyms s
            INNER JOIN text_entry t
              ON t.id = s.entry_id
            INNER JOIN concepts c
              ON c.id = s.concept_id

            -- Остальные связки от текущего текстового входа
            INNER JOIN synonyms s2
              ON s2.entry_id = t.id

            -- Остальные связки от текущего понятия
            INNER JOIN synonyms s3
              ON s3.concept_id = s.concept_id

          GROUP BY t.id, c.id
          ORDER BY t.name NULLS LAST"""
        cur.execute(sql)

        concepts = {}
        # обработка данных из БД
        for row in cur:
            cid = row["c_id"]
            row["name"] = row["name"].strip()
            row["poses"] = row["pos_string"]
            # если текстовый вход многозначный — проставляем номер значения
            if len(row["concept_ids"]) > 1:
                row["meaning"] = sorted(row["concept_ids"]).index(cid) + 1
            else:
                row["meaning"] = 0
            # накопление понятий
            if cid not in concepts:
                concept = {
                    "id": cid,
                    "name": row["c_name"],
                    "gloss": row["gloss"],
                    "relations": [],
                    "entries": [],
                }
                concepts[cid] = concept
            # если текстовый вход имеет тип из списка для экспорта, он добавляется к понятию
            if row["synt_type"] in ALL_TYPES:
                entry = {
                    k: row[k]
                    for k in (
                        "id",
                        "name",
                        "lemma",
                        "synt_type",
                        "meaning",
                        "main_word",
                        "poses",
                    )
                }
                entry["part_of_speech"] = get_part_of_speech(entry["synt_type"])
                concepts[cid]["entries"].append(entry)

        logging.info("{0} entries found.".format(cur.rowcount))

        logging.info("Selecting relations...")
        sql = """
          SELECT r.*
          FROM relations r
          INNER JOIN concepts c1
            ON c1.id = r.from_id
          INNER JOIN concepts c2
            ON c2.id = r.to_id
          -- Связи с понятием-доменом особенные: они могут идти из различных
          -- частей речи к существительным, поэтому будут добавлены отдельно
          WHERE r.name != 'ДОМЕН'"""
        cur.execute(sql)
        # распределение отношений по понятиям
        for relation in cur:
            concepts[relation["from_id"]]["relations"].append(relation)

        count = len(concepts)
        i = 0
        logging.info("Processing concepts (%s)...", count)
        for cid, concept in concepts.items():
            i += 1

            # Определение, в каких частях речи представлено понятие
            poses = {entry["part_of_speech"] for entry in concept["entries"]}

            pos_to_concept_id = {}

            # Создание синсета для каждой из частей речи
            for pos in poses:
                synset_data = {
                    "id": gen_synset_index(cid, pos),
                    "name": concept["name"],
                    "definition": concept["gloss"],
                    "part_of_speech": pos,
                }
                if dry_run:
                    logging.info(synset_data)
                else:
                    inserter.insert_synset(synset_data)
                pos_to_concept_id[pos] = synset_data["id"]

            concepts[cid]["synset_ids"] = pos_to_concept_id

            # Создание понятий
            for entry in concept["entries"]:
                sense_data = {
                    "id": gen_sense_index(cid, entry["part_of_speech"], entry["id"]),
                    "synset_id": gen_synset_index(cid, entry["part_of_speech"]),
                    "name": entry["name"],
                    "lemma": entry["lemma"],
                    "synt_type": entry["synt_type"],
                    "meaning": entry["meaning"],
                    "main_word": entry["main_word"],
                    "poses": entry["poses"],
                }
                if dry_run:
                    logging.info(sense_data)
                else:
                    inserter.insert_sense(sense_data)

            if not dry_run:
                print(
                    "\rProgress: {0}% ({1})".format(round(i / count * 100), i),
                    end="",
                    flush=True,
                )

        print()
        logging.info("Processing relations...")
        i = 0
        for cid, concept in concepts.items():
            i += 1
            # Частеречная синонимия
            for parent_pos, parent_id in concept["synset_ids"].items():
                for child_pos, child_id in concept["synset_ids"].items():
                    if parent_pos != child_pos:
                        relation_data = {
                            "parent_id": parent_id,
                            "child_id": child_id,
                            "name": "POS-synonymy",
                        }
                        if dry_run:
                            logging.info(relation_data)
                        else:
                            inserter.insert_synset_relation(relation_data)

            # Остальные отношения
            for pos, synset_id in concept["synset_ids"].items():
                relations = []
                for relation in concept["relations"]:
                    relations += fix_relation(concepts, relation, POS_TYPE_MAP[pos])

                relations = uniqify(relations, lambda r: "{to_id}|{name}".format(**r))

                for relation in relations:
                    to_concept = concepts[relation["to_id"]]
                    # NOTE Возможно это проверка лишняя
                    if pos in to_concept["synset_ids"]:
                        relation_name = get_relation_name(
                            relation["name"], relation["asp"], pos
                        )
                        if relation_name is not None:
                            relation_data = {
                                "parent_id": synset_id,
                                "child_id": gen_synset_index(to_concept["id"], pos),
                                "name": relation_name,
                            }
                            if dry_run:
                                logging.info(relation_data)
                            else:
                                inserter.insert_synset_relation(relation_data)

            if not dry_run:
                print(
                    "\rProgress: {0}% ({1})".format(round(i / count * 100), i),
                    end="",
                    flush=True,
                )
        conn.commit()
        print()


def gen_synset_index(concept_id, part_of_speech) -> str:
    return "-".join((str(concept_id), part_of_speech[0]))


def gen_sense_index(concept_id, part_of_speech, entry_id) -> str:
    return "-".join((str(concept_id), part_of_speech[0], str(entry_id)))


class SoftInserter:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.synset_query = None
        self.sense_query = None
        self.synset_relation_query = None

    def insert_synset(self, data):
        self.do_insert(self.get_synset_query(), data, "synset")

    def insert_sense(self, data):
        self.do_insert(self.get_sense_query(), data, "sense")

    def insert_synset_relation(self, data):
        self.do_insert(self.get_synset_relation_query(), data, "relation")

    def do_insert(self, sql, data, mark: str):
        try:
            self.cursor.execute(sql, data)
            self.connection.commit()
        except errors.UniqueViolation:
            logging.debug("Skip existing %s %s", mark, data)
            self.connection.rollback()

    def get_synset_query(self) -> str:
        if self.synset_query is None:
            fields = ("id", "name", "definition", "part_of_speech")
            self.synset_query = self.make_insert_query("synsets", fields, self.cursor)
        return self.synset_query

    def get_sense_query(self) -> str:
        if self.sense_query is None:
            fields = (
                "id",
                "synset_id",
                "name",
                "lemma",
                "synt_type",
                "meaning",
                "main_word",
                "poses",
            )
            self.sense_query = self.make_insert_query("senses", fields, self.cursor)
        return self.sense_query

    def get_synset_relation_query(self) -> str:
        if self.synset_relation_query is None:
            fields = (
                "parent_id",
                "child_id",
                "name",
            )
            self.synset_relation_query = self.make_insert_query(
                "synset_relations", fields, self.cursor
            )
        return self.synset_relation_query

    @staticmethod
    def make_insert_query(table, fields, cur):
        fields = sorted(fields)
        fields_str = ", ".join(fields)
        dollars = ", ".join("$" + str(i + 1) for i, _ in enumerate(fields))
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


def get_part_of_speech(synt_type):
    for pos, types in POS_TYPE_MAP.items():
        if synt_type in types:
            return pos


def uniqify(seq, idfun=None):
    # order preserving
    if idfun is None:
        idfun = lambda x: x

    seen = set()
    for item in seq:
        marker = idfun(item)
        if marker in seen:
            continue
        seen.add(marker)
        yield item


def fix_relation(concepts, relation, types, path=None) -> list:
    """
    Проверяем текущее отношение - оно должно указывать на понятие
    с не пустыми текстовыми входами. Если отношение не проходит
    проверку, спускаемся по иерархии отношений вниз и повторяем
    проверку для низлежащих отношений.

    :param path:
    :param types:
    :param concepts:
    :param relation:
    :return:
    """
    if path is None:
        path = []
    # Это отношение уже рассматривалось
    if relation in path:
        return []
    path.append(relation)
    if relation["to_id"] in concepts:
        # Берём понятие, на которое указывает данное отношение
        toc = concepts[relation["to_id"]]
        if len([entry for entry in toc["entries"] if entry["synt_type"] in types]) > 0:
            # Если у понятия есть текстовые входы запрошенного типа, значит отношение нам подходит
            return [relation]
        # Отношение не подходит
        # Замыкание предусмотрено не для всех типов связей
        if relation["name"] not in {
            "НИЖЕ",
            "ВЫШЕ",
        }:  # , 'ЭКЗЕМПЛЯР', 'КЛАСС', 'ЦЕЛОЕ', 'ЧАСТЬ']:
            return []
        # Спускаемся ниже по иерархии
        relations = []
        # Смотрим все отношения низлежащего понятия
        for rel in toc["relations"]:
            # Проверяем, чтобы тип отношения совпадал с исходным отношением
            if rel["name"] == relation["name"]:
                # И запускаем проверку этого отношения
                relations += fix_relation(concepts, rel, types, path)
        return relations
    return []


def get_relation_name(rel_type, asp, pos):
    rel_map = {
        "N": {
            "АСЦ2": None,
            "ЦЕЛОЕ": "part holonym",
            "АСЦ1": None,
            "ЧАСТЬ": "part meronym",
            "НИЖЕ": "hyponym",
            "ВЫШЕ": "hypernym",
            "АСЦ": None,
            "АНТОНИМ": "antonym",
            "ЭКЗЕМПЛЯР": "instance hyponym",
            "КЛАСС": "instance hypernym",
            "ДОМЕН": "domain",
        },
        "V": {
            "АСЦ2": None,
            "ЦЕЛОЕ": "part holonym",
            "АСЦ1": None,
            "ЧАСТЬ": "part meronym",
            "НИЖЕ": "hyponym",
            "ВЫШЕ": "hypernym",
            "АСЦ": None,
            "АНТОНИМ": "antonym",
            "ЭКЗЕМПЛЯР": None,
            "КЛАСС": None,
            "ДОМЕН": "domain",
        },
        "Adj": {
            "АСЦ2": None,
            "ЦЕЛОЕ": None,
            "АСЦ1": None,
            "ЧАСТЬ": None,
            "НИЖЕ": "hyponym",
            "ВЫШЕ": "hypernym",
            "АСЦ": None,
            "АНТОНИМ": "antonym",
            "ЭКЗЕМПЛЯР": None,
            "КЛАСС": None,
            "ДОМЕН": "domain",
        },
    }

    if rel_type in {"ЧАСТЬ", "ЦЕЛОЕ"} and asp == "":
        return None

    return rel_map[pos][rel_type]


if __name__ == "__main__":
    main()
