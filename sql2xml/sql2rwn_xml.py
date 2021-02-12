#!/usr/bin/env python3

import argparse
import os
from collections import defaultdict

from lxml import etree
from nltk.corpus import wordnet as wn
from psycopg2 import connect, extras

PKG_ROOT = os.path.dirname(os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser(description="Generate RuWordNet xml files")
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
        "-o",
        "--output-directory",
        help="A directory where xml-files will be saved",
        default=os.path.join(PKG_ROOT, "out", "rwn"),
    )

    ARGS = parser.parse_args()

    generator = Generator(
        out_dir=ARGS.output_directory, connection=connect(ARGS.connection_string)
    )
    generator.run()


def gen_synset_index(concept_id, part_of_speech) -> str:
    return "-".join((str(concept_id), part_of_speech[0]))


def gen_sense_index(concept_id, part_of_speech, entry_id) -> str:
    return "-".join((str(concept_id), part_of_speech[0], str(entry_id)))


class Generator:
    def __init__(self, out_dir: str, connection):
        self.connection = connection
        self.out_dir = out_dir
        self.synset_counter = 0
        self.sense_counter = 0
        self.synsets = []
        self.senses = []
        self.synset_relations = []
        self.ili = []

    def run(self):
        print("Start")

        with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
            print("Selecting all data...")
            print("synsets")
            cur.execute(
                """
                SELECT
                  synsets.*,
                  c.id concept_id,
                  array_agg(senses.id) senses
                FROM synsets
                INNER JOIN senses ON synsets.id = senses.synset_id
                INNER JOIN concepts c ON c.name = synsets.name
                GROUP BY synsets.id, c.id
                ORDER BY part_of_speech
                """
            )
            self.synsets = [{**row, "relations": [],} for row in cur]
            synsets_by_id = {synset["id"]: synset for synset in self.synsets}

            print("senses")
            cur.execute(
                """
                SELECT
                  senses.*,
                  synsets.part_of_speech,
                  c.id concept_id,
                  t.id entry_id
                FROM synsets
                INNER JOIN senses ON synsets.id = senses.synset_id
                INNER JOIN concepts c ON c.name = synsets.name
                INNER JOIN text_entry t ON t.name = senses.name
                """
            )
            self.senses = {
                row["id"]: {**row, "meaning": int(row["meaning"]) + 1,} for row in cur
            }

            print("synset relations")
            cur.execute("SELECT * FROM synset_relations")
            self.synset_relations = cur.fetchall()

            print("distribute relations...")
            for relation in self.synset_relations:
                synsets_by_id[relation["parent_id"]]["relations"].append(relation)

            print("building trees...")

            current_pos = ""
            synsets_root = None
            senses_root = None
            synset_relations_root = None

            count = len(self.synsets)
            for i, synset in enumerate(self.synsets):
                if current_pos != synset["part_of_speech"]:
                    if current_pos:
                        print()
                        self.write_file(synsets_root, "synsets", current_pos)
                        self.write_file(senses_root, "senses", current_pos)
                        self.write_file(
                            synset_relations_root, "synset_relations", current_pos
                        )
                    synsets_root = etree.Element("synsets")
                    senses_root = etree.Element("senses")
                    synset_relations_root = etree.Element("relations")
                    current_pos = synset["part_of_speech"]
                    print()
                    print("POS: " + current_pos)
                self.add_synset(synsets_root, synset)
                for sense_id in synset["senses"]:
                    self.add_sense(senses_root, self.senses[sense_id])
                for relation in synset["relations"]:
                    self.add_synset_relation(synset_relations_root, relation)
                print(
                    "\rProgress: {0}% ({1})".format(
                        round((i + 1) / count * 100), i + 1
                    ),
                    end="",
                    flush=True,
                )
            self.write_file(synsets_root, "synsets", current_pos)
            self.write_file(senses_root, "senses", current_pos)
            self.write_file(synset_relations_root, "synset_relations", current_pos)

            print()
            self.generate_composed_of_relations_file(cur)
            self.generate_derived_from_relations_file(cur)
            self.generate_ili_file(cur)

        print("Done")

    def write_file(self, root: etree.Element, entity: str, pos: str):
        tree = etree.ElementTree(root)
        filename = os.path.join(self.out_dir, "{0}.{1}.xml".format(entity, pos[0]))
        if os.path.isfile(filename):
            os.remove(filename)
        print("Output file: " + filename)
        tree.write(filename, encoding="utf-8", pretty_print=True)

    def add_synset(self, root: etree.Element, row: dict):
        synset = etree.SubElement(root, "synset")
        synset.set("id", row["id"])
        synset.set("ruthes_name", row["name"])
        synset.set("definition", xstr(row["definition"]))
        synset.set("part_of_speech", row["part_of_speech"])

        for sense_id in row["senses"]:
            sense = self.senses[sense_id]
            sense_el = etree.SubElement(synset, "sense")
            sense_el.set("id", sense["id"])
            sense_el.text = sense["lemma"]

    def add_sense(self, root, row):
        self.fill_element_attributes(etree.SubElement(root, "sense"), row)

    def add_synset_relation(self, root, row):
        self.fill_element_attributes(etree.SubElement(root, "relation"), row)

    @staticmethod
    def fill_element_attributes(element, attributes: dict):
        for k, v in attributes.items():
            element.set(k, xstr(v))

    def generate_sense_relations_file(self, relation_name, cur):
        print('Generating "{}" relations file'.format(relation_name))

        print("Getting relations from the database")
        sql = """
          SELECT
            parent_id,
            array_agg(child_id) child_ids
          FROM sense_relations
          WHERE name = %s
          GROUP BY parent_id"""
        cur.execute(sql, (relation_name,))

        root = etree.Element("senses")

        print("Generating xml")
        for row in cur:
            parent_sense = self.senses[row["parent_id"]]
            x_sense = etree.SubElement(root, "sense")
            x_sense.set("name", parent_sense["name"])
            x_sense.set("id", parent_sense["id"])
            x_sense.set("synset_id", parent_sense["synset_id"])
            x_rel = etree.SubElement(x_sense, relation_name)
            for child_id in row["child_ids"]:
                child_sense = self.senses[child_id]
                x_lexeme = etree.SubElement(x_rel, "sense")
                x_lexeme.set("name", child_sense["name"])
                x_lexeme.set("id", child_sense["id"])
                x_lexeme.set("synset_id", child_sense["synset_id"])

        tree = etree.ElementTree(root)
        filename = os.path.join(self.out_dir, "{}.xml".format(relation_name))
        if os.path.isfile(filename):
            os.remove(filename)
        print("Output file: " + filename)
        tree.write(filename, encoding="utf-8", pretty_print=True)

    def generate_derived_from_relations_file(self, cur):
        self.generate_sense_relations_file("derived_from", cur)

    def generate_composed_of_relations_file(self, cur):
        self.generate_sense_relations_file("composed_of", cur)

    def generate_ili_file(self, cur):
        print("Generating ILI file")

        synsets_by_concept_id = defaultdict(list)
        for synset in self.synsets:
            synsets_by_concept_id[synset["concept_id"]].append(synset)

        cur.execute(
            """
            SELECT concept_id, array_agg(wn_id) wn_ids
            FROM (
              SELECT concept_id, wn_id
              FROM ili
              WHERE source != 'manual'
                AND approved
              UNION
              SELECT concept_id, m.wn30
              FROM ili
                JOIN wn_mapping m ON m.wn31 = ili.wn_id
              WHERE source = 'manual'
                AND approved
            ) t
            GROUP BY concept_id
            """
        )
        ili = cur.fetchall()

        root = etree.Element("ili")
        for row in ili:
            wn_synsets_by_pos = defaultdict(list)
            for wn_synset in [self.get_wn_synset(wn_id) for wn_id in row["wn_ids"]]:
                pos = wn_synset.pos()
                wn_synsets_by_pos["a" if pos == "s" else pos].append(wn_synset)

            for rwn_synset in synsets_by_concept_id[row["concept_id"]]:
                pos = rwn_synset["part_of_speech"]
                wn_synsets = wn_synsets_by_pos.get(
                    "a" if pos == "Adj" else pos.lower(), []
                )
                if not wn_synsets:
                    continue

                x_match = etree.SubElement(root, "match")

                x_rwn_synset = etree.SubElement(x_match, "rwn-synset")
                x_rwn_synset.set("id", rwn_synset["id"])
                x_rwn_synset.set("ruthes_name", rwn_synset["name"])
                x_rwn_synset.set("definition", xstr(rwn_synset["definition"]))
                x_rwn_synset.set("part_of_speech", rwn_synset["part_of_speech"])

                for sense_id in rwn_synset["senses"]:
                    self.add_sense(x_rwn_synset, self.senses[sense_id])

                for wn_synset in wn_synsets:
                    x_wn_synset = etree.SubElement(x_match, "wn-synset")
                    x_wn_synset.set(
                        "id", str(wn_synset.offset()).zfill(8) + "-" + wn_synset.pos()
                    )
                    x_wn_synset.set("definition", wn_synset.definition())
                    for lemma in wn_synset.lemmas():
                        x_lemma = etree.SubElement(x_wn_synset, "lemma")
                        x_lemma.set("name", lemma.name())
                        x_lemma.set("key", lemma.key())

        tree = etree.ElementTree(root)
        filename = os.path.join(self.out_dir, "ili.xml")
        if os.path.isfile(filename):
            os.remove(filename)
        print("Output file: " + filename)
        tree.write(filename, encoding="utf-8", pretty_print=True)

    @staticmethod
    def get_wn_synset(wn_id: str):
        parts = wn_id.split("-")
        return wn.synset_from_pos_and_offset(parts[1], int(parts[0]))


def xstr(value):
    return "" if value is None else str(value)


if __name__ == "__main__":
    main()
