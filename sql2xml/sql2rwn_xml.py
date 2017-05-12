#!/usr/bin/env python3

import argparse
import os

from lxml import etree
from psycopg2 import extras, connect

PKG_ROOT = os.path.split(__file__)[0]

parser = argparse.ArgumentParser(description='Generate RuWordNet xml files')
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    '-c',
    '--connection-string',
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string
)
parser.add_argument(
    '-o',
    '--output-directory',
    help='A directory where xml-files will be saved',
    default=os.path.join(PKG_ROOT, 'out', 'rwn')
)

ARGS = parser.parse_args()

extras.register_uuid()


class Generator:
    def __init__(self, out_dir: str, connection):
        self.connection = connection
        self.out_dir = out_dir
        self.synset_counter = 0
        self.sense_counter = 0
        self.synsets = []
        self.senses = []
        self.synset_relations = []

    def run(self):
        print('Start')

        with self.connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
            print('Selecting all data...')
            print('synsets')
            cur.execute("""
              SELECT
                synsets.*,
                array_agg(senses.id) senses
              FROM synsets
              INNER JOIN senses
                ON synsets.id = senses.synset_id
              GROUP BY synsets.id
              ORDER BY part_of_speech""")
            rows = cur.fetchall()
            self.synsets = [{**row, **{'relations': [], 'index': self.gen_synset_index(row)}} for row in rows]
            synsets_by_id = {synset['id']: synset for synset in self.synsets}

            print('senses')
            cur.execute('SELECT * FROM senses')
            rows = cur.fetchall()
            self.senses = {row['id']: {**row, **{'synset_id': synsets_by_id[row['synset_id']]['index'],
                                                 'id': self.gen_sense_index(row),
                                                 'meaning': int(row['meaning']) + 1}} for row in rows}

            print('synset relations')
            cur.execute('SELECT * FROM synset_relations')
            self.synset_relations = cur.fetchall()

            print('distribute relations...')
            for relation in self.synset_relations:
                synset = synsets_by_id[relation['parent_id']]
                relation['parent_id'] = synset['index']
                relation['child_id'] = synsets_by_id[relation['child_id']]['index']
                synset['relations'].append(relation)

            current_pos = None
            print('building trees...')
            i = 0
            count = len(self.synsets)
            for synset in self.synsets:
                if current_pos != synset['part_of_speech']:
                    if current_pos is not None:
                        print()
                        self.write_file(synsets_root, 'synsets', current_pos)
                        self.write_file(senses_root, 'senses', current_pos)
                        self.write_file(synset_relations_root, 'synset_relations', current_pos)
                    synsets_root = etree.Element("synsets")
                    senses_root = etree.Element("senses")
                    synset_relations_root = etree.Element("relations")
                    current_pos = synset['part_of_speech']
                    print()
                    print('POS: ' + current_pos)
                self.add_synset(synsets_root, synset)
                for sense_id in synset['senses']:
                    self.add_sense(senses_root, self.get_sense(sense_id))
                for relation in synset['relations']:
                    self.add_synset_relation(synset_relations_root, relation)
                i += 1
                print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)
            self.write_file(synsets_root, 'synsets', current_pos)
            self.write_file(senses_root, 'senses', current_pos)
            self.write_file(synset_relations_root, 'synset_relations', current_pos)

            print()
            self.generate_composed_of_relations_file(cur)
            self.generate_derived_from_relations_file(cur)

        print('Done')

    def write_file(self, root: etree.Element, entity: str, pos: str):
        tree = etree.ElementTree(root)
        filename = os.path.join(self.out_dir, '{0}.{1}.xml'.format(entity, pos[0]))
        if os.path.isfile(filename):
            os.remove(filename)
        print('Output file: ' + filename)
        tree.write(filename, encoding="utf-8", pretty_print=True)

    def add_synset(self, root: etree.Element, row: dict):
        synset = etree.SubElement(root, 'synset')
        synset.set('id', row['index'])
        # synset.set('id', str(row['id']))
        synset.set('ruthes_name', row['name'])
        synset.set('definition', xstr(row['definition']))
        synset.set('part_of_speech', row['part_of_speech'])

        for sense_id in row['senses']:
            sense = self.get_sense(sense_id)
            sense_el = etree.SubElement(synset, 'sense')
            sense_el.set('id', str(sense['id']))
            sense_el.text = sense['lemma']

    def add_sense(self, root, row):
        self.fill_element_attributes(etree.SubElement(root, 'sense'), row)

    def add_synset_relation(self, root, row):
        self.fill_element_attributes(etree.SubElement(root, 'relation'), row)

    @staticmethod
    def fill_element_attributes(element, attributes: dict):
        for k, v in attributes.items():
            element.set(k, xstr(v))

    def get_sense(self, sense_id):
        return self.senses[sense_id]

    def get_relations(self, synset):
        return [relation for relation in self.synset_relations if relation['parent_id'] == synset['id']]

    def gen_synset_index(self, row):
        self.synset_counter += 1
        return row['part_of_speech'][0] + str(self.synset_counter)

    def gen_sense_index(self, row):
        self.sense_counter += 1
        return self.sense_counter

    def generate_sense_relations_file(self, relation_name, cur):
        print('Generating "{}" relations file'.format(relation_name))

        print('Getting relations from the database')
        sql = """
          SELECT
            parent_id,
            array_agg(child_id) child_ids
          FROM sense_relations
          WHERE name = %s
          GROUP BY parent_id"""
        cur.execute(sql, (relation_name,))

        root = etree.Element('senses')

        print('Generating xml')
        for row in cur:
            parent_sense = self.senses[row['parent_id']]
            x_sense = etree.SubElement(root, 'sense')
            x_sense.set('name', xstr(parent_sense['name']))
            x_sense.set('id', xstr(parent_sense['id']))
            x_sense.set('synset_id', xstr(parent_sense['synset_id']))
            x_rel = etree.SubElement(x_sense, relation_name)
            for child_id in row['child_ids']:
                child_sense = self.senses[child_id]
                x_lexeme = etree.SubElement(x_rel, 'sense')
                x_lexeme.set('name', xstr(child_sense['name']))
                x_lexeme.set('id', xstr(child_sense['id']))
                x_lexeme.set('synset_id', xstr(child_sense['synset_id']))

        tree = etree.ElementTree(root)
        filename = os.path.join(self.out_dir, '{}.xml'.format(relation_name))
        if os.path.isfile(filename):
            os.remove(filename)
        print('Output file: ' + filename)
        tree.write(filename, encoding="utf-8", pretty_print=True)

    def generate_derived_from_relations_file(self, cur):
        self.generate_sense_relations_file('derived_from', cur)

    def generate_composed_of_relations_file(self, cur):
        self.generate_sense_relations_file('composed_of', cur)


def xstr(value):
    return '' if value is None else str(value)


generator = Generator(out_dir=ARGS.output_directory, connection=connect(ARGS.connection_string))
generator.run()
