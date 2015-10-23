#!/usr/bin/python3
# coding=utf-8

import re

from lxml import etree

root = etree.Element("entries")


def main():
    filename = 'data/textentries.txt'

    word_chars = '[А-Яа-я\d\w\-",\(\)\./]'
    pp = '(\d+)\s+((?:\s{0,2}' + word_chars + ')+)\s+(?:10|20)\s+((?:\s{0,2}' + word_chars + ')+)'
    pattern0 = re.compile(pp)
    pp += '\s+([A-Za-z]+)'
    pattern1 = re.compile(pp)
    pp += '\s+([А-Яа-я\-]+)\s+([A-Za-z ]+)'
    pattern2 = re.compile(pp)

    keys = ('id', 'name', 'lemma', 'synt_type', 'main_word', 'pos_string')

    file = open(filename, 'r', encoding='Cp1251')

    i = 0

    for line in file:
        i += 1
        match_obj = pattern2.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups())))
            continue

        match_obj = pattern1.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups() + ('', ''))))
            continue

        match_obj = pattern0.match(line)
        if match_obj is not None:
            insert_data(dict(zip(keys, match_obj.groups() + ('', '', ''))))
            continue
        print('DOES NOT MATCH: ' + line)
    print(str(i) + ' rows inserted')

    tree = etree.ElementTree(root)
    tree.write("out/text_entry.xml", encoding="utf-8", pretty_print=True)


def insert_data(element):
    doc = etree.SubElement(root, "entry")
    doc.set("id", element['id'])

    name = etree.SubElement(doc, "name")
    name.text = element['name']

    name = etree.SubElement(doc, "lemma")
    name.text = element['lemma']

    mw = etree.SubElement(doc, "main_word")
    mw.text = element['main_word']

    st = etree.SubElement(doc, "synt_type")
    st.text = element['synt_type']

    ps = etree.SubElement(doc, "pos_string")
    ps.text = element['pos_string']


main()
