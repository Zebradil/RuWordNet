#!/usr/bin/python3
# coding=utf-8

from lxml import etree

root = etree.Element("synonyms")

with open("data/synonyms.txt", "r", encoding="Windows-1251") as inp:
    for line in inp:
        spl = line.strip().split('\t')

        doc = etree.SubElement(root, "entry_rel")
        doc.set("concept_id", spl[0])
        doc.set("entry_id", spl[1])

tree = etree.ElementTree(root)
tree.write("out/synonyms.xml", encoding="utf-8", pretty_print=True)
