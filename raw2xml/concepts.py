#!/usr/bin/env python3

import argparse
import os

from lxml import etree

PKG_ROOT = os.path.split(__file__)[0]

parser = argparse.ArgumentParser(description='Generates RuThes concepts.xml file from txt data file.')
parser.add_argument(
    '-s',
    '--source-file',
    type=str,
    help='Source txt file',
    default=os.path.join(PKG_ROOT, 'data', 'concepts.txt')
)
parser.add_argument(
    '-g',
    '--gloss-file',
    type=str,
    help='Additional source txt file with context gloss',
    default=os.path.join(PKG_ROOT, 'data', 'concept_gloss_text_ready.txt')
)
parser.add_argument(
    '-d',
    '--destination-file',
    type=str,
    help='Destination xml file',
    default=os.path.join(PKG_ROOT, 'out', 'concepts.xml')
)

ARGS = parser.parse_args()

concepts = {}

with open(ARGS.source_file, "r", encoding='Windows-1251') as inp:
    for line in inp:
        spl = line.split('\t')
        concepts[spl[0]] = spl[1:] + [""]

concept = None
gloss = ''
with open(ARGS.gloss_file, "r", encoding='Windows-1251') as inp:
    for line in inp:
        spl = line.split('\t')
        if len(spl) == 2:
            if len(gloss):
                if concept and concept[0] in concepts:
                    concepts[concept[0]][-1] = gloss.strip()
                elif concept:
                    print('Concept not found', concept, gloss)
                else:
                    print('Gloss without concept', gloss)
            gloss = ''
            concept = spl
            continue
        gloss = gloss + ' ' + line.strip()

root = etree.Element("concepts")

srt = sorted(concepts.items(), key=lambda item: item[0])

for elem in srt:
    idx = elem[0]
    values = elem[1]
    doc = etree.SubElement(root, "concept")
    doc.set("id", idx)

    name = etree.SubElement(doc, "name")
    name.text = values[0]

    gloss = etree.SubElement(doc, "gloss")
    gloss.text = values[-1]

    domain = etree.SubElement(doc, "domain")
    if int(values[1]) in [8, 40, 264, 2968, 40, 264, 296]:
        domain.text = "GL"
    elif int(values[1]) == 2:
        domain.text = "GEO"
    else:
        domain.text = "SOC-POL"

tree = etree.ElementTree(root)
tree.write("out/concepts.xml", encoding="utf-8", pretty_print=True)
