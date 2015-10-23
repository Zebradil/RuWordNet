#!/usr/bin/python3
# coding=utf-8

from lxml import etree

concepts = {}

with open("data/concepts.txt", "r", encoding='Windows-1251') as inp:
    for line in inp:
        spl = line.split('\t')
        concepts[spl[0]] = spl[1:] + [""]

concept = None
gloss = ''
with open("data/concept_gloss_text_ready.txt", "r", encoding='Windows-1251') as inp:
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
