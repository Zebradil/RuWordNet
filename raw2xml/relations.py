#!/usr/bin/python3
# coding=utf-8

from lxml import etree

root = etree.Element("relations")

with open("data/relats.txt", "r", encoding='Windows-1251') as inp:
    for line in inp:
        spl = line.strip().split('\t')

        doc = etree.SubElement(root, "rel")
        doc.set("from", spl[0])
        doc.set("to", spl[1])
        if int(spl[2]) == 10:
            doc.set("name", u"ВЫШЕ")
        elif int(spl[2]) == 20:
            doc.set("name", u"ЦЕЛОЕ")
        elif int(spl[2]) == 30:
            doc.set("name", u"НИЖЕ")
        elif int(spl[2]) == 40:
            doc.set("name", u"ЧАСТЬ")
        elif int(spl[2]) == 50 and len(spl) == 4 and spl[3] == "1":
            doc.set("name", u"АСЦ1")
        elif int(spl[2]) == 50 and len(spl) == 4 and spl[3] == "2":
            doc.set("name", u"АСЦ2")
        elif int(spl[2]) == 50:
            doc.set("name", u"АСЦ")

        if len(spl) == 4 and spl[3] == "В":
            doc.set("asp", "В")
        else:
            doc.set("asp", " ")

tree = etree.ElementTree(root)
tree.write("out/relations.xml", encoding="utf-8", pretty_print=True)
