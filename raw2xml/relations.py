#!/usr/bin/env python3

import argparse
import os
import re

from lxml import etree

PKG_ROOT = os.path.dirname(os.path.abspath(__file__))

parser = argparse.ArgumentParser(
    description="Generates RuThes relations.xml file from txt data file."
)
parser.add_argument(
    "-s",
    "--source-file",
    type=str,
    help="Source txt file",
    default=os.path.join(PKG_ROOT, "data", "relats.txt"),
)
parser.add_argument(
    "-d",
    "--destination-file",
    type=str,
    help="Destination xml file",
    default=os.path.join(PKG_ROOT, "out", "relations.xml"),
)

ARGS = parser.parse_args()

root = etree.Element("relations")

with open(ARGS.source_file, "r", encoding="Windows-1251") as inp:
    rgx = re.compile("\s+")
    for line in inp:
        spl = rgx.split(line.strip())

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
tree.write(ARGS.destination_file, encoding="utf-8", pretty_print=True)
