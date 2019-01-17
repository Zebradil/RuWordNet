#!/usr/bin/env python3

import argparse
import os
import re

from lxml import etree

PKG_ROOT = os.path.split(__file__)[0]

parser = argparse.ArgumentParser(description="Generates RuThes synonyms.xml file from txt data file.")
parser.add_argument(
    "-s", "--source-file", type=str, help="Source txt file", default=os.path.join(PKG_ROOT, "data", "synonyms.txt")
)
parser.add_argument(
    "-d",
    "--destination-file",
    type=str,
    help="Destination xml file",
    default=os.path.join(PKG_ROOT, "out", "synonyms.xml"),
)

ARGS = parser.parse_args()

root = etree.Element("synonyms")

with open(ARGS.source_file, "r", encoding="Windows-1251") as inp:
    rgx = re.compile("\s{2,}")
    for line in inp:
        spl = rgx.split(line.strip())

        doc = etree.SubElement(root, "entry_rel")
        doc.set("concept_id", spl[0])
        doc.set("entry_id", spl[1])

tree = etree.ElementTree(root)
tree.write(ARGS.destination_file, encoding="utf-8", pretty_print=True)
