#!/usr/bin/env python3

import argparse
import logging
import re
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(
    description="Groups words of the same roots in manually created file"
)
parser.add_argument("-s", "--source-file", type=str, help="Source file with roots")

ARGS = parser.parse_args()

filename = ARGS.source_file

re_line = re.compile(r"^([\w-]+)\s*\|\s*\{([\w\s,.-]+)\}$")

roots_to_words = defaultdict(set)
for line in open(filename):
    line = line.strip()
    if res := re_line.match(line):
        root = res.group(1)
        words = res.group(2)
        logging.debug("%s => %s", root, words)
        roots_to_words[root].update(
            {word.strip() for word in words.split(",") if word.strip()}
        )
    else:
        logging.warning("Skipped: %s", line)

width = len(max(roots_to_words, key=len))
for root, words in roots_to_words.items():
    print(" {:{width}} | {{{}}}".format(root, ",".join(sorted(words)), width=width))
