#!/usr/bin/env python3

import json
import logging
import re
import sys
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

good_fp = open("good_grouped_roots.txt", "w")
bad_fp = open("bad_grouped_roots.txt", "w")

re_line = re.compile(r"^([\w-]+)\s*\|\s*\{([\w\s,.-]+)\}$")

words_to_roots = defaultdict(set)
roots_to_words = defaultdict(set)
good_roots_to_words = defaultdict(set)
bad_roots_to_words = defaultdict(set)
for i, line in enumerate(sys.stdin):
    line = line.strip()
    if res := re_line.match(line):
        root = res.group(1)
        words = {word.strip() for word in res.group(2).split(",") if word.strip()}
        logging.debug("%s => %s", root, words)
        for word in words:
            words_to_roots[word].add(root)
        roots_to_words[root].update(words)
    else:
        logging.warning("[%s] Skipped: %s", i, line)
        sys.exit()

for word, roots in words_to_roots.items():
    if len(roots) > 1:
        for root in roots:
            bad_roots_to_words[root].add(word)
    else:
        good_roots_to_words[next(iter(roots))].add(word)

for words in good_roots_to_words.values():
    print(json.dumps(list(words)), file=good_fp)

width = len(max(bad_roots_to_words, key=len))
for root, words in bad_roots_to_words.items():
    print(
        " {:{width}} | {{{}}}".format(root, ",".join(sorted(words)), width=width),
        file=bad_fp,
    )
