#!/bin/bash

set -e

readonly files=(
    'Syn Adj Lemma.tab'
    'Syn Adj.tab'
    'Syn Noun Lemma.tab'
    'Syn Noun.tab'
    'Syn Verb Lemma.tab'
    'Syn Verb.tab'
)

readonly root=$(git rev-parse --show-toplevel)

for file in "${files[@]}"
do
    pipenv run python "${root}/scripts/import_ili_data.py" -f "${root}/data/ILI/${file}"
done
