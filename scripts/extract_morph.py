#!/usr/bin/env python3

import argparse
import csv
import logging
import sys
from collections import namedtuple

import pymorphy2
from psycopg2 import connect, extras

parser = argparse.ArgumentParser(
    description="Extracts morphological info from text_entries"
)
connection_string = (
    "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
)
parser.add_argument(
    "-c",
    "--connection-string",
    type=str,
    help="Postgresql database connection string ({})".format(connection_string),
    default=connection_string,
)

ARGS = parser.parse_args()

logging.basicConfig(level=logging.INFO)

MorphData = namedtuple("MorphData", "pos_string main_word synt_type")

# Exceptions (due to incorrect detection by pymorphy2)
# WORD, DETECTED POS, CORRECT POS
POS_EXCEPTIONS = (
    ("МАРШ", "INTJ", "N"),
    ("ДЖУЧИ", "VERB", "N"),
    ("МАЛО", "NUMR", "Adv"),
    ("РОД", "VERB", "N"),
    ("ВИЛКОВО", "ADJS", "N"),
    ("КОВРОВ", "ADJS", "N"),
    ("ЛЕБЕДИН", "ADJS", "N"),
    ("ЛИШНЕ", "ADJS", "Adv"),
    ("ОПОЗИЦИОННО", "ADJS", "Adv"),
    ("ОПТО", "ADJS", "Adv"),
    ("ПЕША", "ADJS", "N"),
    ("ТОЛК", "INTJ", "N"),
    ("ОБА", "NUMR", "Adv"),
    ("ВПРАВЕ", "PRED", "Prdc"),
    ("Е", "VERB", "N"),
    ("НЕЛЬЗЯ", "PRED", "Prtc"),
    ("НЕТ", "PRED", "Prtc"),
    ("ПОЗИТИВНО", "ADJS", "Adv"),
    ("РАД", "ADJS", "N"),
    ("СМ", "VERB", "N"),
    ("АНТИВИРУСНИК", "VERB", "N"),
    ("БПЛА", "VERB", "N"),
    ("ВПЕЧАТЛЯЮЩЕ", "ADJS", "Adj"),
    ("ЖИЛИЩНИК", "VERB", "N"),
    ("ЗАРЯДНИК", "VERB", "N"),
    ("ЙЕТИ", "VERB", "N"),
    ("НАЖДАННО-НЕГАДАННО", "ADJS", "Adj"),
    ("ОФИСНИК", "VERB", "N"),
    ("ПЕТРОПАВЛ", "VERB", "N"),
    ("ПОВНИМАТЕЛЬНЕЕ", "COMP", "Adj"),
    ("ПОДОЛЬШЕ", "COMP", "Adj"),
    ("ПОДОРОЖЕ", "COMP", "Adj"),
    ("ПООСТОРОЖНЕЕ", "COMP", "Adj"),
    ("ПОПОДРОБНЕЕ", "COMP", "Adj"),
    ("ПОСЛОЖНЕЕ", "COMP", "Adj"),
    ("СИЗ", "ADJS", "N"),
    ("СКРОМНЕНЬКО", "ADJS", "Adj"),
    ("СПРЕЙ", "VERB", "N"),
    ("ФЕНШУЙ", "VERB", "N"),
    ("ФИОЛЕТОВО", "ADJS", "Adj"),
    ("ФОЛК", "VERB", "N"),
    ("ФЭНШУЙ", "VERB", "N"),
    ("ШИЗО", "ADJS", "N"),
)

# concept, text_entry, lemma, pos_string, main_word, synt_type
PREDEFINED_MORPHS = (
    ("АМЕРИКАНСКИЙ ДОЛЛАР", "У.Е.", "У Е", MorphData("Adj N", "Е", "NG"),),
    (
        "БЕЛОЗЕРСКОЕ",
        "Г. БЕЛОЗЕРСКОЕ",
        "Г БЕЛОЗЕРСКИЙ",
        MorphData("N N", "БЕЛОЗЕРСКИЙ", "NG"),
    ),
    ("ВИДНОЕ", "Г. ВИДНОЕ", "Г ВИДНОЕ", MorphData("N N", "ВИДНОЕ", "NG"),),
    (
        "ЖЕЛАТЕЛЬНЫЙ (НУЖНЫЙ)",
        "НЕ ЛИШНЕ",
        "НЕ ЛИШНЕ",
        MorphData("Prtc Adv ", "ЛИШНЕ", "AdvG"),
    ),
    ("ЖЕЛЕЗНОДОРОЖНЫЙ ТРАНСПОРТ", "Ж/Д", "Ж Д", MorphData("Adj N", "Д", "NG"),),
    ("ЗАОЗЕРНЫЙ", "Г. ЗАОЗЕРНЫЙ", "Г ЗАОЗЕРНЫЙ", MorphData("N N", "ЗАОЗЕРНЫЙ", "NG"),),
    (
        "ИЗОБИЛЬНЫЙ",
        "Г. ИЗОБИЛЬНЫЙ",
        "Г ИЗОБИЛЬНЫЙ",
        MorphData("N N", "ИЗОБИЛЬНЫЙ", "NG"),
    ),
    (
        "ОТСУТСТВИЕ ПРАВ",
        "НЕ ВПРАВЕ",
        "НЕ ВПРАВЕ",
        MorphData("Prtc Adv", "ВПРАВЕ", "AdvG"),
    ),
    (
        "НЕПОНЯТНЫЙ, НЕЯСНЫЙ",
        "НЕ ЯСНО",
        "НЕ ЯСНО",
        MorphData("Prtc Adv", " ЯСНО", " AdvG"),
    ),
    (
        "ПОВТОРНЫЙ",
        "НЕ ВПЕРВЫЕ",
        "НЕ ВПЕРВЫЕ",
        MorphData("Prtc Adv", "ВПЕРВЫЕ", "AdvG"),
    ),
    ("СЕЛЬСКОЕ ХОЗЯЙСТВО", "С/Х", "С Х", MorphData("Adj N", "Х", "NG"),),
    (
        "ЧАСТИЧНЫЙ",
        "НЕ ПОЛНОСТЬЮ",
        "НЕ ПОЛНОСТЬЮ",
        MorphData("Prtc Adv", "ПОЛНОСТЬЮ", "AdvG"),
    ),
    ("БОРЩ", "БОРЩЕВОЙ", "БОРЩЕВОЙ", MorphData("Adj", "БОРЩЕВОЙ", "Adj"),),
    (
        "БОСНИЯ И ГЕРЦЕГОВИНА",
        "БОСНО-ГЕРЦОГОВИНСКИЙ",
        "БОСНО-ГЕРЦОГОВИНСКИЙ",
        MorphData("Adj", "БОСНО-ГЕРЦОГОВИНСКИЙ", "Adj"),
    ),
    ("ПИНГВИН", "ПИНГВИНИЙ", "ПИНГВИНИЙ", MorphData("Adj", "ПИНГВИНИЙ", "Adj"),),
    (
        "ПОЛУСРЕДНИЙ ВЕС",
        "ПОЛУСРЕДНИЙ",
        "ПОЛУСРЕДНИЙ",
        MorphData("Adj", "ПОЛУСРЕДНИЙ", "Adj"),
    ),
    (
        "ПАРАЗИТИЗМ, ПОТРЕБИТЕЛЬСТВО",
        "ОТНОСИТЬСЯ ПОТРЕБИТЕЛЬСКИ",
        "ОТНОСИТЬСЯ ПОТРЕБИТЕЛЬСКИ",
        MorphData("V Adv", "ОТНОСИТЬСЯ", "VG"),
    ),
)

morph = pymorphy2.MorphAnalyzer()


def get_pos(word) -> str:
    pos = morph.parse(word)[0].tag.POS  # type: ignore

    for rule in POS_EXCEPTIONS:
        if rule[0] == word and rule[1] == pos:
            return rule[2]

    # More general rules for renaming POSes
    if pos == "NOUN" or pos is None:
        return "N"
    if pos in {"ADJF", "PRTF", "PRTS"}:
        return "Adj"
    if pos == "INFN":
        return "V"
    if pos == "ADVB":
        return "Adv"
    if pos == "PRCL":
        return "Prtc"
    if pos == "NPRO":
        return "Pron"
    if pos in {"PREP", "CONJ"}:
        return pos.capitalize()

    logging.info("%s: %s", word, pos)
    return str(pos)


def get_poses(word: str) -> str:
    return " ".join(get_pos(part) for part in word.split(" "))


def get_morph_data(word, concept) -> MorphData:
    for predefined_morph in PREDEFINED_MORPHS:
        if concept == predefined_morph[0] and word == predefined_morph[2]:
            return predefined_morph[3]
    parts = word.split(" ")
    poses = [get_pos(part) for part in parts]
    if len(parts) == 1:
        return MorphData(poses[0], word, poses[0])
    if "V" in poses:
        return MorphData(" ".join(poses), parts[poses.index("V")], "VG")
    if "N" in poses:
        return MorphData(" ".join(poses), get_main_noun(parts, poses), "NG")
    if "Adj" in poses:
        return MorphData(" ".join(poses), parts[poses.index("Adj")], "AdjG")


def get_main_noun(parts, poses) -> str:
    for n, pos in enumerate(poses):
        if pos == "N" and len(parts[n]) > 1:
            return parts[n]
    return ""


conn = connect(ARGS.connection_string)
with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
    cur.execute(
        """
            select c2.name concept_name, t.name entry_name, t.lemma
            from (
                select t2.*
                from v2_text_entry t2
                left join text_entry t on t.id = t2.id or t.name = t2.name or t.lemma = t2.lemma
                where t.id is null
            ) t
            join v2_synonyms s on s.entry_id = t.id
            join v2_concepts c2 on c2.id = s.concept_id
            join concepts c on c.id = c2.id
            where c2.id > 0
            order by 1, 2
        """
    )
    morph = pymorphy2.MorphAnalyzer()
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=[
            "concept_name",
            "entry_name",
            "lemma",
            "pos_string",
            "main_word",
            "synt_type",
        ],
        delimiter=",",
        quotechar='"',
    )
    writer.writeheader()
    for row in cur:
        morph_data = get_morph_data(row["lemma"])
        if morph_data is not None:
            row["pos_string"] = morph_data.pos_string
            row["main_word"] = morph_data.main_word
            row["synt_type"] = morph_data.synt_type
        writer.writerow(row)
