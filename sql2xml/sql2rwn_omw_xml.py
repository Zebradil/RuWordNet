#!/usr/bin/env python3

import argparse
import logging
from collections import defaultdict

from lxml import etree
from psycopg2 import connect, extras

parser = argparse.ArgumentParser(description="Generate RuWordNet OMW xml files")
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
parser.add_argument("-l", "--level", help="Logging level", default=logging.INFO)

ARGS = parser.parse_args()

logging.basicConfig(level=ARGS.level)

extras.register_uuid()

LEXICON_ID = "RuWordNet"

allowed_synset_relations = {
    "agent",
    "also",
    "antonym",
    "attribute",
    "be_in_state",
    "causes",
    "classified_by",
    "classifies",
    "co_agent_instrument",
    "co_agent_patient",
    "co_agent_result",
    "co_instrument_agent",
    "co_instrument_patient",
    "co_instrument_result",
    "co_patient_agent",
    "co_patient_instrument",
    "co_result_agent",
    "co_result_instrument",
    "co_role",
    "direction",
    "domain_region",
    "domain_topic",
    "entails",
    "eq_synonym",
    "exemplifies",
    "has_domain_region",
    "has_domain_topic",
    "holo_location",
    "holo_member",
    "holo_part",
    "holo_portion",
    "holo_substance",
    "holonym",
    "hypernym",
    "hyponym",
    "in_manner",
    "instance_hypernym",
    "instance_hyponym",
    "instrument",
    "involved",
    "involved_agent",
    "involved_direction",
    "involved_instrument",
    "involved_location",
    "involved_patient",
    "involved_result",
    "involved_source_direction",
    "involved_target_direction",
    "is_caused_by",
    "is_entailed_by",
    "is_exemplified_by",
    "is_subevent_of",
    "location",
    "manner_of",
    "mero_location",
    "mero_member",
    "mero_part",
    "mero_portion",
    "mero_substance",
    "meronym",
    "other",
    "patient",
    "restricted_by",
    "restricts",
    "result",
    "role",
    "similar",
    "source_direction",
    "state_of",
    "subevent",
    "target_direction",
}

allowed_sense_relations = {
    "also",
    "antonym",
    "derivation",
    "domain_region",
    "domain_topic",
    "exemplifies",
    "has_domain_region",
    "has_domain_topic",
    "is_exemplified_by",
    "other",
    "participle",
    "pertainym",
    "similar",
}


def sense_rel(relation):
    return {"derived_from": "derivation"}.get(relation, relation)


def synset_rel(relation):
    return {
        "cause": "causes",
        "domain": "domain_topic",
        "entailment": "entails",
        "instance hypernym": "instance_hypernym",
        "instance hyponym": "instance_hyponym",
        "part holonym": "holo_part",
        "part meronym": "mero_part",
    }.get(relation, relation)


def pos(part_of_speech):
    return {"adj": "a"}.get(part_of_speech, part_of_speech)


def synset_id(data):
    return "-".join((LEXICON_ID, str(data["concept_id"]), pos(data["part_of_speech"])))


def sense_id(data):
    return "-".join(
        (
            str(data["concept_id"]),
            pos(data["part_of_speech"]),
            str(data["text_entry_id"]),
        )
    )


def run(connection):
    nsmap = {"dc": "http://purl.org/dc/elements/1.1/"}
    LexicalResource = etree.Element("LexicalResource", nsmap=nsmap)
    Lexicon = etree.SubElement(
        LexicalResource,
        "Lexicon",
        id=LEXICON_ID,
        label="RuWordNet",
        language="ru",
        email="john.doe@example.com",
        license="proprietary",
        version="1.0",
        citation="TODO",
        url="http://ruwordnet.ru",
    )

    with connection.cursor(cursor_factory=extras.RealDictCursor) as cur:
        logging.info("Selecting all data...")

        logging.info("sense relations")
        cur.execute(
            """
            SELECT
                  ser.parent_id,
                  ser.child_id,
                  ser.name,
                  c.id concept_id,
                  LOWER(sy.part_of_speech) part_of_speech,
                  t.id text_entry_id
                FROM sense_relations ser
                  JOIN senses se ON se.id = ser.child_id
                  JOIN synsets sy ON sy.id = se.synset_id
                  JOIN concepts c ON c.name = sy.name
                  JOIN text_entry t ON t.name = se.name
            """
        )
        sense_relations = defaultdict(list)
        for row in cur:
            sense_relations[row["parent_id"]].append(row)

            logging.info("senses")
            cur.execute(
                """
                SELECT
                  se.id,
                  se.synset_id,
                  LOWER(se.name) "name",
                  LOWER(se.lemma) lemma,
                  t.id text_entry_id,
                  LOWER(sy.part_of_speech) part_of_speech,
                  c.id concept_id
                FROM senses se
                  JOIN synsets sy ON sy.id = se.synset_id
                  JOIN concepts c ON c.name = sy.name
                  JOIN text_entry t ON t.name = se.name
                """
            )
            lexical_entries = defaultdict(list)
            for row in cur:
                lexical_entries[row["lemma"]].append(row)

            for lemma, senses in lexical_entries.items():
                sense = senses[0]
                LexicalEntry = etree.SubElement(
                    Lexicon, "LexicalEntry", id=str(sense["text_entry_id"])
                )
                etree.SubElement(
                    LexicalEntry,
                    "Lemma",
                    writtenForm=lemma,
                    partOfSpeech=pos(sense["part_of_speech"]),
                )
                etree.SubElement(LexicalEntry, "Form", writtenForm=sense["name"])
                for sense in senses:
                    Sense = etree.SubElement(
                        LexicalEntry,
                        "Sense",
                        id=sense_id(sense),
                        synset=synset_id(sense),
                    )

                    for relation in sense_relations[sense["id"]]:
                        args = {"target": sense_id(relation)}
                        relType = sense_rel(relation["name"])
                        if relType in allowed_sense_relations:
                            args["relType"] = relType
                        else:
                            args["relType"] = "other"
                            args["note"] = relType
                        etree.SubElement(Sense, "SenseRelation", **args)

            logging.info("synset relations")
            cur.execute(
                """
                SELECT
                  syr.parent_id,
                  syr.child_id,
                  syr.name,
                  LOWER(sy.part_of_speech) part_of_speech,
                  c.id concept_id
                FROM synset_relations syr
                  JOIN synsets sy ON sy.id = syr.child_id
                  JOIN concepts c ON c.name = sy.name
                """
            )
            synset_relations = defaultdict(list)
            for row in cur:
                synset_relations[row["parent_id"]].append(row)

            logging.info("synsets")
            cur.execute(
                """
                SELECT
                  sy.id,
                  sy.name,
                  sy.definition,
                  LOWER(sy.part_of_speech) part_of_speech,
                  c.id concept_id,
                  ili.wn_id,
                  ili.wn_gloss
                FROM synsets sy
                 JOIN concepts c ON c.name = sy.name
                 LEFT JOIN ili ON ili.concept_id = c.id
                """
            )
            for synset in cur:
                args = {
                    "id": synset_id(synset),
                    "partOfSpeech": pos(synset["part_of_speech"]),
                }
                if (
                    synset["wn_id"] is not None
                    and args["partOfSpeech"] == synset["wn_id"][-1]
                ):
                    args["ili"] = synset["wn_id"]
                Synset = etree.SubElement(Lexicon, "Synset", **args)

                for relation in synset_relations[synset["id"]]:
                    args = {"target": synset_id(relation)}
                    relType = synset_rel(relation["name"])
                    if relType in allowed_synset_relations:
                        args["relType"] = relType
                    else:
                        args["relType"] = "other"
                        args["note"] = relType
                    etree.SubElement(Synset, "SynsetRelation", **args)

        print(
            etree.tostring(
                LexicalResource,
                encoding="utf-8",
                pretty_print=True,
                xml_declaration=True,
                doctype='<!DOCTYPE LexicalResource SYSTEM "http://globalwordnet.github.io/schemas/WN-LMF-1.0.dtd">',
            ).decode()
        )


run(connect(ARGS.connection_string))
