-- DROP TABLE IF EXISTS lexemes;
DROP TABLE IF EXISTS sense_relations;
DROP TABLE IF EXISTS synset_relations;
DROP TABLE IF EXISTS relation_types;
DROP TABLE IF EXISTS senses;
DROP TABLE IF EXISTS synsets;

-- CREATE TABLE lexemes (
--   id       UUID PRIMARY KEY,
--   sense_id UUID REFERENCES senses (id),
--   name     TEXT UNIQUE
-- );

CREATE TABLE synsets (
  id             UUID PRIMARY KEY,
  name           TEXT,
  definition     TEXT,
  part_of_speech TEXT,
  UNIQUE (name, part_of_speech)
);

CREATE TABLE senses (
  id        UUID PRIMARY KEY,
  synset_id UUID REFERENCES synsets (id),
  name      TEXT,
  lemma     TEXT,
  main_word TEXT,
  synt_type TEXT,
  poses     TEXT,
  meaning   SMALLINT,
  UNIQUE (name, synset_id)
);

CREATE TABLE relation_types (
  name                  TEXT PRIMARY KEY,
  reverse_relation_name TEXT,
  parent_name           TEXT
);

CREATE TABLE sense_relations (
  parent_id UUID REFERENCES senses (id),
  child_id  UUID REFERENCES senses (id),
  name      TEXT REFERENCES relation_types (name),
  PRIMARY KEY (parent_id, child_id, name)
);

CREATE TABLE synset_relations (
  parent_id UUID REFERENCES synsets (id),
  child_id  UUID REFERENCES synsets (id),
  name      TEXT REFERENCES relation_types (name),
  PRIMARY KEY (parent_id, child_id, name)
);


INSERT INTO relation_types (name, reverse_relation_name, parent_name) VALUES
  ('antonym', 'antonym', NULL),
  ('hypernym', 'hyponym', NULL),
  ('hyponym', 'hypernym', NULL),
  ('instance hypernym', 'instance hyponym', 'hypernym'),
  ('instance hyponym', 'instance hypernym', 'hyponym'),
  ('member holonym', 'member meronym', 'holonym'),
  ('member meronym', 'member holonym', 'meronym'),
  ('substance holonym', 'substance meronym', 'holonym'),
  ('substance meronym', 'substance holonym', 'meronym'),
  ('part holonym', 'part meronym', 'holonym'),
  ('part meronym', 'part holonym', 'meronym'),
  ('derivational', 'derivational', NULL);
