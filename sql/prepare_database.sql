-- Functions

CREATE OR REPLACE FUNCTION is_multiword(name text)
RETURNS BOOL AS $$
    BEGIN
        RETURN array_length(regexp_split_to_array(name, '\s+'), 1) > 1;
    END;
$$ LANGUAGE PLPGSQL IMMUTABLE;



-- RuThes tables

CREATE TABLE concepts (
  id     INTEGER PRIMARY KEY,
  name   TEXT,
  gloss  TEXT,
  domain TEXT
);

CREATE INDEX ON concepts (name);

CREATE TABLE relations (
  from_id INTEGER, -- REFERENCES concepts (id),
  to_id   INTEGER, -- REFERENCES concepts (id),
  name    TEXT,
  asp     TEXT,
  PRIMARY KEY (from_id, to_id, name)
);

CREATE INDEX ON relations (name);
CREATE INDEX ON relations (from_id);
CREATE INDEX ON relations (to_id);

CREATE TABLE text_entry (
  id         INTEGER PRIMARY KEY,
  name       TEXT,
  lemma      TEXT,
  main_word  TEXT,
  synt_type  TEXT,
  pos_string TEXT
);

CREATE INDEX ON text_entry (name);
CREATE INDEX ON text_entry (lemma);

CREATE TABLE synonyms (
  concept_id INTEGER, -- REFERENCES concepts (id),
  entry_id   INTEGER, -- REFERENCES text_entry (id)
  PRIMARY KEY (concept_id, entry_id)
);

-- RuWordNet tables

CREATE TABLE synsets (
  id             UUID PRIMARY KEY,
  name           TEXT,
  definition     TEXT,
  part_of_speech TEXT,
  UNIQUE (name, part_of_speech)
);

CREATE INDEX ON synsets (name);
CREATE INDEX ON synsets (part_of_speech);

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

CREATE INDEX ON senses (name);
CREATE INDEX ON senses (lemma);
CREATE INDEX senses_is_multiword ON senses (is_multiword(name));
CREATE INDEX senses_words ON senses USING GIN (regexp_split_to_array(name, '\s+')) WHERE is_multiword(name);

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

CREATE INDEX ON sense_relations (parent_id);
CREATE INDEX ON sense_relations (child_id);
CREATE INDEX ON sense_relations (name);

CREATE TABLE synset_relations (
  parent_id UUID REFERENCES synsets (id),
  child_id  UUID REFERENCES synsets (id),
  name      TEXT REFERENCES relation_types (name),
  PRIMARY KEY (parent_id, child_id, name)
);

CREATE INDEX ON synset_relations (parent_id);
CREATE INDEX ON synset_relations (child_id);
CREATE INDEX ON synset_relations (name);


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
  ('domain', 'domain member', NULL),
  ('domain member', 'domain', NULL),
  ('cause', NULL, NULL),
  ('entailment', NULL, NULL),
  ('derived_from', NULL, NULL),
  ('composed_of', NULL, NULL),
  ('POS-synonymy', 'POS-synonymy', NULL);

CREATE TABLE ili (
  link_type TEXT,
  concept_id INT REFERENCES concepts (id),
  wn_lemma TEXT,
  wn_id TEXT,
  wn_gloss TEXT,
  source TEXT NOT NULL,
  PRIMARY KEY (concept_id, wn_id, source)
);
CREATE INDEX ili_wn_id ON ili (wn_id);
CREATE INDEX ili_wn_id_substring_1 ON ili (substring(wn_id, '^\d+'));
CREATE INDEX ili_wn_id_substring_2 ON ili (substring(wn_id, '.$'));
CREATE INDEX ili_source ON ili (source);

CREATE TABLE ili_map_wn (
    ili text NOT NULL,
    wn text,
    version int,
    PRIMARY KEY (ili, wn, version)
);
CREATE INDEX ili_map_wn_wn ON ili_map_wn (wn);
CREATE INDEX ili_map_wn_version ON ili_map_wn (version);
CREATE INDEX ili_map_wn_wn_substring_1 ON ili_map_wn (substring(wn, '^\d+'));
CREATE INDEX ili_map_wn_wn_substring_2 ON ili_map_wn (substring(wn, '.$'));

CREATE TABLE wn_mapping (
    wn30 text NOT NULL,
    wn31 text NOT NULL,
    kind text NOT NULL,
    PRIMARY KEY (wn30, wn31, kind)
);
CREATE INDEX wn_mapping_wn31 ON wn_mapping (wn31);

CREATE TABLE roots (
    word text NOT NULL,
    root text NOT NULL,
    index int NOT NULL,
    quality text NOT NULL,
    PRIMARY KEY (word, root, index, quality)
);
/* CREATE INDEX roots_word_root ON roots(word, root); */
