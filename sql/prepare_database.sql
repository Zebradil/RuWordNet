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
CREATE INDEX ON synonyms (entry_id);

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

CREATE OR REPLACE FUNCTION wn_id_variants(text) RETURNS text[]
  AS $$
    SELECT CASE substring($1, '.$')
      WHEN 'a'
        THEN array[$1, substring($1, '^\d+') || '-' || 's']
      WHEN 's'
        THEN array[$1, substring($1, '^\d+') || '-' || 'a']
      ELSE array[$1]
    END
  $$
  LANGUAGE SQL
  IMMUTABLE
  RETURNS NULL ON NULL INPUT;

CREATE TABLE ili (
  link_type TEXT,
  concept_id INT REFERENCES concepts (id),
  wn_lemma TEXT,
  wn_id TEXT,
  wn_gloss TEXT,
  source TEXT NOT NULL,
  approved BOOL,
  PRIMARY KEY (concept_id, wn_id, source)
);
CREATE INDEX ili_wn_id ON ili (wn_id);
CREATE INDEX ili_wn_id_substring_1 ON ili (substring(wn_id, '^\d+'));
CREATE INDEX ili_wn_id_substring_2 ON ili (substring(wn_id, '.$'));
CREATE INDEX ili_wn_id_variants ON ili USING GIN (wn_id_variants(wn_id));
CREATE INDEX ili_source ON ili (source);
CREATE INDEX ili_approved ON ili (approved);

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
CREATE INDEX ili_map_wn_wn_variants ON ili_map_wn USING GIN (wn_id_variants(wn));

CREATE TABLE wn_mapping (
    wn30 text NOT NULL,
    wn31 text NOT NULL,
    kind text NOT NULL,
    PRIMARY KEY (wn30, wn31, kind)
);
CREATE INDEX wn_mapping_wn31 ON wn_mapping (wn31);
CREATE INDEX wn_mapping_wn30_substring_1 ON wn_mapping (substring(wn30, '^\d+'));
CREATE INDEX wn_mapping_wn30_substring_2 ON wn_mapping (substring(wn30, '.$'));
CREATE INDEX wn_mapping_wn31_substring_1 ON wn_mapping (substring(wn31, '^\d+'));
CREATE INDEX wn_mapping_wn31_substring_2 ON wn_mapping (substring(wn31, '.$'));
CREATE INDEX wn_mapping_wn30_variants ON wn_mapping USING GIN (wn_id_variants(wn30));
CREATE INDEX wn_mapping_wn31_variants ON wn_mapping USING GIN (wn_id_variants(wn31));

CREATE TABLE roots (
    word text NOT NULL,
    root text NOT NULL,
    index int NOT NULL,
    quality text NOT NULL,
    PRIMARY KEY (word, root, index, quality)
);
/* CREATE INDEX roots_word_root ON roots(word, root); */

CREATE TABLE wn_data (
    id text NOT NULL,
    name text NOT NULL,
    definition text NOT NULL,
    lemma_names json NOT NULL,
    version int NOT NULL,
    PRIMARY KEY (id)
);

/*
Tables for RuThes v2 (loaded from json representation)
*/
CREATE TABLE v2_concepts (
  id INTEGER PRIMARY KEY,
  name TEXT,
  gloss TEXT,
  en_name TEXT,
  is_abstract BOOL,
  is_arguable BOOL,
  domainmask INTEGER
);
CREATE INDEX ON v2_concepts (name);
CREATE INDEX ON v2_concepts USING GIST (name gist_trgm_ops);

CREATE TABLE v2_text_entry (
  id INTEGER PRIMARY KEY,
  name TEXT,
  lemma TEXT,
  is_ambig BOOL,
  is_arguable BOOL
);
CREATE INDEX ON v2_text_entry (name);
CREATE INDEX ON v2_text_entry (lemma);

CREATE TABLE v2_synonyms (
  concept_id INTEGER, -- REFERENCES v2_concepts (id),
  entry_id   INTEGER, -- REFERENCES v2_text_entry (id)
  PRIMARY KEY (concept_id, entry_id)
);

CREATE TABLE v2_relations (
  from_id INTEGER, -- REFERENCES concepts (id),
  to_id   INTEGER, -- REFERENCES concepts (id),
  name    TEXT,
  asp     TEXT,
  is_arguable BOOL,
  PRIMARY KEY (from_id, to_id, name)
);
CREATE INDEX ON v2_relations (name);
CREATE INDEX ON v2_relations (from_id);
CREATE INDEX ON v2_relations (to_id);
