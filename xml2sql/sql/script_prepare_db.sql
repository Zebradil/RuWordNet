DROP TABLE IF EXISTS synonyms;
DROP TABLE IF EXISTS text_entry;
DROP TABLE IF EXISTS relations;
DROP TABLE IF EXISTS concepts;

CREATE TABLE concepts (
  id INTEGER,
  name TEXT,
  gloss TEXT,
  domain TEXT
);

CREATE TABLE relations (
  from_id INTEGER, -- REFERENCES concepts (id),
  to_id INTEGER, -- REFERENCES concepts (id),
  name TEXT,
  asp TEXT
);

CREATE TABLE text_entry (
  id INTEGER,
  name TEXT,
  lemma TEXT,
  main_word TEXT,
  synt_type TEXT,
  pos_string TEXT
);

CREATE TABLE synonyms (
  concept_id INTEGER, -- REFERENCES concepts (id),
  entry_id INTEGER -- REFERENCES text_entry (id)
);