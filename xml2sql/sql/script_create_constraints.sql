ALTER TABLE concepts ADD PRIMARY KEY (id);
ALTER TABLE relations ADD PRIMARY KEY (from_id, to_id, name);
ALTER TABLE text_entry ADD PRIMARY KEY (id);
ALTER TABLE synonyms ADD PRIMARY KEY (concept_id, entry_id);
