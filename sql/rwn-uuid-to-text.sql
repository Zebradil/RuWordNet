ALTER TABLE sense_relations DROP CONSTRAINT "sense_relations_pkey";
ALTER TABLE sense_relations DROP CONSTRAINT "sense_relations_child_id_fkey";
ALTER TABLE sense_relations DROP CONSTRAINT "sense_relations_parent_id_fkey";

ALTER TABLE sense_relations RENAME child_id TO child_id_uuid;
ALTER TABLE sense_relations RENAME parent_id TO parent_id_uuid;

ALTER TABLE sense_relations ADD child_id TEXT;
ALTER TABLE sense_relations ADD parent_id TEXT;


ALTER TABLE synset_relations DROP CONSTRAINT "synset_relations_pkey";
ALTER TABLE synset_relations DROP CONSTRAINT "synset_relations_child_id_fkey";
ALTER TABLE synset_relations DROP CONSTRAINT "synset_relations_parent_id_fkey";

ALTER TABLE synset_relations RENAME child_id TO child_id_uuid;
ALTER TABLE synset_relations RENAME parent_id TO parent_id_uuid;

ALTER TABLE synset_relations ADD child_id TEXT;
ALTER TABLE synset_relations ADD parent_id TEXT;


ALTER TABLE senses DROP CONSTRAINT "senses_pkey";
ALTER TABLE senses DROP CONSTRAINT "senses_synset_id_fkey";

ALTER TABLE senses RENAME ID TO id_uuid;
ALTER TABLE senses RENAME synset_id TO synset_id_uuid;

ALTER TABLE senses ADD ID TEXT;
ALTER TABLE senses ADD synset_id TEXT;


ALTER TABLE synsets DROP CONSTRAINT "synsets_pkey";

ALTER TABLE synsets RENAME ID TO id_uuid;

ALTER TABLE synsets ADD ID TEXT;



UPDATE synsets s
   SET ID = c.ID || '-' || LEFT(s.part_of_speech, 1)
       FROM concepts c
 WHERE c.name = s.name;

UPDATE senses se
   SET synset_id = sy.id
       FROM synsets sy
 WHERE sy.id_uuid = se.synset_id_uuid;

UPDATE senses s
   SET ID = synset_id || '-' || t.id
       FROM text_entry T
 WHERE t.name = s.name;

UPDATE synset_relations sr
   SET child_id = sy.ID
       FROM synsets sy
 WHERE sy.id_uuid = sr.child_id_uuid;

UPDATE synset_relations sr
   SET parent_id = sy.ID
       FROM synsets sy
 WHERE sy.id_uuid = sr.parent_id_uuid;

UPDATE sense_relations sr
   SET child_id = sy.ID
       FROM senses sy
 WHERE sy.id_uuid = sr.child_id_uuid;

UPDATE sense_relations sr
  SET parent_id = sy.ID
      FROM senses sy
 WHERE sy.id_uuid = sr.parent_id_uuid;



ALTER TABLE synsets ADD PRIMARY KEY (id);

ALTER TABLE senses ADD PRIMARY KEY (id);
ALTER TABLE senses ADD CONSTRAINT "senses_synset_id_fkey" FOREIGN KEY (synset_id) REFERENCES synsets(id);

ALTER TABLE synset_relations ADD PRIMARY KEY(parent_id, child_id, name);
ALTER TABLE synset_relations ADD CONSTRAINT "synset_relations_child_id_fkey" FOREIGN KEY (child_id) REFERENCES synsets(id);
ALTER TABLE synset_relations ADD CONSTRAINT "synset_relations_parent_id_fkey" FOREIGN KEY (parent_id) REFERENCES synsets(id);

ALTER TABLE sense_relations ADD PRIMARY KEY (parent_id, child_id, name);
ALTER TABLE sense_relations ADD CONSTRAINT "sense_relations_child_id_fkey" FOREIGN KEY (child_id) REFERENCES senses(id);
ALTER TABLE sense_relations ADD CONSTRAINT "sense_relations_parent_id_fkey" FOREIGN KEY (parent_id) REFERENCES senses(id);
