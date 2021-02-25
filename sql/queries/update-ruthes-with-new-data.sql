-- Insert new text entries connected to the old concepts
insert into text_entry
    (id, name, lemma, main_word, synt_type, pos_string, version)
select
    t2.id, t2.name, t2.lemma, t2.main_word, t2.synt_type, t2.pos_string, 'v2 old concepts'
from v2_text_entry t2
join v2_synonyms s2 on s2.entry_id = t2.id
join concepts c on c.id = s2.concept_id
where synt_type <> ''
  and s2.concept_id != 0
  and c.version = 'initial'
on conflict do nothing;

-- Insert new synonyms to connect new text entries to old synsets
insert into synonyms
    (concept_id, entry_id, version)
select
    s2.concept_id, s2.entry_id, 'v2 old concepts'
from v2_text_entry t2
join v2_synonyms s2 on s2.entry_id = t2.id
join concepts c on c.id = s2.concept_id
where synt_type <> ''
  and s2.concept_id != 0
  and c.version = 'initial'
on conflict do nothing;


-- Insert new concepts
insert into concepts
    (id, name, gloss, version)
select
    c2.id, c2.name, c2.gloss, 'v2'
from v2_concepts c2
on conflict do nothing;

-- Update old concepts with corrected data
UPDATE concepts c
   SET name = v2.name
       FROM v2_concepts v2
 WHERE v2.id = c.id
   AND v2.name != c.name;

UPDATE concepts c
   SET gloss = v2.gloss
       FROM v2_concepts v2
 WHERE v2.id = c.id
   AND (c.gloss IS NULL OR c.gloss = '')
   AND NOT (v2.gloss IS NULL OR v2.gloss = '');

-- Insert new text entries NOT connected to the old concepts
insert into text_entry
    (id, name, lemma, main_word, synt_type, pos_string, version)
select
    t2.id, t2.name, t2.lemma, t2.main_word, t2.synt_type, t2.pos_string, 'v2'
from v2_text_entry t2
join v2_synonyms s2 on s2.entry_id = t2.id
  -- New concepts should be already there => joining concepts instead of v2_concepts
join concepts c on c.id = s2.concept_id
where synt_type <> ''
  and s2.concept_id != 0
  and c.version = 'v2'
on conflict do nothing;

-- Insert new synonyms to connect new text entries to new synsets
insert into synonyms
            (concept_id, entry_id, version)
SELECT
  s2.concept_id, s2.entry_id, 'v2'
  from v2_text_entry t2
         join v2_synonyms s2 on s2.entry_id = t2.id
         join concepts c on c.id = s2.concept_id
 where synt_type <> ''
   and s2.concept_id != 0
   and c.version = 'v2'
       on conflict do nothing;

-- Insert new relations
INSERT INTO relations
            (from_id, to_id, name, asp, version)
SELECT
  from_id, to_id, name, asp, 'v2'
  FROM v2_relations
         ON conflict do nothing;
