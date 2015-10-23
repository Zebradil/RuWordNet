SELECT *
FROM text_entry_new
WHERE synt_type IN ('10', '20');
--
SELECT *
FROM text_entry_new
WHERE main_word IN ('10', '20');
--
SELECT
  name,
  count(id) cnt
FROM text_entry_new
GROUP BY name
ORDER BY cnt DESC;
--
SELECT *
FROM text_entry_new
WHERE id IN (752483, 752482, 750972);
--
SELECT *
FROM text_entry_new
WHERE name = 'НЕНАСИЛЬСТВЕННЫЙ';
--
SELECT *
FROM text_entry_new
WHERE synt_type NOT IN (
  'Adj',
  'AdjG',
  'AdjGprep',
  'Adv',
  'AdvG',
  'Conj',
  'Misc',
  'Num',
  'NumG',
  'NGPrep',
  'Prdc',
  'N', 'NG', 'NGprep', 'Prep', 'PrepG', 'Pron', 'Prtc', 'V', 'VG', 'VGprep'
);
--
SELECT *
FROM text_entry_new t
  LEFT JOIN synonyms s
    ON s.entry_id = t.id
WHERE s.concept_id IS NULL;
--
SELECT *
FROM text_entry_new
WHERE
  array_length(regexp_split_to_array(pos_string, '\s+'), 1) <> array_length(regexp_split_to_array(lemma, '\s+'), 1);
--
SELECT *
FROM text_entry_new
WHERE
  array_length(regexp_split_to_array(name, '\s+-*\s*'), 1) <> array_length(regexp_split_to_array(lemma, '\s+'), 1);
--
SELECT *
FROM text_entry_new
WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) > 1
      AND main_word = '';
--
SELECT
  lemma,
  array_agg(name),
  count(1) cnt
FROM text_entry_new
GROUP BY lemma
ORDER BY cnt DESC;
--
SELECT
  t.id,
  t.name
FROM text_entry t
WHERE NOT EXISTS(
    SELECT 1
    FROM synonyms s
      INNER JOIN concepts c
        ON c.id = s.concept_id
    WHERE s.entry_id = t.id
          AND c.id IS NOT NULL
);
--
SELECT
  c.id,
  c.name
FROM concepts c
WHERE NOT EXISTS(
    SELECT 1
    FROM synonyms s
      INNER JOIN text_entry t
        ON t.id = s.entry_id
    WHERE s.concept_id = c.id
          AND t.id IS NOT NULL
);
