SELECT
  c.name,
  r.name,
  c2.name,
  c2.id
FROM concepts c
  INNER JOIN relations r ON r.from_id = c.id
  INNER JOIN concepts c2 ON r.to_id = c2.id
WHERE c.id IN (133163);

SELECT *
FROM concepts c
WHERE NOT exists(SELECT 1
                 FROM relations
                 WHERE from_id = c.id AND name = 'ВЫШЕ');

SELECT *
FROM concepts
WHERE id IN (153471, 106562, 106768);

-- этапы процессов
WITH RECURSIVE ctree(id, name, root_id, root_name) AS (
  SELECT
    c.id,
    c.name,
    c.id   root_id,
    c.name root_name
  FROM concepts c
  WHERE c.id IN (153471)
  UNION
  SELECT
    c.id,
    c.name,
    ct.root_id,
    ct.root_name
  FROM concepts c
    INNER JOIN relations r ON r.from_id = c.id
    INNER JOIN ctree ct ON ct.id = r.to_id
  WHERE r.name = 'ВЫШЕ'
), ctree2(id, name, root_id, root_name) AS (
  SELECT
    c.id,
    c.name,
    c.id   root_id,
    c.name root_name
  FROM concepts c
  WHERE c.id IN (106562)
  UNION
  SELECT
    c.id,
    c.name,
    ct.root_id,
    ct.root_name
  FROM concepts c
    INNER JOIN relations r ON r.from_id = c.id
    INNER JOIN ctree2 ct ON ct.id = r.to_id
  WHERE r.name = 'ВЫШЕ'
), ctree3 (id, name) AS (
  SELECT
    id,
    name
  FROM ctree
  INTERSECT
  SELECT
    id,
    name
  FROM ctree2
)

SELECT
  DISTINCT
  c1.id,
  c1.name,
  c2.id,
  c2.name
FROM relations r
  INNER JOIN ctree3 c1 ON c1.id = r.from_id
  INNER JOIN concepts c2 ON c2.id = r.to_id
WHERE r.name = 'ЧАСТЬ';

SELECT *
FROM concepts
WHERE name LIKE 'СВОЙСТВО, ХАРАКТЕРИСТИКА';
SELECT *
FROM concepts
WHERE id IN (106562);


WITH RECURSIVE ctree(l, id, name, parent_id, parent_name) AS (
  SELECT
    0      l,
    c.id,
    c.name,
    c.id   parent_id,
    c.name parent_name
  FROM concepts c
  WHERE c.id IN (153471)
  UNION
  SELECT
    ct.l + 1,
    c.id,
    c.name,
    ct.id,
    ct.name
  FROM concepts c
    INNER JOIN relations r ON r.from_id = c.id
    INNER JOIN ctree ct ON ct.id = r.to_id
  WHERE r.name = 'ВЫШЕ'
)
SELECT *
FROM ctree
WHERE id = 125162;
-- супер запрос
WITH RECURSIVE ctree(id, name, root_id, root_name) AS (
  SELECT
    c.id,
    c.name,
    c.id   root_id,
    c.name root_name
  FROM concepts c
  WHERE c.id IN (106768)
  UNION
  SELECT
    c.id,
    c.name,
    ct.root_id,
    ct.root_name
  FROM concepts c
    INNER JOIN relations r ON r.from_id = c.id
    INNER JOIN ctree ct ON ct.id = r.to_id
  WHERE r.name = 'ВЫШЕ'
)
SELECT
  DISTINCT
  c1.id,
  c1.name,
  c2.id,
  c2.name
FROM relations r
  INNER JOIN ctree c1 ON c1.id = r.from_id
  INNER JOIN concepts c2 ON c2.id = r.to_id
WHERE r.name = 'ЦЕЛОЕ';

SELECT
  DISTINCT
  c.id,
  c.name,
  r2.name,
  c2.id,
  c2.name
FROM concepts c
  INNER JOIN relations r
    ON r.from_id = c.id
       AND r.to_id = 106562
       AND r.name = 'ВЫШЕ'
  INNER JOIN relations r2
    ON r2.to_id = c.id
       AND r2.name != 'НИЖЕ'
  --        AND (r2.name IN ('ЦЕЛОЕ', 'АСЦ1', 'ВЫШЕ') OR r2.name = 'АСЦ' AND r2.to_id < r2.from_id)
  INNER JOIN concepts c2
    ON c2.id = r2.from_id
WHERE c.id IN (20, 7001, 7064);
--
SELECT *
FROM concepts
WHERE name LIKE 'СВОЙСТВО%';
--
SELECT
  r.name,
  c.id,
  c.name
FROM concepts c INNER JOIN relations r ON r.to_id = c.id AND r.from_id = 106562 AND r.name = 'НИЖЕ'
ORDER BY c.name;
--
SELECT
  r.name,
  c.id,
  c.name
FROM concepts c INNER JOIN relations r ON r.to_id = c.id AND r.from_id = 50 AND r.name = 'ЧАСТЬ'
ORDER BY c.name;
--
SELECT
  c2.name,
  c.name
FROM concepts c
  INNER JOIN relations r ON r.from_id = c.id AND r.name = 'ЦЕЛОЕ'
  INNER JOIN relations r2 ON r2.from_id = r.to_id AND r2.name = 'ВЫШЕ' AND r2.to_id = 106562
  INNER JOIN concepts c2 ON c2.id = r.to_id
ORDER BY c.name, c2.name;
--

SELECT *
FROM text_entry_new
WHERE synt_type IN ('Prep', 'Pron');

SELECT
  s.synt_type,
  count(1)
FROM (
       SELECT
         t.synt_type,
         c.id
       FROM text_entry t
         INNER JOIN synonyms s ON s.entry_id = t.id
         INNER JOIN concepts c ON c.id = s.concept_id
       GROUP BY t.synt_type, c.id
     ) s
GROUP BY s.synt_type;

SELECT count(*)
FROM text_entry;

SELECT count(1)
FROM relations
WHERE name IN ('ВЫШЕ', 'ЧАСТЬ');

SELECT *
FROM text_entry
WHERE id = 142260;

SELECT synt_type
FROM text_entry
GROUP BY synt_type
ORDER BY synt_type;
--
DELETE FROM synonyms s
WHERE NOT exists(
    SELECT 1
    FROM concepts c
    WHERE c.id = s.concept_id
)
      OR NOT exists(
    SELECT 1
    FROM text_entry t
    WHERE t.id = s.entry_id
);
--
SELECT s.*
FROM synonyms s
  LEFT JOIN concepts c
    ON c.id = s.concept_id
  LEFT JOIN text_entry t
    ON t.id = s.entry_id
WHERE c.id IS NULL OR t.id IS NULL;
--
SELECT
  --   c1.id,
  c1.name,
  r.name,
  --   c2.id,
  c2.name
FROM relations r
  JOIN concepts c1
    ON c1.id = r.from_id
  JOIN concepts c2
    ON c2.id = r.to_id
WHERE r.asp = 'add_part';
--
SELECT
  c.name,
  t.name
FROM synonyms s
  INNER JOIN concepts c
    ON c.id = s.concept_id
  INNER JOIN text_entry t
    ON t.id = s.entry_id
WHERE c.id IN (130894, 130893);

SELECT
  c.name,
  c.domain,
  r.name
FROM relations r
  JOIN concepts c ON c.id = from_id
WHERE to_id = 104850;


SELECT *
FROM relations
WHERE name = 'ЦЕЛОЕ';
DELETE FROM relations
WHERE name = 'АНТ';


SELECT
  c.id   c_id,
  c.name c_name,
  c.gloss,
  t.id,
  t.name,
  t.synt_type,
  array_remove(
      array_agg(DISTINCT s2.concept_id),
      NULL
  )      concept_ids,
  array_remove(
      array_agg(DISTINCT s3.entry_id),
      NULL
  )      entry_ids
FROM synonyms s
  LEFT JOIN text_entry t
    ON t.id = s.entry_id
  --AND t.synt_type IN %(types)s
  LEFT JOIN synonyms s2
    ON s2.entry_id = t.id
  INNER JOIN synonyms s3
    ON s3.concept_id = s.concept_id
  INNER JOIN concepts c
    ON c.id = s.concept_id
GROUP BY t.id, c.id
ORDER BY t.name NULLS FIRST;

SELECT DISTINCT c.*
FROM synonyms s
  INNER JOIN concepts c ON c.id = s.concept_id
WHERE NOT exists(SELECT 1
                 FROM text_entry t
                 WHERE t.id = s.entry_id);

SELECT DISTINCT t.*
FROM synonyms s
  INNER JOIN text_entry t ON t.id = s.entry_id
WHERE NOT exists(SELECT 1
                 FROM concepts c
                 WHERE c.id = s.concept_id);

SELECT DISTINCT
  name,
  asp
FROM relations;

UPDATE relations
SET asp = ''
WHERE asp IS NULL;

UPDATE relations
SET name = 'ЧАСТЬ'
WHERE asp = 'process_steps';

DELETE FROM relations
WHERE asp = 'process_steps';

SELECT *
FROM relations
WHERE asp = 'process_steps';

SELECT *
FROM relations r
WHERE NOT exists(SELECT 1
                 FROM concepts c
                 WHERE c.id = r.from_id)
      OR NOT exists(SELECT 1
                    FROM concepts c
                    WHERE c.id = r.from_id);

INSERT INTO relations (from_id, to_id, name, asp)
  (SELECT
     to_id,
     from_id,
     'КЛАСС',
     asp
   FROM relations r
   WHERE name = 'ЭКЗЕМПЛЯР'
         AND NOT exists(
       SELECT 1
       FROM relations
       WHERE from_id = r.to_id
             AND to_id = r.from_id
             AND name = 'КЛАСС')
  );

UPDATE relations r
SET asp = (
  SELECT asp
  FROM relations
  WHERE from_id = r.to_id
        AND to_id = r.from_id
        AND asp != ''
        AND name = 'ЧАСТЬ'
);
SELECT
  *,
  (SELECT asp
   FROM relations
   WHERE from_id = r.to_id AND to_id = r.from_id AND asp != '' AND name = 'ЧАСТЬ') asp
FROM relations r
WHERE name = 'ЦЕЛОЕ'
      --       AND asp = 'В'
      AND exists(
          SELECT 1
          FROM relations
          WHERE from_id = r.to_id
                AND to_id = r.from_id
                AND asp != r.asp
                --                 AND asp != 'В'
                AND name = 'ЧАСТЬ'
      );

UPDATE relations
SET asp = ''
WHERE asp IS NULL;
SELECT *
FROM relations
WHERE asp IS NULL;


SELECT
  --   c1.id,
  r.asp,
  c1.name,
  r.name,
  --   c2.id,
  c2.name
FROM relations r
  JOIN concepts c1
    ON c1.id = r.from_id
  JOIN concepts c2
    ON c2.id = r.to_id
WHERE r.name = 'ЧАСТЬ' AND exists(SELECT 1
                                  FROM relations
                                  WHERE from_id = r.to_id
                                        AND to_id = r.from_id AND asp != r.asp AND name = 'ЦЕЛОЕ');

SELECT *
FROM relations r1
WHERE r1.name IN ('ЧАСТЬ', 'ЦЕЛОЕ', 'КЛАСС', 'ЭКЗЕМПЛЯР')
      AND NOT exists(
    SELECT 1
    FROM relations r2
    WHERE r2.from_id = r1.to_id AND r2.to_id = r1.from_id
          AND (
            r1.name IN ('ЧАСТЬ', 'ЦЕЛОЕ') AND
            r2.name IN ('ЧАСТЬ', 'ЦЕЛОЕ') AND
            r1.name != r2.name
            OR
            r1.name IN ('КЛАСС', 'ЭКЗЕМПЛЯР') AND
            r2.name IN ('КЛАСС', 'ЭКЗЕМПЛЯР') AND
            r1.name != r2.name

          )
);

SELECT
  sum((asp = 'add_part') :: INT),
  sum((name = 'АНТОНИМ') :: INT),
  sum((name IN ('КЛАСС', 'ЭКЗЕМПЛЯР')) :: INT),
  sum((asp = 'classical_meronymy') :: INT),
  sum((asp = 'process_steps') :: INT)
FROM relations;


SELECT
  sum((name = 'ЧАСТЬ') :: INT),
  sum((name = 'ЦЕЛОЕ') :: INT)
FROM relations
WHERE asp = 'add_part';

SELECT
  concept_id,
  count(entry_id) cnt,
  array_agg(t.lemma)
FROM synonyms s
  INNER JOIN text_entry t ON t.id = s.entry_id
WHERE concept_id = 117545
GROUP BY concept_id
ORDER BY cnt DESC;

SELECT *
FROM concepts
WHERE name = 'ЗАВЕСТИ МЕХАНИЗМ, УСТРОЙСТВО';

DELETE FROM relations
WHERE ctid IN (
  SELECT ctid
  FROM relations r1
  WHERE r1.name = 'ВЫШЕ' AND exists(
      SELECT *
      FROM relations r2
      WHERE r2.from_id = r1.from_id
            AND r2.to_id = r1.to_id
            AND r2.name = 'КЛАСС'
  ));

SELECT DISTINCT
  name,
  asp
FROM relations;
SELECT
  asp,
  count(1)
FROM relations
GROUP BY asp;

SELECT *
FROM concepts
WHERE name LIKE 'ДЕЙСТВИЕ%';

SELECT DISTINCT part_of_speech
FROM synsets;

SELECT
  synsets.*,
  array_agg(senses.id) senses
FROM synsets
  INNER JOIN senses
    ON synsets.id = senses.synset_id
GROUP BY synsets.id
ORDER BY part_of_speech;

SELECT
  (SELECT name
   FROM concepts
   WHERE id = r1.from_id),
  (SELECT name
   FROM concepts
   WHERE id = r1.to_id),
  r1.name,
  r2.name,
  r1.asp,
  r2.asp
FROM relations r1
  INNER JOIN relations r2
    ON r2.to_id = r1.from_id
       AND r2.from_id = r1.to_id
WHERE r1.asp != r2.asp AND r1.to_id > r1.from_id;


SELECT
  c1.name,
  r.name,
  c2.name,
  r.asp
FROM concepts c1
  INNER JOIN relations r
    ON r.from_id = c1.id
  INNER JOIN concepts c2
    ON c2.id = r.to_id
WHERE c1.name = 'СУТКИ';


SELECT
  (SELECT name
   FROM concepts
   WHERE id = r1.from_id),
  (SELECT name
   FROM concepts
   WHERE id = r1.to_id),
  r1.name,
  r2.name,
  r1.asp,
  r2.asp
FROM relations r1
  INNER JOIN relations r2
    ON r2.from_id = r1.to_id
       AND r2.to_id = r1.from_id
       AND r2.asp != r1.asp
WHERE ARRAY [r1.name, r2.name] <@ ARRAY ['ЧАСТЬ', 'ЦЕЛОЕ'] OR ARRAY [r1.name, r2.name] <@ ARRAY ['КЛАСС', 'ЭКЗЕМПЛЯР'];