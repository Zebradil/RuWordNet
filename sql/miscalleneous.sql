-- Получает список коренвых понятий
SELECT *
FROM concepts c
WHERE NOT exists(SELECT 1
                 FROM relations
                 WHERE from_id = c.id AND name = 'ВЫШЕ');

-- Извлечение этапов процессов
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

-- 1. Сторится дерево отношений ВЫШЕ-НИЖЕ от корневого понятие «СВОЙСТВО, ХАРАКТЕРИСТИКА».
-- 2. Выбираются понятия отношений ЧАСТЬ-ЦЕЛОЕ с частью в вышеуказанном дереве.
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

-- Посчёт количества понятий, имеющих определённый синтаксический тип
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

-- Отношения, для которых отсутствуют обратные
SELECT *
FROM relations r1
WHERE r1.name IN ('ЧАСТЬ', 'ЦЕЛОЕ', 'КЛАСС', 'ЭКЗЕМПЛЯР', 'ВЫШЕ', 'НИЖЕ')
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
            OR
            r1.name IN ('ВЫШЕ', 'НИЖЕ') AND
            r2.name IN ('ВЫШЕ', 'НИЖЕ') AND
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

SELECT
  synsets.*,
  array_agg(senses.id) senses
FROM synsets
  INNER JOIN senses
    ON synsets.id = senses.synset_id
GROUP BY synsets.id
ORDER BY part_of_speech;

-- Получение реверсивных отношений, у которых не совпадают значения поля asp
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

-- Получение реверсивных отношений, у которых не совпадают значения поля asp
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

-- Выяснение различий среди отношений между различными версиями RWN
SELECT
    COALESCE(new.relation_name, old.relation_name) relation_name,
    new.parent_pos  new_parent_pos,
    new.parent_name new_parent_name,
    new.child_pos   new_child_pos,
    new.child_name  new_child_name,
    old.parent_pos  old_parent_pos,
    old.parent_name old_parent_name,
    old.child_pos   old_child_pos,
    old.child_name  old_child_name
FROM (
    SELECT
        parent.name      parent_name,
        parent.synt_type parent_pos,
        sr.name          relation_name,
        child.synt_type  child_pos,
        child.name       child_name
    FROM public.sense_relations sr
      JOIN public.senses parent ON parent.id = sr.parent_id
      JOIN public.senses child  ON child.id  = sr.child_id
    UNION ALL
    SELECT
        parent.name           parent_name,
        parent.part_of_speech parent_pos,
        sr.name               relation_name,
        child.part_of_speech  child_pos,
        child.name            child_name
    FROM public.synset_relations sr
      JOIN public.synsets parent ON parent.id = sr.parent_id
      JOIN public.synsets child  ON child.id  = sr.child_id
) new
FULL OUTER JOIN (
    SELECT
        parent.name      parent_name,
        parent.synt_type parent_pos,
        sr.name          relation_name,
        child.synt_type  child_pos,
        child.name       child_name
    FROM published.sense_relations sr
      JOIN published.senses parent ON parent.id = sr.parent_id
      JOIN published.senses child  ON child.id  = sr.child_id
    UNION ALL
    SELECT
        parent.name           parent_name,
        parent.part_of_speech parent_pos,
        sr.name               relation_name,
        child.part_of_speech  child_pos,
        child.name            child_name
    FROM published.synset_relations sr
      JOIN published.synsets parent ON parent.id = sr.parent_id
      JOIN published.synsets child  ON child.id  = sr.child_id
) old
   ON old.parent_name   = new.parent_name
  AND old.child_name    = new.child_name
  AND old.relation_name = new.relation_name
WHERE new IS NULL OR old IS NULL
ORDER BY relation_name,
    new.parent_pos  ,
    new.parent_name ,
    new.child_pos   ,
    new.child_name  ,
    old.parent_pos  ,
    old.parent_name ,
    old.child_pos   ,
    old.child_name  ;
