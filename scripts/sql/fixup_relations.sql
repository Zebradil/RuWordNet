-- Удаление отношений, в которых участвуют несуществующие понятия.
DELETE FROM relations
WHERE (from_id, to_id, name) IN (
  SELECT
    r.from_id,
    r.to_id,
    r.name
  FROM relations r
    LEFT JOIN concepts cf ON cf.id = r.from_id
    LEFT JOIN concepts ct ON ct.id = r.to_id
  WHERE cf.id IS NULL OR ct.id IS NULL
);

-- Добавление обратных отношений.
-- Необходимо выполнить для всех пар (по два раза на каждую):
-- ВЫШЕ-НИЖЕ
-- ЧАСТЬ-ЦЕЛОЕ
-- ЭКЗЕМПЛЯР-КЛАСС
INSERT INTO relations (from_id, to_id, name, asp)
  (SELECT
     to_id,
     from_id,
     :direct_relation_name,
     asp
   FROM relations r
   WHERE name = :reverse_relation_name
         AND NOT exists(
       SELECT 1
       FROM relations
       WHERE from_id = r.to_id
             AND to_id = r.from_id
             AND name = :direct_relation_name)
  );

-- Выявление различий среди значений поля asp для реверсивных отношений
SELECT
  r1.*,
  r2.name,
  r2.asp
FROM relations r1
  INNER JOIN relations r2
    ON r2.to_id = r1.from_id
       AND r2.from_id = r1.to_id
       AND r2.name != r1.name
       AND r2.asp != r1.asp
       AND r2.to_id > r1.to_id
WHERE ARRAY [r1.name, r2.name] <@ ARRAY ['ЧАСТЬ', 'ЦЕЛОЕ']
      OR ARRAY [r1.name, r2.name] <@ ARRAY ['КЛАСС', 'ЭКЗЕМПЛЯР']
      OR ARRAY [r1.name, r2.name] <@ ARRAY ['ВЫШЕ', 'НИЖЕ'];

-- Исправление различий
-- (заполняются asp со значениями, отличными от ['add_part', 'classical_meronymy', 'process_steps'])
WITH target_relations AS (
    SELECT
      r1.to_id,
      r1.from_id,
      r1.name,
      r2.asp
    FROM relations r1
      INNER JOIN relations r2
        ON r2.to_id = r1.from_id
           AND r2.from_id = r1.to_id
           AND r2.name != r1.name
           AND r2.asp != r1.asp
           AND r2.asp IN ('add_part', 'classical_meronymy', 'process_steps')
           AND r1.asp NOT IN ('add_part', 'classical_meronymy', 'process_steps')
    WHERE ARRAY [r1.name, r2.name] <@ ARRAY ['ЧАСТЬ', 'ЦЕЛОЕ']
          OR ARRAY [r1.name, r2.name] <@ ARRAY ['КЛАСС', 'ЭКЗЕМПЛЯР']
          OR ARRAY [r1.name, r2.name] <@ ARRAY ['ВЫШЕ', 'НИЖЕ']
)

UPDATE relations r
SET asp = (
  SELECT asp
  FROM target_relations
  WHERE r.from_id = target_relations.from_id
        AND r.to_id = target_relations.to_id
        AND r.name = target_relations.name
)
WHERE ROW (r.from_id, r.to_id, r.name)
      IN (
        SELECT
          from_id,
          to_id,
          name
        FROM target_relations
      );

-- Очистка поля asp
UPDATE relations
SET asp = ''
WHERE asp IS NULL;