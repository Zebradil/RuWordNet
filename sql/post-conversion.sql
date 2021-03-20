SELECT 'замена отношений ЦЕЛОЕ → entailment для глаголов';
UPDATE synset_relations sr
   SET name = 'entailment'
 WHERE name = 'part holonym'
   AND NOT EXISTS (
     SELECT 1
       FROM synset_relations
      WHERE parent_id = sr.parent_id
        AND child_id = sr.child_id
        AND name = 'entailment'
   )
   AND EXISTS (
     SELECT 1
       FROM synsets
      WHERE id = sr.parent_id AND part_of_speech = 'V'
   );

SELECT 'удаление оставшихся отношений ЦЕЛОЕ для глаголов';
DELETE FROM synset_relations sr
WHERE name = 'part holonym'
      AND exists(SELECT *
                 FROM synsets
                 WHERE id = sr.parent_id AND part_of_speech = 'V');

SELECT 'удаление отношений ЧАСТЬ для глаголов';
DELETE FROM synset_relations sr
WHERE name = 'part meronym'
      AND exists(SELECT *
                 FROM synsets
                 WHERE id = sr.parent_id AND part_of_speech = 'V');

SELECT 'Удаление отношений ВЫШЕ-НИЖЕ, если имеется отношение ЭКЗЕМПЛЯР-КЛАСС';
DELETE
FROM synset_relations sr1
WHERE name IN ('hypernym', 'hyponym')
      AND exists(
          SELECT *
          FROM synset_relations sr2
          WHERE sr2.parent_id = sr1.parent_id
                AND sr2.child_id = sr1.child_id
                AND sr2.name IN ('instance hypernym', 'instance hyponym')
      );

SELECT 'Импорт отношений domain из РуТез в RuWordNet';
INSERT INTO synset_relations (parent_id, name, child_id)
  SELECT
    s1.id    AS parent_id,
    'domain' AS name,
    s2.id    AS child_id
  FROM relations r
    JOIN concepts c1
      ON c1.id = r.from_id
    JOIN synsets s1
      ON s1.name = c1.name
    JOIN concepts c2
      ON c2.id = r.to_id
    JOIN synsets s2
      ON s2.name = c2.name
  WHERE r.name = 'ДОМЕН'
    AND s2.part_of_speech = 'N'
ON CONFLICT DO NOTHING;
