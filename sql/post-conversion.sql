-- замена отношений ЦЕЛОЕ → entailment для глаголов
UPDATE synset_relations sr
SET name = 'entailment'
WHERE name = 'part holonym'
      AND exists(SELECT *
                 FROM synsets
                 WHERE id = sr.parent_id AND part_of_speech = 'V');

-- удаление отношений ЧАСТЬ для глаголов
DELETE FROM synset_relations sr
WHERE name = 'part meronym'
      AND exists(SELECT *
                 FROM synsets
                 WHERE id = sr.parent_id AND part_of_speech = 'V');

-- Удаление отношений ВЫШЕ-НИЖЕ, если имеется отношение ЭКЗЕМПЛЯР-КЛАСС
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