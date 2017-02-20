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