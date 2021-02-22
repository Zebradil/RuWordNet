SELECT se.id, sy.name concept_name, se.name
  FROM synsets sy
         JOIN senses se ON se.synset_id = sy.id
         JOIN concepts c ON c.name = sy.name
         LEFT JOIN ili ON ili.concept_id = c.id
         LEFT JOIN frequent_words fr ON fr.word = se.name
 WHERE ili.concept_id IS NULL
 ORDER BY fr.position NULLS LAST;
