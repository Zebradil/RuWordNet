-- отношения, в которых участвуют несуществующие понятия
SELECT r.*
FROM relations r
WHERE
   NOT EXISTS(SELECT 1 FROM concepts WHERE ID = r.from_id)
   OR
   NOT EXISTS(SELECT 1 FROM concepts WHERE ID = r.to_id);

-- неконсистентные связи между понятиями и текстовыми входами
SELECT
  c.id "concept.id",
  s.*,
  t.id "text_entry.id"
FROM synonyms s
  LEFT JOIN text_entry t ON t.id = s.entry_id
  LEFT JOIN concepts c ON c.id = s.concept_id
WHERE c.id IS NULL OR t.id IS NULL;

DELETE FROM synonyms s
WHERE NOT EXISTS(
    SELECT 1
    FROM concepts c
    WHERE c.id = s.concept_id
)
      OR NOT EXISTS(
    SELECT 1
    FROM text_entry t
    WHERE t.id = s.entry_id
);

-- текстовые входы, не связанные ни с одним понятием
SELECT t.*
FROM text_entry t
 WHERE NOT EXISTS(
   SELECT 1
     FROM synonyms
    WHERE entry_id = t.ID
      AND concept_id IS NOT NULL
 );

-- понятия без текстовых входов
SELECT c.*
FROM concepts c
WHERE NOT EXISTS(
  SELECT 1
    FROM synonyms
   WHERE entry_id IS NOT NULL
     AND concept_id = c.id
);
