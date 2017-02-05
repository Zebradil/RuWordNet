-- отношения, в которых участвуют несуществующие понятия
SELECT r.*
FROM relations r
  LEFT JOIN concepts cf ON cf.id = r.from_id
  LEFT JOIN concepts ct ON ct.id = r.to_id
WHERE cf.id IS NULL OR ct.id IS NULL;

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
  LEFT JOIN synonyms s ON s.entry_id = t.id
WHERE s.concept_id IS NULL;

-- понятия без текстовых входов
SELECT c.*
FROM concepts c
  LEFT JOIN synonyms s ON s.concept_id = c.id
WHERE s.entry_id IS NULL;