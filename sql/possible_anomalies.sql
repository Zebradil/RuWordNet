-- В версии xml-файлов РуТез, с которой я работал первый раз, были ошибочно заполнены некоторые свойства.
-- В новых версиях это исправлено, но проверку всё же оставлю.
SELECT *
FROM text_entry
WHERE synt_type IN ('10', '20') OR main_word IN ('10', '20');

-- Обнаружение повторяющихся текстовых входов
SELECT
  name,
  count(id) cnt
FROM text_entry
GROUP BY name
HAVING count(id) > 1;

-- Обнаружение незарегистрированных синтаксических типов
SELECT *
FROM text_entry
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

-- Обнаружение текстовых входов, не привязанных ни к одному понятию
SELECT *
FROM text_entry t
  LEFT JOIN synonyms s
    ON s.entry_id = t.id
  LEFT JOIN concepts c
    ON c.id = s.concept_id
WHERE c.id IS NULL;

-- Обнаружение понятий, не имеющих текстовых входов
SELECT *
FROM concepts c
  LEFT JOIN synonyms s
    ON s.concept_id = c.id
  LEFT JOIN text_entry t
    ON t.id = s.entry_id
WHERE t.id IS NULL;

-- Несоответствие количества элементов в поле lemma количеству элементов в поле pos_string
SELECT *
FROM text_entry
WHERE
  array_length(regexp_split_to_array(pos_string, '\s+'), 1) <> array_length(regexp_split_to_array(lemma, '\s+'), 1);

-- Несоответствие количества элементов в поле lemma количеству элементов в поле name
SELECT *
FROM text_entry
WHERE
  array_length(regexp_split_to_array(name, '\s+-*\s*'), 1) <> array_length(regexp_split_to_array(lemma, '\s+'), 1);

-- Не указано главное слово для текстовых входов, состоящих из нескольких слов
SELECT *
FROM text_entry
WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) > 1
      AND main_word = '';

-- Текстовые входы с одинаковым полем lemma
SELECT
  lemma,
  array_agg(name),
  count(1) cnt
FROM text_entry
GROUP BY lemma
HAVING count(1) > 1;

-- Отсутствие реверсивных ассциативных связей
SELECT
    *
FROM
    relations AS r
WHERE
    name = 'АСЦ'
    AND NOT
            EXISTS(
                SELECT
                    1
                FROM
                    relations
                WHERE
                    from_id = r.to_id
                    AND to_id = r.from_id
                    AND name = 'АСЦ'
            );
