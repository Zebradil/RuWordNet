-- Исправление synt_type, который был записан в pos_string. Актуально для записей с synt_type = 10/20.
UPDATE text_entry
SET main_word = NULL, synt_type = pos_string, pos_string = NULL
WHERE synt_type IN ('10', '20');

-- Очистка полей с некорректными значениями. Актуально для записей с main_word = 10/20.
UPDATE text_entry
SET main_word = NULL, synt_type = NULL, pos_string = NULL
WHERE main_word IN ('10', '20');

-- Удаление дублирующегося текстового входа "СТАЖЕРКА".
UPDATE synonyms
SET entry_id = 191728
WHERE entry_id = 750968;

DELETE FROM text_entry
WHERE id = 750968;

-- Исправление текстовых входов "СРЕДИНА" и "ФОНОВЫЙ".
UPDATE text_entry
SET lemma = main_word, synt_type = pos_string
WHERE id IN (752483, 752482);

-- Исправление текстового входа "НЕНАСИЛЬСТВЕННЫЙ".
UPDATE text_entry
SET lemma = name, main_word = name, synt_type = NULL, pos_string = NULL
WHERE id = 750972;

-- Исправление текстовых входов, в которых перепутаны местами main_word и pos_string.
-- Актуально для оставшихся после предыдущих преобразований записей с ошибочным synt_type.
UPDATE text_entry
SET main_word = pos_string, synt_type = NULL, pos_string = main_word
WHERE synt_type NOT IN
      ('Adj', 'AdjG', 'AdjGprep', 'Adv', 'AdvG', 'Conj', 'Misc', 'N', 'NG', 'NGprep', 'Prep', 'PrepG', 'Pron', 'Prtc', 'V', 'VG', 'VGprep')
      AND synt_type IS NOT NULL;

-- Копирование main_word из lemma/name в случае однословных текстовых входов.
-- ...