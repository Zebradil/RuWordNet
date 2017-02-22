# Общие требования

Для работы потребуются:
- python3 (пекеты: psycopg2, lxml, argparse, os, re, csv, getopt, sys, uuid),
- postgresql9.6 (9.5 тоже должна подойти)

# Инструкция

## Общий порядок действий

0. [Генерация xml-файлов РуТез из текстовых файлов](#zero)
- [Подготовка базы данных](#one)
- [Импорт данных РуТез из xml-файлов](#two)
- [Импорт дополнительных отношений](#three)
- [Конвертация данных РуТез → RuWordNet](#four)
- [Импорт отношений причины и следствия](#five)
- [Извлечение отношений `derived_from` и `composed_of`](#six)
- [Генерация xml-файлов RuWordNet.](#seven)


## <a name="zero"></a>Порождение xml-файлов РуТез из исходных текстовых файлов

Запустите скрипты из директории `raw2xml`:
- `concepts.py`
- `relations.py`
- `synonyms.py`
- `text_entries.py`

Каждый скрипт генерирует соответствующий xml-файл из указанного txt-файла.
Предполагается, что txt-файлы записаны в кодировке Windows-1251.


## <a name="one"></a>Подготовка базы данных

Используется Postgresql 9.6, кодировка utf-8.
Необходимо создать новую базу данных, используя sql-скрипт `sql/prepare_database.sql`.
Он создаст таблицы для данных РуТез и RuWordNet:

**Таблицы для РуТез:**
- concepts
- relations
- text_entry
- synonyms

**Таблицы для RuWordNet:**
- sense_relations
- synset_relations
- relation_types
- senses
- synsets

## <a name="two"></a>Загрузка данных из xml-файлов РуТез

### Загрузка данных

Используйте скрипт `xml2sql/xml2sql.py`:

```
usage: xml2sql.py [-h] [-s XML_DIR] [-l LOG_DIR] [-c CONNECTION_STRING]

Run import RuThes from xml to database.

optional arguments:
  -h, --help            show this help message and exit
  -s XML_DIR, --xml-dir XML_DIR
                        Source xml root directory
  -l LOG_DIR, --log-dir LOG_DIR
                        Log files destination
  -c CONNECTION_STRING, --connection-string CONNECTION_STRING
                        Postgresql database connection string
                        (host='localhost' dbname='ruwordnet' user='ruwordnet'
                        password='ruwordnet')
```
Он испортирует данные из xml-файлов РуТез в базу данных.
Ожидается, что имеются следующие файлы:
- concepts.xml
- relations.xml
- text_entry.xml
- synonyms.xml

### Проверка целостности данных

Запустить запросы в файле `sql/consistency_checkings.sql`. При необходимости, удалить некончичтентные записи или
проверить исходные данные, исправить их и импортировать повторно.

### Выявление аномалий

В файле `sql/possible_anomalies.sql` записаны запросы, которые могут помочь выявить ошибки в логической структуре РуТез.
На момент публикации этой инструкции таких ошибок не было — они были исправлены на предыдущих этапах. Тем не менее,
оставляю эти хапросы для будущих проверок.


## <a name="three"></a>Импорт дополнительных отношений в РуТез

Для этого используются следующие скрипты из директории `scripts`:
- `scripts/import_antonyms.py` загружает отношения антонимии из указанного csv-файла.
Предполагается, что файл имеет следующие колонки (они соответствуют полям таблицы `concepts`): `id`, `name`, `id`, `name`.
- `scripts/import_class-instance_relations.py` загружает отношения экзепляр-класс из указанного csv-файла.
 filename = 'in/add_part.txt'
 asp = 'add_part'
 relation_name = 'ЦЕЛОЕ'
 filename = 'in/process_steps_final.txt'
 asp = 'process_steps'
 relation_name = 'ЧАСТЬ'
 filename = 'in/classical_meronymy_edited.txt'
 asp = 'classical_meronymy'
 relation_name = 'ЧАСТЬ'
- `scripts/import_meronymy_relations.py` загружает отношения часть-целое из указанного csv-файла.

Затем необходимо проверить консистентность добавленных данных.
Для этого используйте sql-запросы из файла `scripts/sql/fixup_relations.sql`

Скрипт `scripts/import_domains.py` создаёт новые отношения типа «ДОМЕН», загружая их из подготовленных файлов.


## <a name="four"></a>Порождение RuWordNet

Для конвертации РуТез в RuWordNet запустите скрипт `sql2sql/sql2sql.py`.
Затем необходимо выполнить миграции по переименованию отношений: `sql/post-conversion.sql`.


## <a name="five"></a>Импорт отношений причины и следствия

Скрипт `scripts/import_cause-entailment.py` импортирует отношения из подготовленных файлов непосредственно в RuWordNet.
Формат файлов предполагается следующий. Синсеты идут парами, каждый синсет на новой строке.
Выделение синсета производится регулярным выражением `^.:\s+(.*)$`. Синонимы в синсете должны быть разделены знаком `;`.
Например:
```
A:      ВЕНЧАТЬ ЛАВРАМИ
B:      ИДТИ К УСПЕХУ; ПРИЙТИ К УСПЕХУ; СНИСКАТЬ ЛАВРЫ; ДОБИТЬСЯ УСПЕХА; ДОБИВАТЬСЯ УСПЕХА
A:      СТЛАТЬ; СТЕЛИТЬ; НАСТЛАТЬ; ПОСТЛАТЬ; НАСТИЛАТЬ; ПОСТЕЛИТЬ; ПОСТИЛАТЬ; РАЗОСТЛАТЬ; РАССТЕЛИТЬ; РАССТИЛАТЬ; РАССТИЛАТЬСЯ
B:      РАЗВЕРНУТЬ; РАЗВЕРТЫВАТЬ; РАЗВОРАЧИВАТЬ
```


## <a name="six"></a>Извлечение отношений `derived_from` и `composed_of`

Необходимо запустить два скрипта: `scripts/cognates_relation_statistics.py` и `scripts/collocation_relation_statistics.py`.
Они выделят из имеющихся данных новые отношения и запишут их в таблицу `sense_relations`.


## <a name="seven"></a>Генерация xml-файлов RuWordNet

Необходимо запустить скрипт `sql2xml/sql2rwn_xml.py`. Он сгенерирет xml-файлы RuWordNet (путь по-умолчанию: `sql2xml/out/rwn/`):
- `composed_of.xml` — файл отношений типа `composed_of` между текстовыми входами
- `derived_from.xml` — файл отношений типа `derived_from` между текстовыми входами
- `senses.A.xml` — файл со списком текстовых входов в подсети прилагательных
- `senses.N.xml` — файл со списком текстовых входов в подсети существительных
- `senses.V.xml` — файл со списком текстовых входов в подсети глаголов
- `synset_relations.A.xml` — файл отношений между синсетами в подсети прилагательных
- `synset_relations.N.xml` — файл отношений между синсетами в подсети существительных
- `synset_relations.V.xml` — файл отношений между синсетами в подсети глаголов
- `synsets.A.xml` — файл со списком синсетов в подсети прилагательных
- `synsets.N.xml` — файл со списком синсетов в подсети существительных
- `synsets.V.xml` — файл со списком синсетов в подсети глаголов
