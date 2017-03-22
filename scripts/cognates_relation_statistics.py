#!/usr/bin/env python3

# pylint: disable=C0103
# pylint: disable=C0111

import argparse
import re

from psycopg2 import connect, extras
from psycopg2._psycopg import IntegrityError

parser = argparse.ArgumentParser(
    description='Extract derivation relations from RuThes and RuWordNet.')
connection_string = "host='localhost' dbname='ruwordnet' user='ruwordnet' password='ruwordnet'"
parser.add_argument(
    '-c',
    '--connection-string',
    type=str,
    help="Postgresql database connection string ({})".format(
        connection_string),
    default=connection_string
)
parser.add_argument(
    '-t',
    '--test',
    help="Only show found relations, don't insert new relations in database",
    action='store_true'
)

ARGS = parser.parse_args()

conn = connect(ARGS.connection_string)
conn.autocommit = True

prefixes = [
    'БЕЗ',
    'БЕС',
    'ВО',
    'ВЗ',
    'ВЗО',
    'ВС',
    'ВНЕ',
    'ВНУТРИ',
    'ВОЗ',
    'ВОЗО',
    'ВОС',
    'ВСЕ',
    'ВЫ',
    'ДО',
    'ЗА',
    'ИЗ',
    'ИЗО',
    'ИС',
    'ИСПОД',
    'КОЕ',
    'КОЙ',
    'МЕЖ',
    'МЕЖДО',
    'МЕЖДУ',
    'НА',
    'НАД',
    'НАДО',
    'НАИ',
    'НЕ',
    'НЕБЕЗ',
    'НЕБЕС',
    'НЕДО',
    'НИ',
    'НИЗ',
    'НИЗО',
    'НИС',
    'ОБ',
    'ОБО',
    'ОБЕЗ',
    'ОБЕС',
    'ОКОЛО',
    'ОТ',
    'ОТО',
    'ПА',
    'ПЕРЕ',
    'ПО',
    'ПОД',
    'ПОД',
    'ПОЗА',
    'ПОСЛЕ',
    'ПРА',
    'ПРЕ',
    'ПРЕД',
    'ПРЕДО',
    'ПРЕДИ',
    'ПРИ',
    'ПРО',
    'ПРОТИВО',
    'РАЗ',
    'РАЗО',
    'РАС',
    'РОЗ',
    'РОС',
    'СО',
    'СВЕРХ',
    'СРЕДИ',
    'СУ',
    'СЫЗ',
    'ТРЕ',
    'ЧРЕЗ',
    'ЧЕРЕЗ',
    'ЧЕРЕС',
    'АНТИ',
    'АРХИ',
    'БИ',
    'ВИЦЕ',
    'ГИПЕР',
    'ДЕ',
    'ДЕЗ',
    'ДИС',
    'ИМ',
    'ИНТЕР',
    'ИР',
    'КВАЗИ',
    'КОНТР',
    'МАКРО',
    'МИКРО',
    'ОБЕР',
    'ПОСТ',
    'ПРОТО',
    'ПСЕВДО',
    'РЕ',
    'СУБ',
    'СУПЕР',
    'ТРАНС',
    'УЛЬТРА',
    'ЭКЗО',
    'ЭКС',
    'ЭКСТРА'
]
prefixes.sort(key=len, reverse=True)

roots_groups = (
    ('БР', 'БИР'),  # +
    ('БИВ', 'БИТ'),  # +
    ('БЫВ', 'БЫТ'),  # +
    ('ВЕР', 'ВОР'),  # +
    ('ГН', 'ГОН'),  # +
    ('ДАВ', 'ДАТ', 'ДАЧ'),  # +
    ('ЛИВ', 'ЛИТ'),  # +
    ('ЛЕЧ', 'ЛЕГ'),  # +
    ('ЧЕС', 'ЧЕТ'),  # +
    ('КАЖ', 'КАЗ'),  # +
    ('ЖИВ', 'ЖИТ'),  # +
    ('ПИВ', 'ПИТ'),  # +
    ('ЛЕП', 'ЛИП'),  # +
    ('МЕЩ', 'МЕСТ'),  # +
    ('МЫСЛ', 'МЫШЛ'),  # +
    ('МЯТ', 'МИН'),  # +
    ('РАЖ', 'РАЗ'),  # +
    ('РОД', 'РОЖ'),  # +
    ('СЫТ', 'СЫЩ'),  # +
    ('СИД', 'СИЖ'),  # +
    ('СОБ', 'САБ'),  # +
    ('СКОЛ', 'СКАЛ'),  # +
    ('СКОБ', 'СКАБ'),  # +
    ('ХОД', 'ХОЖ'),  # +
    ('НЕС', 'НОС', 'НОШ'),  # +
    ('ТЯГ', 'ТЯН', 'ТЯЖ'),  # +
    ('ДЛЕ', 'ДЛИ'),  # +
    ('ОБИЖ', 'ОБИД'),  # +
    ('СМОТР', 'СМАТР'),  # +
    ('ГОР', 'ГАР'),
    ('КЛОН', 'КЛАН'),
    ('ТВОР', 'ТВАР'),
    ('ЗОР', 'ЗАР'),
    ('ПЛАВ', 'ПЛОВ'),
    ('ЛАГ', 'ЛОЖ'),
    ('РАСТ', 'РАЩ', 'РОС'),
    ('КАС', 'КОС'),
    ('СКАК', 'СКОЧ'),
    ('БИР', 'БЕР'),
    ('ДИР', 'ДЕР'),
    ('МИР', 'МЕР'),
    ('ТИР', 'ТЕР'),
    ('ПИР', 'ПЕР'),
    ('ЖИГ', 'ЖЕГ'),
    ('СТИЛ', 'СТЕЛ'),
    ('БЛИСТ', 'БЛЕСТ'),
    ('ЧИТ', 'ЧЕТ'),
    ('МОК', 'МОЧ', 'МАК'),
    ('РАВН', 'РОВН'),
)

prefix_exceptions = (
    'НЕРВ',
    'ОБИЖ', 'ОБИД',
    'ПОЧТ',
    'ОТВЕТ', 'ОТВЕЧ',
    'ПОЛН',
    'РОСТ',
    'РОССИ',
    'ПРАВ',
    'НИЩЕ',
    'ТРЕН',
    'ТРЕВОЖ', 'ТРЕВОГ',
    'ЗАВИД', 'ЗАВИСТ',
)


def prepare_search_cognates(cursor):
    sql = r"""
        SELECT s.name, s.rel_name
        FROM (
               SELECT
                 name,
                 'synset' rel_name,
                 'RuWordNet' source
               FROM senses
               WHERE id != $1
                     AND synset_id = $2
               UNION
               SELECT
                 se.name,
                 sr.name,
                 'RuWordNet' source
               FROM senses se
                 INNER JOIN synset_relations sr
                   ON sr.child_id = se.synset_id
               WHERE sr.parent_id = $2
               UNION
               SELECT
                 t2.name,
                 r.name,
                 'RuThes' source
               FROM text_entry t1
                 INNER JOIN synonyms s1 ON s1.entry_id = t1.id
                 INNER JOIN relations r ON r.from_id = s1.concept_id
                 INNER JOIN synonyms s2 ON s2.concept_id = r.to_id
                 INNER JOIN text_entry t2 ON t2.id = s2.entry_id
               WHERE t1.name = $3
             ) s
        WHERE substr(s.name, 1, 4) = substr($3, 1, 4)
          AND array_length(regexp_split_to_array(s.name, '\s+'), 1) = 1"""

    cursor.execute('PREPARE search_cognates AS ' + sql)


def prepare_search_cognates_transitionally(cursor):
    sql = r"""
        WITH RECURSIVE tree (id, name, id_path, name_path, parent_relation_name) AS (
          SELECT
            id,
            name,
            ARRAY[id] id_path,
            ARRAY[name] name_path,
            $2 parent_relation_name
          FROM concepts
          WHERE id IN(
            SELECT concept_id
            FROM synonyms s
              INNER JOIN text_entry t
                ON t.id = s.entry_id
            WHERE t.name = $1
          )
          UNION ALL
          SELECT
            c.id,
            c.name,
            array_append(tree.id_path, c.id),
            array_append(tree.name_path, c.name),
            r.name parent_relation_name
          FROM tree
            INNER JOIN relations r
              ON r.from_id = tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
          WHERE r.name = ANY($3) AND tree.parent_relation_name = $2
        )

        SELECT t.name, tree.name_path, tree.parent_relation_name
        FROM tree
          INNER JOIN synonyms s
            ON s.concept_id = tree.id
          INNER JOIN text_entry t
            ON t.id = s.entry_id
        WHERE t.name != $1
          AND array_length(id_path, 1) > 1
          AND substr(t.name, 1, 4) = substr($1, 1, 4)
          AND array_length(regexp_split_to_array(t.name, '\s+'), 1) = 1"""
    cursor.execute('PREPARE search_cognates_transitionally AS ' + sql)


def prepare_search_sense(cursor):
    sql = """
        SELECT
          id,
          name,
          synset_id
        FROM senses
        WHERE name = $1
        ORDER BY meaning
        LIMIT 1"""

    cursor.execute('PREPARE search_sense AS ' + sql)


def make_insert_query(table, fields, cur):
    fields_str = ', '.join(str(v) for v in fields)
    dollars = ', '.join('$' + str(i + 1) for i in range(len(fields)))
    placeholders = ', '.join('%({0})s'.format(f) for f in fields)

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(
        placeholders=placeholders, table=table)

    sql = 'PREPARE prepared_query_{table} AS '.format(table=table) + \
          'INSERT INTO {tbl} ({fields}) VALUES ({dollars})' \
        .format(fields=fields_str, dollars=dollars, tbl=table)

    cur.execute(sql)
    return sql_str


cached_results = {}


def cache_result(func):
    def get_key_and_needle(word1, word2):
        if word1 > word2:
            return word1, word2
        else:
            return word2, word1

    def cache_result_inner(word1, word2):
        key, needle = get_key_and_needle(word1, word2)
        if key in cached_results:
            if needle in cached_results[key]:
                # print('got from cache')
                return cached_results[key][needle]
        else:
            cached_results[key] = {}
        result = func(word1, word2)
        cached_results[key][needle] = result
        # print('added to cache')
        return result
    return cache_result_inner


@cache_result
def is_cognates(word1, word2):
    print('checking words: {} {}'.format(word1, word2))
    if word1 == word2:
        print('same word')
        return False
    words1 = remove_prefixes(word1)
    words2 = remove_prefixes(word2)
    for sub1 in words1:
        for sub2 in words2:
            if check_substrings(sub1, sub2):
                print("is cognates: {} {}".format(word1, word2))
                return True
    print("isn't cognates: {} {}".format(word1, word2))
    return False


def check_substrings(word1, word2):
    match_len = min(len(word1), len(word2), 3)
    print('words after processing: {} {}'
          .format(word1[:match_len], word2[:match_len]))
    if word1[:match_len] == word2[:match_len]:
        print('beginnigs are equal')
        return True
    for root in get_roots_group(word1):
        if word2.find(root) == 0:
            print('root is found {}'.format(root))
            return True
    return False


def get_roots_group(word):
    for group in roots_groups:
        for root in group:
            if word.find(root) == 0:
                return group
    return []


def remove_prefixes(word):
    for exception in prefix_exceptions:
        if word.find(exception) == 0:
            return word
    forms = []
    for prefix in prefixes:
        if word.startswith(prefix):
            forms.append(word.replace(prefix, '', 1))
    return forms if forms else [word]


def main():
    test = ARGS.test

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            conn.cursor(cursor_factory=extras.RealDictCursor) as cur2:

        print('prepare_search_cognates', flush=True)
        prepare_search_cognates(cur2)
        print('prepare_search_cognates_transitionally', flush=True)
        prepare_search_cognates_transitionally(cur2)

        if not test:
            print('prepare_search_sense', flush=True)
            prepare_search_sense(cur2)
            insert_relation_sql = make_insert_query(
                'sense_relations', ('parent_id', 'child_id', 'name'), cur)

        print('search collocations', flush=True)
        sql = r"""
          SELECT
            id,
            name,
            synset_id
          FROM senses
          WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) = 1"""
        cur.execute(sql)

        print('start looping', flush=True)
        for row in cur:
            print(flush=True)
            print(row['name'], flush=True)

            if not test:
                lexemes = []

            params = {
                'sense_id': row['id'],
                'synset_id': row['synset_id'],
                'word': row['name']
            }
            cur2.execute(
                'EXECUTE search_cognates(%(sense_id)s, %(synset_id)s, %(word)s)', params)
            for cognate in cur2.fetchall():
                if is_cognates(row['name'], cognate['name']):
                    print(cognate['name'] + ': ' + cognate['rel_name'])
                    if not test:
                        lexemes.append(cognate['name'])

            for name in ('ВЫШЕ', 'НИЖЕ', 'ЧАСТЬ', 'ЦЕЛОЕ'):
                if name == 'ВЫШЕ':
                    tail_names = ['АСЦ', 'ЧАСТЬ']
                elif name == 'ЧАСТЬ':
                    tail_names = ['АСЦ']
                else:
                    tail_names = ['']

                params = {
                    'word': row['name'],
                    'name': name,
                    'tail_names': [name] + tail_names
                }
                cur2.execute(
                    """EXECUTE search_cognates_transitionally(
                        %(word)s, %(name)s, %(tail_names)s)""", params)
                for senses_chain in cur2.fetchall():
                    if is_cognates(row['name'], senses_chain['name']):
                        chain = senses_chain['name'] + ':' + \
                            ' (' + name + ') ' + \
                            ' → '.join(senses_chain['name_path']) + \
                            ' (' + senses_chain['parent_relation_name'] + ')'
                        print(chain)
                        if not test:
                            lexemes.append(senses_chain['name'])

            if not test and lexemes:
                params = {
                    'parent_id': row['id'],
                    'name': 'derived_from',
                }
                for lexeme in set(lexemes):
                    cur2.execute('EXECUTE search_sense(%(name)s)',
                                 {'name': lexeme})
                    row_lexeme = cur2.fetchone()
                    if row_lexeme:
                        try:
                            cur2.execute(insert_relation_sql, {
                                'child_id': row_lexeme['id'], **params})
                        except IntegrityError:
                            # Если такое отношение уже есть, не останавливаем
                            # выполнение
                            pass

    print('Done')


if __name__ == "__main__":
    main()
