#!/usr/bin/python3
# coding=utf-8

from psycopg2 import connect, extras

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}

blacklist = ['И', 'ДА', 'ЖЕ', 'ТО', 'ИЛИ', 'КАК', 'РАЗ', 'ТАК', 'ЧТО', 'ЛИШЬ', 'БУДТО', 'ПОСЛЕ', 'ТОЧНО', 'ЧТОБЫ',
             'СЛОВНО', 'Д', 'Ж', 'И', 'О', 'С', 'Ф', 'Х', 'В', 'И', 'К', 'О', 'С', 'У', 'Х', 'А-ЛЯ', 'ВО', 'ДО', 'ЗА',
             'ИЗ', 'КО', 'НА', 'ОБ', 'ОТ', 'ПО', 'СО', 'ИЗ-ПОД', 'БЕЗ', 'ВНЕ', 'ДЛЯ', 'ИЗО', 'НАД', 'ОТО', 'ПОД', 'ПРИ',
             'ПРО', 'ЧТО', 'ВЫШЕ', 'МИМО', 'РАДИ', 'СЕБЯ', 'ВНИЗУ', 'МЕЖДУ', 'ПЕРЕД', 'ПОСЛЕ', 'САМЫЙ', 'СВЕРХ',
             'СЗАДИ', 'СНИЗУ', 'СРЕДИ', 'ЧЕРЕЗ', 'ВМЕСТО', 'ВНУТРИ', 'ВНУТРЬ', 'ВОКРУГ', 'ВПЕРЕД', 'НАСЧЕТ', 'ПОЗАДИ',
             'ПРОТИВ', 'СВЕРХУ', 'СКВОЗЬ', 'СЛОВНО', 'ВПЕРЕДИ', 'ИЗНУТРИ', 'СДЕЛАТЬ', 'СПЕРЕДИ', 'СТОРОНА', 'НАПРОТИВ',
             'СОГЛАСНО', 'ОТНОСИТЕЛЬНО', 'О', 'Я', 'НА', 'ОН', 'ТЫ', 'ВСЕ', 'ЕГО', 'КТО', 'МОЙ', 'НАШ', 'ОБА', 'ОНИ',
             'САМ', 'ТАК', 'ТОМ', 'ТОТ', 'ЧТО', 'ЧЕЙ-ТО', 'КТО-НИБУДЬ', 'ВЕСЬ', 'ВСЕЙ', 'ИНОЙ', 'ОДИН', 'СВОЙ', 'СЕБЯ',
             'ЭТОТ', 'ВЕСТИ', 'КАКОЙ', 'НИКТО', 'НИЧЕЙ', 'ПЛОХО', 'САМЫЙ', 'СОБОЙ', 'КАКОЙ-ТО', 'ВСЯКИЙ', 'ДАННЫЙ',
             'ДРУГОЙ', 'КАЖДЫЙ', 'МНОГИЙ', 'МНОГОЕ', 'НЕЧЕГО', 'НИЧЕГО', 'НЕКОТОРЫЙ', 'НЕПОХОЖИЙ', 'ОСТАЛЬНОЙ',
             'СТАРАТЕЛЬНО', 'БЫ', 'ЖЕ', 'НЕ', 'НИ', 'ТО', 'ВОН', 'ЕЩЕ', 'НЕТ', 'УЖЕ', 'СЕБЯ', 'ТОГО', 'ВСЕГО', 'ДОБРО',
             'ПРОСТО', 'ХОРОШО']

conn = connect(**dbconfig)


def prepare_rwn_relation_query(cursor):
    sql = """
      SELECT sr.name
      FROM senses se
        INNER JOIN synsets sy
          ON sy.id = se.synset_id
        INNER JOIN synset_relations sr
          ON sr.child_id = sy.id
      WHERE sr.parent_id = $1
            AND se.lemma = $2
      UNION ALL
      SELECT 'synset'
      FROM senses
      WHERE lemma = $2
            AND synset_id = $1
      LIMIT 1"""
    cursor.execute('PREPARE select_rwn_relation AS ' + sql)


def prepare_ruthes_relation_query(cursor):
    sql = """
      SELECT
        r.name,
        r.asp
      FROM text_entry t
        INNER JOIN synonyms s
          ON s.entry_id = t.id
        INNER JOIN relations r
          ON r.to_id = s.concept_id
        INNER JOIN concepts c
          ON c.id = r.from_id
        INNER JOIN synonyms s2
          ON s2.concept_id = c.id
        INNER JOIN text_entry t2
          ON t2.id = s2.entry_id
      WHERE t.lemma = $1
            AND t2.lemma = $2
      LIMIT 1"""
    cursor.execute('PREPARE select_ruthes_relation AS ' + sql)


def prepare_transitional_relation_query(cursor):
    sql = """
        WITH RECURSIVE tree (root_id, root_lemma, id, lemma, id_path, lemma_path) AS (
          SELECT
            c.id root_id,
            t.lemma root_lemma,
            c.id,
            t.lemma,
            ARRAY[c.id] id_path,
            ARRAY[t.lemma] lemma_path
          FROM text_entry t
            INNER JOIN synonyms s
              ON s.entry_id = t.id
            INNER JOIN concepts c
              ON c.id = s.concept_id
            WHERE t.lemma = $2
          UNION ALL
          SELECT
            tree.id root_id,
            tree.root_lemma root_lemma,
            c.id,
            t.lemma,
            array_append(tree.id_path, c.id),
            array_append(tree.lemma_path, t.lemma)
          FROM tree
            INNER JOIN relations r
              ON r.from_id = tree.id
            INNER JOIN concepts c
              ON c.id = r.to_id
            INNER JOIN synonyms s
              ON s.concept_id = c.id
            INNER JOIN text_entry t
              ON t.id = s.entry_id
          WHERE r.name = $3
        )

        SELECT
          *
        FROM tree
        WHERE lemma = $1
        LIMIT 1"""
    cursor.execute('PREPARE select_transited_relation AS ' + sql)


def prepare_sense_existance_check_query(cursor):
    sql = """
      SELECT
        (SELECT count(1) FROM senses WHERE lemma = $1) sense,
        (SELECT count(1) FROM text_entry WHERE lemma = $1) entry"""
    cursor.execute('PREPARE check_sense_existence AS ' + sql)


def main():
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            conn.cursor(cursor_factory=extras.RealDictCursor) as cur2:
        print('search collocations', flush=True)
        sql = """
          SELECT
            name,
            lemma,
            synset_id
          FROM senses
          WHERE array_length(regexp_split_to_array(lemma, '\s+'), 1) > 1"""
        cur.execute(sql)

        print('prepare_rwn_relation_query', flush=True)
        prepare_rwn_relation_query(cur2)
        print('prepare_ruthes_relation_query', flush=True)
        prepare_ruthes_relation_query(cur2)
        print('prepare_transitional_relation_query', flush=True)
        prepare_transitional_relation_query(cur2)
        print('prepare_sense_existance_check_query', flush=True)
        prepare_sense_existance_check_query(cur2)

        counters = {
            'collocations': 0,
            'noRelation': 0,
            'wordPresented': 0,
            'relations': {

            },
        }
        print('start looping', flush=True)
        for row in cur:
            print(flush=True)
            print(row['name'], ':', flush=True)
            counters['collocations'] += 1

            for word in row['lemma'].split():
                if word in blacklist:
                    continue
                string = word + ' - '
                params = {
                    'synset_id': row['synset_id'],
                    'word': word
                }
                cur2.execute('EXECUTE select_rwn_relation(%(synset_id)s, %(word)s)', params)
                rwn_relation = cur2.fetchone()

                if rwn_relation is None:
                    params = {
                        'collocation': row['lemma'],
                        'word': word,
                    }
                    cur2.execute('EXECUTE select_ruthes_relation(%(word)s, %(collocation)s)', params)
                    ruthes_relation = cur2.fetchone()
                    if ruthes_relation is None:
                        chain = None
                        for name in ['ВЫШЕ', 'НИЖЕ', 'ЧАСТЬ', 'ЦЕЛОЕ']:
                            params = {
                                'word': word,
                                'collocation': row['lemma'],
                                'name': name
                            }
                            cur2.execute('EXECUTE select_transited_relation(%(word)s, %(collocation)s, %(name)s)',
                                         params)
                            senses_chain = cur2.fetchone()
                            if senses_chain is not None:
                                chain = '(' + name + ') ' + ' → '.join(senses_chain['lemma_path'])

                        if chain is not None:
                            string += chain
                        else:
                            string += 'нет'

                            # n = None
                            # string += 'нет'
                            # counters['noRelation'] += 1
                            # cur2.execute('EXECUTE check_sense_existence(%(word)s)', {'word': word})
                            # sense_entry = cur2.fetchone()
                            # if sense_entry['sense'] > 0 or sense_entry['entry'] > 0:
                            #     counters['wordPresented'] += 1
                            #     existence_strings = []
                            #     if sense_entry['entry'] > 0:
                            #         existence_strings.append('есть в РуТез')
                            #     if sense_entry['sense'] > 0:
                            #         existence_strings.append('есть в RWN')
                            #     string += ' (' + (', '.join(existence_strings)) + ')'
                    else:
                        n = ruthes_relation['name']
                        string += n
                else:
                    n = rwn_relation['name']
                    string += n

                if n is not None:
                    if n not in counters['relations']:
                        counters['relations'][n] = 0
                    counters['relations'][n] += 1

                print(string, flush=True)
        print(flush=True)
        print('Словосочетаний: ' + str(counters['collocations']))
        print('Слов без отношений: ' + str(counters['noRelation']) +
              ' (' + str(counters['wordPresented']) + ' слов представлены в тезаурусе)')
        print('Количество связей:')
        for relation, count in counters['relations'].items():
            print(relation + ' — ' + str(count))

    print('Done')


if __name__ == "__main__":
    main()