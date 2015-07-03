import getopt
import os
import sys
from psycopg2 import connect, extras
import re


PKG_ROOT = os.path.split(__file__)[0]
OUT_ROOT = os.path.join(PKG_ROOT, 'out')

conn = None

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'ruwordnet',
    'host': '127.0.0.1'
}


def main(argv):
    global OUT_ROOT, conn

    help_str = 'Usage: {0} [-h] [--out-dir=<output_directory>]'.format(os.path.split(__file__)[1])
    try:
        opts, args = getopt.getopt(argv, "h", ["out-dir="])
    except getopt.GetoptError:
        print(help_str)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_str)
            sys.exit()
        elif opt == '--out-dir':
            OUT_ROOT = arg

    try:
        conn = connect(**dbconfig)
    except:
        print('I am unable to connect to the database')
        exit(1)

    print('Start')

    print('Generating lexfile for nouns')
    generate_lex_file('N')

    print('Done')


def generate_lex_file_empty(pos):
    types = {
        'N': ('N', 'NG')
    }[pos]
    lex_file = {
        'N': 'noun.all'
    }[pos]
    empty_word = 'empty{0}"'
    filename = os.path.join(OUT_ROOT, lex_file)
    if os.path.isfile(filename):
        os.remove(filename)
    print('Output file: ' + filename)
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            open(filename, 'w') as file:

        print('Finding entries...')
        sql = """
          SELECT
            c.id   c_id,
            c.name c_name,
            c.gloss,
            t.id,
            t.name,
            t.synt_type,
            array_remove(
                array_agg(DISTINCT s2.concept_id),
                NULL
            )      concept_ids,
            array_remove(
                array_agg(DISTINCT s3.entry_id),
                NULL
            )      entry_ids
          FROM synonyms s
            LEFT JOIN text_entry t
              ON t.id = s.entry_id
              AND t.synt_type IN %(types)s
            LEFT JOIN synonyms s2
              ON s2.entry_id = t.id
            INNER JOIN synonyms s3
              ON s3.concept_id = s.concept_id
            INNER JOIN concepts c
              ON c.id = s.concept_id
          GROUP BY t.id, c.id
          ORDER BY t.name NULLS LAST"""
        cur.execute(sql, {'types': types})

        rgxSpace = re.compile('([,()"]|\s+)')
        rgxEscape = re.compile('([,()])')
        empty_cnt = 1
        concepts = {}
        for row in cur:
            cid = row['c_id']
            if row['id'] is None:
                empty_cnt += 1
                row['name'] = empty_word.format(empty_cnt)
            else:
                row['name'] = rgxSpace.sub('_', row['name'].strip())
                # row['name'] = rgxEscape.sub(r'\\\1', row['name'])
                if len(row['concept_ids']) > 1:
                    row['name'] += str(row['concept_ids'].index(cid) + 1)
            if cid not in concepts:
                concept = {
                    'id': cid,
                    'name': row['c_name'],
                    'gloss': row['gloss'],
                    'relations': [],
                    'entries': []
                }
                concepts[cid] = concept
            concepts[cid]['entries'].append({k: row[k] for k in ('id', 'name', 'synt_type')})
        print('{0} entries found. {1} are empty.'.format(cur.rowcount, empty_cnt))

        print('Selecting relations...')
        sql = 'SELECT * FROM relations'
        cur.execute(sql)

        for relation in cur:
            cid = relation['from_id']
            # бывает, связь есть, а понятия такого нет
            if cid in concepts:
                concepts[cid]['relations'].append(relation)

        synset_tpl = '{{{words}{pointers} ({gloss})}}'

        count = len(concepts)
        i = 0
        print('Processing concepts ({0}) and relations...'.format(count))
        for cid, concept in concepts.items():
            i += 1
            gloss = concept['name'] + (' | ' + xstr(concept['gloss']) if concept['gloss'] is not None else '')
            gloss = rgxSpace.sub('_', gloss)
            words = [entry['name'] + ',' for entry in concept['entries'] if entry['id'] is not None]
            if not words:
                words = concept['entries'][0]['name'] + ','

            pointers = []
            for relation in concept['relations']:
                ptr_chr = get_pointer(relation['name'], relation['asp'], 'N')
                tid = relation['to_id']
                if ptr_chr is not None and tid in concepts:
                    ptr_word = get_pointer_word(concepts[tid]['entries'])
                    pointers.append(ptr_word + ',' + ptr_chr)

            synset = synset_tpl.format(words=''.join(words), pointers=' '.join(pointers), gloss=gloss)
            file.write(synset + '\n')
            print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)
        print()


def generate_lex_file(pos):

    types = {'N': ('N', 'NG')}[pos]
    lex_file = {'N': 'noun.all'}[pos]

    filename = os.path.join(OUT_ROOT, lex_file)

    if os.path.isfile(filename):
        os.remove(filename)

    print('Output file: ' + filename)

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, \
            open(filename, 'w') as file:

        print('Finding entries...')

        sql = """
          SELECT
            c.id   c_id,
            c.name c_name,
            c.gloss,
            t.id,
            t.name,
            t.synt_type,
            array_remove(
                array_agg(DISTINCT s2.concept_id),
                NULL
            )      concept_ids,
            array_remove(
                array_agg(DISTINCT s3.entry_id),
                NULL
            )      entry_ids
          FROM synonyms s
            LEFT JOIN text_entry t
              ON t.id = s.entry_id
              AND t.synt_type IN %(types)s
            LEFT JOIN synonyms s2
              ON s2.entry_id = t.id
            INNER JOIN synonyms s3
              ON s3.concept_id = s.concept_id
            INNER JOIN concepts c
              ON c.id = s.concept_id
          GROUP BY t.id, c.id
          ORDER BY t.name NULLS LAST"""
        cur.execute(sql, {'types': types})

        rgxSpace = re.compile('([,()"\s]+)')
        rgxEscape = re.compile('([,()])')
        empty_cnt = 1
        concepts = {}
        for row in cur:
            cid = row['c_id']
            if row['id'] is None:
                empty_cnt += 1
            else:
                row['name'] = rgxSpace.sub('_', row['name'].strip())
                # row['name'] = rgxEscape.sub(r'\\\1', row['name'])
                if len(row['concept_ids']) > 1:
                    row['name'] += str(row['concept_ids'].index(cid) + 1)
            if cid not in concepts:
                concept = {
                    'id': cid,
                    'name': row['c_name'],
                    'gloss': row['gloss'],
                    'relations': [],
                    'entries': []
                }
                concepts[cid] = concept
            concepts[cid]['entries'].append({k: row[k] for k in ('id', 'name', 'synt_type')})

        print('{0} entries found. {1} are empty.'.format(cur.rowcount, empty_cnt))

        print('Selecting relations...')
        sql = 'SELECT * FROM relations'
        cur.execute(sql)

        for relation in cur:
            cid = relation['from_id']
            # бывает, связь есть, а понятия такого нет
            if cid in concepts:
                concepts[cid]['relations'].append(relation)

        synset_tpl = '{{{words},{pointers} ({gloss})}}'

        count = len(concepts)
        i = 0
        print('Processing concepts ({0}) and relations...'.format(count))
        for cid, concept in concepts.items():
            i += 1

            concept['entries'] = [entry for entry in concept['entries'] if entry['id'] is not None]

            # Если у понятия нет текстовых входов необходимой части речи, пропускаем его.
            if len(concept['entries']) == 0:
                continue

            gloss = concept['name'] + (' | ' + xstr(concept['gloss']) if concept['gloss'] is not None else '')
            gloss = rgxSpace.sub('_', gloss)

            relations = []
            for relation in concept['relations']:
                relations += fix_relation(concepts, relation)

            pointers = []
            for relation in relations:
                ptr_chr = get_pointer(relation['name'], relation['asp'], 'N')
                if ptr_chr is not None:
                    toc = concepts[relation['to_id']]
                    ptr_word = get_pointer_word(toc['entries'])
                    pointers.append(ptr_word + ',' + ptr_chr)

            words = [entry['name'] for entry in concept['entries']]
            synset = synset_tpl.format(words=','.join(words), pointers=' '.join(pointers), gloss=gloss)
            file.write(synset + '\n')
            print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)
        print()


def fix_relation(concepts, relation) -> object:
    """
    Проверяем текущее отношение - оно должно указывать на понятие
    с не пустыми текстовыми входами. Если отношение не проходит
    проверку, спускаемся по иерархии отношений вниз и повторяем
    проверку для низлежащих отношений.

    :param concepts:
    :param relation:
    :return:
    """
    if relation['to_id'] in concepts:
        # Берём понятие, на которое указывает данно отношение
        toc = concepts[relation['to_id']]
        if len([entry for entry in toc['entries'] if entry['id'] is not None]) > 0:
            # Если у понятия есть не пустые текстовые входы, значит отношение нам подходит
            return [relation]
        # Отношение не подходит - спускаемся ниже по иерархии
        relations = []
        # Смотрим все отношения низлежащего понятия
        for rel in toc['relations']:
            # Проверяем, чтобы тип отношения совпадал с исходным отношением
            if rel['name'] == relation['name']:
                # И запускаем проверку этого отношения
                relations += fix_relation(concepts, rel)
        return relations
    return []


def get_pointer(rel_type, asp, pos):
    rel_map = {
        'АСЦ2': None,
        'ЦЕЛОЕ': '#p',
        'АСЦ1': None,
        'ЧАСТЬ': '%p',
        'НИЖЕ': '~',
        'ВЫШЕ': '@',
        'АСЦ': None
    }
    return rel_map[rel_type]


def get_pointer_word(entries):
    return next((entry['name'] for entry in entries if entry['id'] is not None), entries[0]['name'])


def xstr(s):
    return '' if s is None else str(s)


if __name__ == "__main__":
    main(sys.argv[1:])