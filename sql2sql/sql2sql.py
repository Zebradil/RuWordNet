import os
import sys
import uuid
from subprocess import Popen, PIPE

from psycopg2 import connect, extras

PKG_ROOT = os.path.split(__file__)[0]

conn = None

dbconfig = {
    'database': 'ruthes',
    'user': 'ruwordnet',
    'password': 'tyjn2008',
    'host': '127.0.0.1'
}

dry_run = False


def main(argv):
    global conn

    try:
        conn = connect(**dbconfig)
    except:
        print('I am unable to connect to the database')
        exit(1)

    print('Start')
    init_db()
    transform_ruthes_to_ruwordnet()
    create_indexes()

    print('Done')


def transform_ruthes_to_ruwordnet():
    all_types = {
        'N': ('N', 'NG', 'NGprep', 'PrepG'),
        'V': ('V', 'VG', 'VGprep', 'Prdc'),
        'Adj': ('Adj', 'AdjG', 'AdjGprep'),
    }
    # все типы текстовых входов, которые можно экспортировать
    types = [i for sub in all_types.values() for i in sub]

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:

        print('Finding entries...')

        sql = """
          SELECT
            c.id   c_id,
            c.name c_name,
            c.gloss,
            t.id,
            t.name,
            t.lemma,
            t.main_word,
            t.pos_string,
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
            INNER JOIN text_entry t
              ON t.id = s.entry_id
            INNER JOIN synonyms s2
              ON s2.entry_id = t.id
            INNER JOIN synonyms s3
              ON s3.concept_id = s.concept_id
            INNER JOIN concepts c
              ON c.id = s.concept_id
          GROUP BY t.id, c.id
          ORDER BY t.name NULLS LAST"""
        cur.execute(sql)

        concepts = {}
        # обработка данных из БД
        for row in cur:
            cid = row['c_id']
            row['name'] = row['name'].strip()
            row['poses'] = row['pos_string']
            # если текстовый вход многозначный — проставляем номер значения
            if len(row['concept_ids']) > 1:
                row['meaning'] = row['concept_ids'].index(cid) + 1
            else:
                row['meaning'] = 0
            # накопление понятий
            if cid not in concepts:
                concept = {
                    'id': cid,
                    'name': row['c_name'],
                    'gloss': row['gloss'],
                    'relations': [],
                    'entries': [],
                }
                concepts[cid] = concept
            # если текстовый вход имеет тип из списка для экспорта, он добавляется к понятию
            if row['synt_type'] in types:
                entry = {k: row[k] for k in ('id', 'name', 'lemma', 'synt_type', 'meaning', 'main_word', 'poses')}
                entry['part_of_speech'] = get_part_of_speech(entry['synt_type'], all_types)
                concepts[cid]['entries'].append(entry)

        print('{0} entries found.'.format(cur.rowcount))

        print('Selecting relations...')
        sql = """
          SELECT r.*
          FROM relations r
          INNER JOIN concepts c1
            ON c1.id = r.from_id
          INNER JOIN concepts c2
            ON c2.id = r.to_id"""
        cur.execute(sql)
        # распределение отношений по понятиям
        for relation in cur:
            concepts[relation['from_id']]['relations'].append(relation)

        insert_synset_sql = make_insert_query('synsets', ('id', 'name', 'definition', 'part_of_speech'), cur)
        insert_sense_sql = make_insert_query('senses', (
        'id', 'synset_id', 'name', 'lemma', 'synt_type', 'meaning', 'main_word', 'poses'), cur)

        count = len(concepts)
        i = 0
        print('Processing concepts ({0})...'.format(count))
        for cid, concept in concepts.items():
            i += 1

            # Определение, в каких частях речи представлено понятие
            uuids = {}
            for entry in concept['entries']:
                uuids[entry['part_of_speech']] = True

            # Создание синсета для каждой из частей речи
            for pos in uuids.keys():
                new_uuid = str(uuid.uuid4())
                synset_data = {
                    'id': new_uuid,
                    'name': concept['name'],
                    'definition': concept['gloss'],
                    'part_of_speech': pos
                }
                if dry_run:
                    print(synset_data)
                else:
                    cur.execute(insert_synset_sql, synset_data)
                uuids[pos] = new_uuid

            concepts[cid]['uuids'] = uuids

            # Создание понятий
            for entry in concept['entries']:
                sense_data = {
                    'id': str(uuid.uuid4()),
                    'synset_id': uuids[entry['part_of_speech']],
                    'name': entry['name'],
                    'lemma': entry['lemma'],
                    'synt_type': entry['synt_type'],
                    'meaning': entry['meaning'],
                    'main_word': entry['main_word'],
                    'poses': entry['poses'],
                }
                if dry_run:
                    print(sense_data)
                else:
                    cur.execute(insert_sense_sql, sense_data)

            if not dry_run:
                print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)

        print('\nProcessing relations...')
        i = 0
        insert_relation_sql = make_insert_query('synset_relations', ('parent_id', 'child_id', 'name'), cur)
        for cid, concept in concepts.items():
            i += 1
            # Деривативные связи
            for parent_pos, parent_uuid in concept['uuids'].items():
                for child_pos, child_uuid in concept['uuids'].items():
                    if parent_pos != child_pos:
                        relation_data = {
                            'parent_id': parent_uuid,
                            'child_id': child_uuid,
                            'name': 'derivational',
                        }
                        if dry_run:
                            print(relation_data)
                        else:
                            cur.execute(insert_relation_sql, relation_data)

            # Остальные отношения
            for pos, c_uuid in concept['uuids'].items():
                relations = []
                for relation in concept['relations']:
                    relations += fix_relation(concepts, relation, all_types[pos])

                relations = uniqify(relations, lambda r: "{to_id}|{name}".format(**r))

                for relation in relations:
                    to_concept = concepts[relation['to_id']]
                    # TODO Проверить — возможно это проверка лишняя
                    if pos in to_concept['uuids']:
                        relation_name = get_relation_name(relation['name'], relation['asp'], pos)
                        if relation_name is not None:
                            relation_data = {
                                'parent_id': c_uuid,
                                'child_id': to_concept['uuids'][pos],
                                'name': relation_name,
                            }
                            if dry_run:
                                print(relation_data)
                            else:
                                cur.execute(insert_relation_sql, relation_data)
            if not dry_run:
                print('\rProgress: {0}% ({1})'.format(round(i / count * 100), i), end='', flush=True)
        conn.commit()
        print()


def get_part_of_speech(synt_type, all_types):
    for pos, types in all_types.items():
        if synt_type in types:
            return pos


def uniqify(seq, idfun=None):
    # order preserving
    if idfun is None:
        def idfun(x): return x
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        if marker in seen:
            continue
        seen[marker] = 1
        result.append(item)
    return result


def make_insert_query(table, fields, cur):
    fields_str = ', '.join(str(v) for v in fields)
    dollars = ', '.join('$' + str(i + 1) for i in range(len(fields)))
    placeholders = ', '.join('%({0})s'.format(f) for f in fields)

    sql_str = "EXECUTE prepared_query_{table} ({placeholders})".format(placeholders=placeholders, table=table)

    sql = 'PREPARE prepared_query_{table} AS '.format(table=table) + \
          'INSERT INTO {tbl} ({fields}) VALUES ({dollars})' \
              .format(fields=fields_str, dollars=dollars, tbl=table)

    cur.execute(sql)
    return sql_str


def fix_relation(concepts, relation, types, path=None) -> object:
    """
    Проверяем текущее отношение - оно должно указывать на понятие
    с не пустыми текстовыми входами. Если отношение не проходит
    проверку, спускаемся по иерархии отношений вниз и повторяем
    проверку для низлежащих отношений.

    :param path:
    :param types:
    :param concepts:
    :param relation:
    :return:
    """
    if path is None:
        path = []
    # Это отношение уже рассматривалось
    if relation in path:
        return []
    path.append(relation)
    if relation['to_id'] in concepts:
        # Берём понятие, на которое указывает данное отношение
        toc = concepts[relation['to_id']]
        if len([entry for entry in toc['entries'] if entry['synt_type'] in types]) > 0:
            # Если у понятия есть текстовые входы запрошенного типа, значит отношение нам подходит
            return [relation]
        # Отношение не подходит
        # Замыкание предусмотрено не для всех типов связей
        if relation['name'] not in ['НИЖЕ', 'ВЫШЕ']:  # , 'ЭКЗЕМПЛЯР', 'КЛАСС', 'ЦЕЛОЕ', 'ЧАСТЬ']:
            return []
        # Спускаемся ниже по иерархии
        relations = []
        # Смотрим все отношения низлежащего понятия
        for rel in toc['relations']:
            # Проверяем, чтобы тип отношения совпадал с исходным отношением
            if rel['name'] == relation['name']:
                # И запускаем проверку этого отношения
                relations += fix_relation(concepts, rel, types, path)
        return relations
    return []


def get_relation_name(rel_type, asp, pos):
    rel_map = {
        'N': {
            'АСЦ2': None,
            'ЦЕЛОЕ': 'part holonym',
            'АСЦ1': None,
            'ЧАСТЬ': 'part meronym',
            'НИЖЕ': 'hyponym',
            'ВЫШЕ': 'hypernym',
            'АСЦ': None,
            'АНТОНИМ': 'antonym',
            'ЭКЗЕМПЛЯР': 'instance hyponym',
            'КЛАСС': 'instance hypernym',
        },
        'V': {
            'АСЦ2': None,
            'ЦЕЛОЕ': 'part holonym',
            'АСЦ1': None,
            'ЧАСТЬ': 'part meronym',
            'НИЖЕ': 'hyponym',
            'ВЫШЕ': 'hypernym',
            'АСЦ': None,
            'АНТОНИМ': 'antonym',
            'ЭКЗЕМПЛЯР': None,
            'КЛАСС': None,
        },
        'Adj': {
            'АСЦ2': None,
            'ЦЕЛОЕ': None,
            'АСЦ1': None,
            'ЧАСТЬ': None,
            'НИЖЕ': 'hyponym',
            'ВЫШЕ': 'hypernym',
            'АСЦ': None,
            'АНТОНИМ': 'antonym',
            'ЭКЗЕМПЛЯР': None,
            'КЛАСС': None,
        }
    }

    if rel_type in ('ЧАСТЬ', 'ЦЕЛОЕ') and asp == '':
        return None

    return rel_map[pos][rel_type]


def create_indexes():
    print('Creating indexes')
    filename = os.path.join(PKG_ROOT, 'sql', 'script_create_constraints.sql')
    run_sql_file(filename)


def run_sql_file(filename):
    print('Run sql from ' + filename)
    if dry_run:
        print('Not really, because of dry-run mode')
        return
    pg_env = os.environ.copy()
    pg_env.update({'PGPASSWORD': dbconfig['password']})
    cmd_str = 'psql -U {user} -d {database} -h {host} -f {filename}'.format(filename=filename, **dbconfig)
    process = Popen(cmd_str, stdout=PIPE, stdin=PIPE, shell=True, env=pg_env)
    stdout, stderr = process.communicate()
    if stderr is None:
        print(stdout.decode())
    else:
        print(stderr.decode())
    return stderr is None


def init_db():
    print('Preparing database')
    filename = os.path.join(PKG_ROOT, 'sql', 'script_prepare_db.sql')
    run_sql_file(filename)


if __name__ == "__main__":
    main(sys.argv[1:])
