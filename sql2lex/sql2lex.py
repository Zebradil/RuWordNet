import getopt
import os
import re
import sys

from psycopg2 import connect, extras

PKG_ROOT = os.path.split(__file__)[0]
OUT_ROOT = os.path.join(PKG_ROOT, "out")

conn = None

dbconfig = {"database": "ruthes", "user": "ruwordnet", "password": "ruwordnet", "host": "127.0.0.1"}


def main(argv):
    global OUT_ROOT, conn

    help_str = "Usage: {0} [-h] [--out-dir=<output_directory>]".format(os.path.split(__file__)[1])
    try:
        opts, args = getopt.getopt(argv, "h", ["out-dir="])
    except getopt.GetoptError:
        print(help_str)
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(help_str)
            sys.exit()
        elif opt == "--out-dir":
            OUT_ROOT = arg

    try:
        conn = connect(**dbconfig)
    except:
        print("I am unable to connect to the database")
        exit(1)

    print("Start")

    print("Generating lexfile for nouns")
    generate_lex_file("N")

    print("Generating lexfile for verbs")
    generate_lex_file("V")

    print("Generating lexfile for adjectives")
    generate_lex_file("Adj")

    print("Done")


def generate_lex_file(pos):
    all_types = {
        "N": ("N", "NG", "NGprep", "PrepG"),
        "V": ("V", "VG", "VGprep", "Prdc"),
        "Adj": ("Adj", "AdjG", "AdjGprep"),
    }
    types = all_types[pos]
    lex_files = {"N": "noun.all", "V": "verb.all", "Adj": "adj.all"}
    lex_file = lex_files[pos]

    if pos == "N":
        derivational_types = all_types["V"]
        d_file = lex_files["V"]
    elif pos == "V":
        derivational_types = all_types["N"]
        d_file = lex_files["N"]
    else:
        derivational_types = None

    filename = os.path.join(OUT_ROOT, lex_file)

    if os.path.isfile(filename):
        os.remove(filename)

    print("Output file: " + filename)

    with conn.cursor(cursor_factory=extras.RealDictCursor) as cur, open(filename, "w", encoding="utf-8") as file:

        print("Finding entries...")

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
            INNER JOIN text_entry t
              ON t.id = s.entry_id
            INNER JOIN synonyms s2
              ON s2.entry_id = t.id
            INNER JOIN synonyms s3
              ON s3.concept_id = s.concept_id
            INNER JOIN concepts C
              ON C.id = s.concept_id
          GROUP BY t.id, C.id
          ORDER BY t.name NULLS LAST"""
        cur.execute(sql)

        rgxSpace = re.compile('([,()"\s]+)')
        empty_cnt = 0
        concepts = {}
        for row in cur:
            cid = row["c_id"]
            if row["id"] is None:
                empty_cnt += 1
            else:
                row["name"] = rgxSpace.sub("_", row["name"].strip())
                # row['name'] = rgxEscape.sub(r'\\\1', row['name'])
                if len(row["concept_ids"]) > 1:
                    row["name"] += str(row["concept_ids"].index(cid) + 1)
            if cid not in concepts:
                concept = {
                    "id": cid,
                    "name": row["c_name"],
                    "gloss": row["gloss"],
                    "relations": [],
                    "entries": [],
                    "all_entries": [],
                }
                concepts[cid] = concept
            concepts[cid]["all_entries"].append({k: row[k] for k in ("id", "name", "synt_type")})
            # Фильтруем текстовые входы, оставляя только определённую часть речи
            if row["synt_type"] in types:
                concepts[cid]["entries"].append({k: row[k] for k in ("id", "name", "synt_type")})

        print("{0} entries found. {1} are empty.".format(cur.rowcount, empty_cnt))

        print("Selecting relations...")
        sql = """
          SELECT r.*
          FROM relations r
            INNER JOIN concepts cf
              ON cf.id = r.from_id
            INNER JOIN concepts ct
              ON ct.id = r.to_id
        """
        cur.execute(sql)

        for relation in cur:
            concepts[relation["from_id"]]["relations"].append(relation)

        if pos == "V":
            synset_tpl = "{{{words},{pointers} frames: 1 ({gloss})}}"
        else:
            synset_tpl = "{{{words},{pointers} ({gloss})}}"

        count = len(concepts)
        i = 0
        print("Processing concepts ({0}) and relations...".format(count))
        for cid, concept in concepts.items():
            i += 1

            concept["entries"] = [entry for entry in concept["entries"] if entry["id"] is not None]

            # Если у понятия нет текстовых входов необходимой части речи, пропускаем его.
            if len(concept["entries"]) == 0:
                continue

            gloss = concept["name"] + (" | " + xstr(concept["gloss"]) if concept["gloss"] is not None else "")
            gloss = rgxSpace.sub("_", gloss)

            relations = []
            for relation in concept["relations"]:
                relations += fix_relation(concepts, relation)

            relations = uniqify(relations, lambda r: "{to_id}|{name}".format(**r))

            pointers = []
            for relation in relations:
                ptr_chr = get_pointer(relation["name"], relation["asp"], pos)
                if ptr_chr is not None:
                    toc = concepts[relation["to_id"]]
                    ptr_word = get_pointer_word(toc["entries"])
                    pointers.append(ptr_word + "," + ptr_chr)

            # Отдельно добавляем указатели для словообразовательных отношений
            if derivational_types:
                pointers += gen_derivational_pointers(derivational_types, concept, d_file)
            else:
                for entry in concept["all_entries"]:
                    est = entry["synt_type"]
                    if est in all_types["N"]:
                        d_file = lex_files["N"]
                        ptr_chr = "\\"
                    elif est in all_types["V"]:
                        d_file = lex_files["V"]
                        ptr_chr = "<"
                    else:
                        continue
                    pointers += ["{0}:{1},{2}".format(d_file, entry["name"], ptr_chr)]

            words = [entry["name"] for entry in concept["entries"]]
            synset = synset_tpl.format(words=",".join(words), pointers=" ".join(pointers), gloss=gloss)
            file.write(synset + "\n")
            print("\rProgress: {0}% ({1})".format(round(i / count * 100), i), end="", flush=True)
        print()


def gen_derivational_pointers(derivational_types, concept, d_file):
    for d_type in derivational_types:
        for entry in concept["all_entries"]:
            if entry["synt_type"] == d_type:
                return ["{0}:{1},+".format(d_file, entry["name"])]
    return []


def uniqify(seq, idfun=None):
    # order preserving
    if idfun is None:

        def idfun(x):
            return x

    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        if marker in seen:
            continue
        seen[marker] = 1
        result.append(item)
    return result


def fix_relation(concepts, relation, path=None) -> object:
    """
    Проверяем текущее отношение - оно должно указывать на понятие
    с не пустыми текстовыми входами. Если отношение не проходит
    проверку, спускаемся по иерархии отношений вниз и повторяем
    проверку для низлежащих отношений.

    :param path:
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
    if relation["to_id"] in concepts:
        # Берём понятие, на которое указывает данное отношение
        toc = concepts[relation["to_id"]]
        if len([entry for entry in toc["entries"] if entry["id"] is not None]) > 0:
            # Если у понятия есть не пустые текстовые входы, значит отношение нам подходит
            return [relation]
        # Отношение не подходит
        # Замыкание предусмотрено не для всех типов связей
        if relation["name"] not in ["НИЖЕ", "ВЫШЕ"]:  # , 'ЭКЗЕМПЛЯР', 'КЛАСС', 'ЦЕЛОЕ', 'ЧАСТЬ']:
            return []
        # Спускаемся ниже по иерархии
        relations = []
        # Смотрим все отношения низлежащего понятия
        for rel in toc["relations"]:
            # Проверяем, чтобы тип отношения совпадал с исходным отношением
            if rel["name"] == relation["name"]:
                # И запускаем проверку этого отношения
                relations += fix_relation(concepts, rel, path)
        return relations
    return []


def get_pointer(rel_type, asp, pos):
    rel_map = {
        "N": {
            "АСЦ2": None,
            "ЦЕЛОЕ": "#p",
            "АСЦ1": None,
            "ЧАСТЬ": "%p",
            "НИЖЕ": "~",
            "ВЫШЕ": "@",
            "АСЦ": None,
            "АНТОНИМ": "!",
            "ЭКЗЕМПЛЯР": "~i",
            "КЛАСС": "@i",
        },
        "V": {
            "АСЦ2": None,
            "ЦЕЛОЕ": "*",
            "АСЦ1": None,
            "ЧАСТЬ": "*",
            "НИЖЕ": "~",
            "ВЫШЕ": "@",
            "АСЦ": None,
            "АНТОНИМ": "!",
            "ЭКЗЕМПЛЯР": None,
            "КЛАСС": None,
        },
        "Adj": {
            "АСЦ2": None,
            "ЦЕЛОЕ": None,
            "АСЦ1": None,
            "ЧАСТЬ": None,
            "НИЖЕ": "&",
            "ВЫШЕ": "&",
            "АСЦ": None,
            "АНТОНИМ": "!",
            "ЭКЗЕМПЛЯР": None,
            "КЛАСС": None,
        },
    }

    if rel_type in ("ЧАСТЬ", "ЦЕЛОЕ") and asp == "":
        return None

    return rel_map[pos][rel_type]


def get_pointer_word(entries):
    return next((entry["name"] for entry in entries if entry["id"] is not None), entries[0]["name"])


def xstr(s):
    return "" if s is None else str(s)


if __name__ == "__main__":
    main(sys.argv[1:])
