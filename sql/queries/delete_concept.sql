-- Search duplicated concepts
-- That means that the concepts were merged in the newer version
SELECT
    name, array_agg(id) AS ids
FROM
    concepts
GROUP BY
    name
HAVING
    count(1) > 1;

-- Select concepts to be deleted (the ones which are not in the new version)
WITH
    sub
        AS (
            SELECT
                name, array_agg(id) AS ids
            FROM
                concepts
            GROUP BY
                name
            HAVING
                count(1) > 1
        )
SELECT
    array_remove(sub.ids, c2.id) AS to_delete,
    sub.name,
    c2.*
FROM
    sub LEFT JOIN v2_concepts AS c2 ON c2.id = ANY (sub.ids);

-- Replace concept ids in ili to the new ones
WITH
    sub
        AS (
            SELECT
                name, array_agg(id) AS ids
            FROM
                concepts
            GROUP BY
                name
            HAVING
                count(1) > 1
        ),
    sub2
        AS (
            SELECT
                array_remove(sub.ids, c2.id) AS to_delete,
                c2.id
            FROM
                sub
                LEFT JOIN v2_concepts AS c2 ON
                        c2.id = ANY (sub.ids)
        )
UPDATE ili
   SET concept_id = sub2.id
FROM
    sub2
WHERE
    ili.concept_id = ANY (sub2.to_delete);

-- Delete entities for every concept_id separately
-- Set concept_id with `\set concept_id 123`

DELETE FROM synonyms WHERE concept_id = :concept_id;

DELETE FROM relations WHERE from_id = :concept_id OR to_id = :concept_id;

DELETE FROM concepts WHERE id = :concept_id;

DELETE FROM sense_relations WHERE SUBSTRING(parent_id, '^\d+')::int = :concept_id OR SUBSTRING(child_id, '^\d+')::int = :concept_id;

DELETE FROM senses WHERE SUBSTRING(id, '^\d+')::int = :concept_id;

DELETE FROM synset_relations WHERE SUBSTRING(parent_id, '^\d+')::int = :concept_id OR SUBSTRING(child_id, '^\d+')::int = :concept_id;

DELETE FROM synsets WHERE SUBSTRING(id, '^\d+')::int = :concept_id;
