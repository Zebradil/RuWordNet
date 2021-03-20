WITH non_abstract AS (
  SELECT
    row_number() OVER (ORDER BY sy.name) AS rn,
    sy.name,
    sr.parent_id,
    sr.child_id,
    cc_.version AS c_version,
    cp_.version AS p_version
    FROM synsets AS sy
           JOIN v2_concepts AS cc
               ON cc.id = substring(sy.id, e'^\\d+')::INT8
           LEFT JOIN concepts cc_
               ON cc_.ID = cc.id
           JOIN synset_relations AS sr
               ON sr.child_id = sy.id
           JOIN v2_concepts AS cp
               ON cp.id = substring(sr.parent_id, e'^\\d+')::INT8
           LEFT JOIN concepts cp_
               ON cp_.ID = cp.id
   WHERE NOT cc.is_abstract
     AND sr.name = 'hyponym'
     AND sy.part_of_speech = 'N'
)
SELECT
  name, VERSION, senses
  FROM (
    SELECT
      na.rn,
      0,
      sy.name,
      array_agg(se.name) AS senses,
      na.p_version AS version
      FROM non_abstract AS na
             JOIN synsets AS sy
                 ON sy.id = na.parent_id
             JOIN senses AS se
                 ON se.synset_id = sy.id
     GROUP BY na.rn, sy.name, na.p_version
     UNION
    SELECT
      na.rn,
      1,
      sy.name,
      array_agg(se.name) AS senses,
      na.c_version
      FROM non_abstract AS na
             JOIN synsets AS sy
                 ON sy.id = na.child_id
             JOIN senses AS se
                 ON se.synset_id = sy.id
     GROUP BY na.rn, sy.name, na.c_version
     UNION
    SELECT na.rn, 2, NULL, NULL, NULL
      FROM non_abstract AS na
     ORDER BY 1, 2
  ) AS _;
