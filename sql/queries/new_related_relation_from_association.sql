WITH rel AS (
  SELECT
    row_number() over (ORDER BY sf.name, st.name) rn,
    sf.id fid,
    sf.name fname,
    st.id tid,
    st.name tname
    FROM relations AS r
           JOIN synsets sf
               ON SUBSTRING(sf.id, '^\d+') = r.from_id::text
           JOIN synsets st
               ON SUBSTRING(st.id, '^\d+') = r.to_id::text
           LEFT JOIN relations rr
               ON rr.from_id = r.from_id
               AND rr.to_id = r.to_id
               AND rr.name != r.name
   WHERE r.name IN ('АСЦ', 'АСЦ1', 'АСЦ2')
     AND sf.part_of_speech = 'N'
     AND st.part_of_speech = 'N'
     AND sf.ID > st.ID
     AND rr.name IS NULL
)

SELECT fname AS synset, senses
  FROM (

    SELECT rel.rn, rel.fname, array_agg(se.name) AS senses
      FROM rel
             JOIN senses se
                 ON se.synset_id = rel.fid
     GROUP BY rel.rn, rel.fname
     UNION ALL
    SELECT rel.rn, rel.tname, array_agg(se.name)
      FROM rel
             JOIN senses se
                 ON se.synset_id = rel.tid
     GROUP BY rel.rn, rel.tname
     UNION ALL
    SELECT rel.rn, NULL, NULL
      FROM rel
     ORDER BY 1, 2 NULLS LAST

  ) _;
