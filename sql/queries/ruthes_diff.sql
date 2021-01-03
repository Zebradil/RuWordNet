select
s.*, s2.*
from (
    select c.id, c.name, t.id t_id, t.name t_name
    from concepts c
    left join synonyms s on s.concept_id = c.id
    left join text_entry t on t.id = s.entry_id
) s
full join (
    select c.id, c.name, t.id t_id, t.name t_name
    from v2_concepts c
    left join v2_synonyms s on s.concept_id = c.id
    left join v2_text_entry t on t.id = s.entry_id
) s2
    on s2.id = s.id and s2.t_id = s.t_id
where s.t_id is null or s2.t_id is null
order by
    case when s.name is null then s2.name else s.name end,
    case when s.t_name is null then s2.t_name else s.t_name end;
