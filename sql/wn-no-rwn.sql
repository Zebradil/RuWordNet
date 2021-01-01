select wn.*
from wn_data wn
join ili_map_wn map on map.wn = wn.id and map.version = wn.version
left join wn_mapping ON array[wn.id] <@ wn_id_variants(wn_mapping.wn30)
left join ili ili30 ON ili30.wn_id = wn_mapping.wn30
left join ili ili31 ON ili31.source = 'manual' and ili31.wn_id = wn_mapping.wn31
WHERE ili30.wn_id is null and ili31.wn_id is null;
