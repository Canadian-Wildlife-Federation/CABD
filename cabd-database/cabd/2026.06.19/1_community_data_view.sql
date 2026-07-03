set role cabd;

--create a function to determine passability status from stream crossing community data
create or replace function stream_crossings.community_passability_status(c stream_crossings.stream_crossings_community_holding)
returns integer
language plpgsql 
immutable 
as $$
BEGIN
RETURN CASE 
WHEN
(
c.obs_constriction_code is not null 
and
--none, null, or only beaver dams
COALESCE(c.upstream_physical_blockages_code, '{}') || COALESCE(c.downstream_physical_blockages_code, '{}') <@ ARRAY[9]::int[]
and 
--no constriction
c.obs_constriction_code in (1, 2) -- 1/2 (no)
)
THEN 3
WHEN
(
c.cbs_constriction_code is not null
and
--no blockages
(COALESCE(c.upstream_physical_blockages_code, '{}') || COALESCE(c.downstream_physical_blockages_code, '{}')) = '{}'
and
-- no outlet drop code
c.outlet_drop_code is null
and 
-- no constriction
c.obs_constriction_code in (1, 2) -- 1/2 (no)
and 
-- water inside structure != no/dry
c.water_flowing_through_code != 1 --no_dry
)
THEN 3
WHEN
(
c.cbs_constriction_code is not null
and
--no blockages
(COALESCE(c.upstream_physical_blockages_code, '{}') || COALESCE(c.downstream_physical_blockages_code, '{}')) = '{}'
and
-- no outlet drop
c.outlet_drop_code is null
and 
-- constriction
c.obs_constriction_code = 3 -- yes
and 
-- water inside structure = yes
c.water_flowing_through_code != 2 --yes-standing
)
THEN 3
WHEN 
(
c.cbs_constriction_code is not null
and 
--constriction
c.cbs_constriction_code = 3 -- yes
)
THEN 1
WHEN
(
c.cbs_constriction_code is not null
and 
-- outlet drop > 30cm
c.outlet_drop_code = 1 -- >30cm
)
THEN 1
WHEN 
(
c.cbs_constriction_code is not null
and 
-- water inside structure No - Dry and 
c.water_flowing_through_code = 1 --no_dry
and 
-- water flowing upstream = No - Dry
c.water_flowing_upstream_code = 1 --no_dry
)
THEN 1
ELSE 4
END;
END;
$$;



create or replace view cabd.community_data_staging_view as 
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 
'stream_crossings' as feature_type, 
case when b.status is null then 'NEW' else b.status::varchar end as status,
case when b.id is not null then stream_crossings.community_passability_status(b) else 4 end as passability_status_code
from stream_crossings.stream_crossings_community_staging a left join
stream_crossings.stream_crossings_community_holding b on a.id = b.id 
union
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 'dams' as feature_type, 'NEW'::varchar as status,
case when b.passability_status_code is null then 4 else b.passability_status_code end as passability_status_code
from dams.dams_community_staging a left join dams.dams b on b.cabd_id = a.cabd_id;