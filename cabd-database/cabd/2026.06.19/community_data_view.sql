create or replace view cabd.community_data_staging_view as 
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 
'stream_crossings' as feature_type, 
case when b.status is null then 'NEW' else b.status::varchar end as status,
case when c.passability_status_code is null then 4 else c.passability_status_code end as passability_status_code
from stream_crossings.stream_crossings_community_staging a left join
stream_crossings.stream_crossings_community_holding b on a.id = b.id 
left join stream_crossings.structures c on c.site_id = b.cabd_id and c.primary_structure
union
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 'dams' as feature_type, 'NEW'::varchar as status,
case when b.passability_status_code is null then 4 else b.passability_status_code end as passability_status_code
from dams.dams_community_staging a left join dams.dams b on b.cabd_id = a.cabd_id