create or replace view cabd.community_data_staging_view as 
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 'stream_crossings' as feature_type, case when b.status is null then 'NEW' else b.status::varchar end as status
from stream_crossings.stream_crossings_community_staging a left join
stream_crossings.stream_crossings_community_holding b on a.id = b.id 
union
select a.id, a.cabd_id, a.user_id, a.uploaded_datetime, a.data, 'dams' as feature_type, 'NEW'::varchar as status
from dams.dams_community_staging a 