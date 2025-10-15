
---------------------------------------------------------------
--
--  All features with CanVec as the only data source.
--
---------------------------------------------------------------
with canvec_updates as (
	SELECT distinct cabd_id 
	FROM cabd.featureupdates_by_datasource('nrcan_canvec_mm')
	where raw_action not in ('INSERT')
	and new_datasource_id is not null
	UNION 
	SELECT distinct cabd_id
	FROM cabd.featureupdates_by_datasource('nrcan_canvec_hyf')
	where raw_action not in ('INSERT')
	and new_datasource_id is not null
	UNION
	SELECT distinct cabd_id 
	FROM cabd.featureupdates_by_datasource('canvec_track_nb')
	where raw_action not in ('INSERT')
	and new_datasource_id is not null
	),
canvec as (
	SELECT * FROM dams.dams_feature_source
	where datasource_id in (
		select id
		from cabd.data_source
		where name ilike '%canvec%'
	)
	UNION
	SELECT * FROM waterfalls.waterfalls_feature_source
	where datasource_id in (
		select id
		from cabd.data_source
		where name ilike '%canvec%'
	)
)
select * 
from cabd.all_features_view_en
where feature_type != 'stream_crossings' 
and cabd_id in (select cabd_id from canvec)
and cabd_id not in (select * from canvec_updates);
