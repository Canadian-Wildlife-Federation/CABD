-- This query will group modelled crossings that have the same chyf_stream_id
-- and transport_feature_id, and return one crossing from that group.
--
-- The rows returned by this query will direct you to areas where multiple stream
-- crossings exist on the same stream and transport feature (i.e., where the geometry
-- of a road may be closely following the geometry of a stream).
--
-- Make sure to update the {schema} values below to match the schema of interest.

drop table if exists {schema}.parallel_crossings;

create table {schema}.parallel_crossings as (
	select * from (
		select id, chyf_stream_id, transport_feature_id, geometry
			, row_number() over (partition by chyf_stream_id, transport_feature_id order by id desc) as rn
			, count(*) over (partition by chyf_stream_id, transport_feature_id) cn 
		from {schema}.modelled_crossings
	) t where cn > 1
	order by cn desc
);

grant select on {schema}.parallel_crossings to gistech;