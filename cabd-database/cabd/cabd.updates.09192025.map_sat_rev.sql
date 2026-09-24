-- This script maps satellite reviews to the new cwf_satellite_review table for the new stream crossings layer
-- To be done once when the new stream crossings layers were created

truncate table stream_crossings.cwf_satellite_review;

with i as (
	select 
		cabd_id,
		date_of_review,
		reviewer,
		case 
			when reviewer_status = 'checked' then 'REVIEWED'::stream_crossings.status_type
			else 'NEW'::stream_crossings.status_type
		end as status,
		coalesce(multipoint_feature, false) as multipoint_feature,
		coalesce(crossing_type, 'Unknown') as crossing_type,
		new_crossing_type,
		new_x,
		new_y,
		coalesce(driveway_crossing, false) as driveway_crossing,
		reviewer_comments
	from stream_crossings.satellite_review
	order by date_of_review desc
)
insert into stream_crossings.cwf_satellite_review (
	cabd_id,
	date_of_review,
	reviewer,
	status,
	multipoint_feature,
	crossing_type,
	new_crossing_type,
	new_dam_latitude,
	new_dam_longitude,
	driveway_crossing,
	reviewer_comments
)
select *
from i;

select *
from stream_crossings.cwf_satellite_review
where status='PROCESSED';

select *
from stream_crossings.sites_attribute_source
where cabd_id in (select cabd_id from stream_crossings.cwf_satellite_review where status='PROCESSED');
