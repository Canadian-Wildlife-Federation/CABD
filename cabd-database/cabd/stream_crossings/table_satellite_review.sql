-- drops and re-creates satellite review table
-- source: tbl_structure_stream_crossings_satellite_review.xlsx

-- notes:
-- added an original_point geometry field
-- added a crossing_type_code field that is automatically calculated based on the new_crossing_type_field
-- status is an int here, but you might want to use the string values instead
-- added date_of_review -> not in original xlsx file but in mapping file
drop table if exists stream_crossings.cwf_satellite_review;

create table stream_crossings.cwf_satellite_review (
    id uuid default gen_random_uuid() primary key,
    cabd_id	uuid default gen_random_uuid(),
    last_modified timestamp, -- autopopulated from last update
    reviewer varchar, --autopopulated from user
    status int default 1 references cabd.status_codes(code),
    multipoint_feature boolean default false,
    crossing_type varchar,
    new_crossing_type varchar,
    crossing_type_code int references stream_crossings.crossing_type_codes(code) 
        GENERATED always as (
            case 
              when new_crossing_type ilike 'open-bottom structure' then 1 
              when new_crossing_type ilike 'closed-bottom structure' then 2
              when new_crossing_type ilike 'multiple closed-bottom structure' then 3
              when new_crossing_type ilike 'ford-like structure' then 4
              when new_crossing_type ilike 'no crossing' then 5
              when new_crossing_type ilike 'removed crossing' then 6
              else null
            end)
        stored,
    create_dam boolean default false,
    existing_dam_cabd_id uuid,
    new_dam_latitude decimal,
    new_dam_longitude decimal,
    driveway_crossing boolean default false,
    reviewer_comments varchar,
    
    date_of_review date --not in original table structure but in mapping definition
);

INSERT INTO stream_crossings.cwf_satellite_review (ID,CABD_ID,LAST_MODIFIED,REVIEWER,STATUS,MULTIPOINT_FEATURE,CROSSING_TYPE,"new_crossing_type",CREATE_DAM,EXISTING_DAM_CABD_ID,NEW_DAM_LATITUDE,NEW_DAM_LONGITUDE,DRIVEWAY_CROSSING,REVIEWER_COMMENTS,DATE_OF_REVIEW) VALUES
	 ('89de3e0a-8610-4fbd-9aa1-883421365373'::uuid,'e0338c6c-0cbc-436d-a74b-becb7f97bd88'::uuid,NULL,'Emily',2,false,'No Crossing','No Crossing',false,NULL,NULL,NULL,true,'Test','2025-05-27');
