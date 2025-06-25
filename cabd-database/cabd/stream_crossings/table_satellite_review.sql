-- drops and re-creates satellite review table
-- source: tbl_structure_stream_crossings_satellite_review.xlsx

-- notes:
-- added a crossing_type_code field that is automatically calculated based on the new_crossing_type_field
-- status is an int here, but you might want to use the string values instead
-- added date_of_review -> not in original xlsx file but in mapping file
drop table if exists stream_crossings.cwf_satellite_review;

drop type if exists
    stream_crossings.status_type,
    stream_crossings.new_crossing_type;

-- Enum type columns appear as dropdowns when the layer is pulled into QGIS.
-- Unfortunately, NULL is not an option that can appear in a dropdown in QGIS but the type can be 
-- set to NULL. One way to get around this is to have a 'NULL' option in the enum and a trigger to set this value to NULL
create type stream_crossings.status_type as enum('NEW', 'REVIEWED', 'PROCESSED', 'ERROR/WARNING', 'REQUIRES CLARIFICATION');
CREATE TYPE stream_crossings.new_crossing_type AS ENUM('open-bottom structure', 'closed-bottom structure', 'multiple closed-bottom structure', 'ford-like structure', 'no crossing', 'removed crossing', 'NULL');

create table stream_crossings.cwf_satellite_review (
    id uuid default gen_random_uuid() primary key,
    cabd_id	uuid default gen_random_uuid(),
    last_modified timestamp, -- autopopulated from last update
    reviewer varchar, --autopopulated from user
    status stream_crossings.status_type,
    multipoint_feature boolean, -- setting default values causes qgis to display as text field
    crossing_type varchar,
    new_crossing_type stream_crossings.new_crossing_type,
    create_dam boolean,
    existing_dam_cabd_id uuid,
    new_dam_latitude decimal,
    new_dam_longitude decimal,
    driveway_crossing boolean,
    reviewer_comments varchar,
    
    date_of_review date --not in original table structure but in mapping definition
);

GRANT ALL ON TABLE stream_crossings.satellite_review TO cabd;

GRANT ALL ON TABLE stream_crossings.satellite_review TO cwf_analyst;

GRANT UPDATE, DELETE, INSERT, SELECT ON TABLE stream_crossings.satellite_review TO cwf_user;

INSERT INTO stream_crossings.cwf_satellite_review (ID,CABD_ID,LAST_MODIFIED,REVIEWER,STATUS,MULTIPOINT_FEATURE,CROSSING_TYPE,"new_crossing_type",CREATE_DAM,EXISTING_DAM_CABD_ID,NEW_DAM_LATITUDE,NEW_DAM_LONGITUDE,DRIVEWAY_CROSSING,REVIEWER_COMMENTS,DATE_OF_REVIEW) VALUES
	 ('89de3e0a-8610-4fbd-9aa1-883421365373'::uuid,'e0338c6c-0cbc-436d-a74b-becb7f97bd88'::uuid,NULL,'Emily','REVIEWED',false,'No Crossing','no crossing',false,NULL,NULL,NULL,true,'Test','2025-05-27');
