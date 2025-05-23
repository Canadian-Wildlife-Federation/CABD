-- drops and re-creates satellite review table
-- source: tbl_structure_stream_crossings_satellite_review.xlsx

-- notes:
-- added an original_point geometry field
-- added a crossing_type_code field that is automatically calculated based on the new_crossing_type_field
-- status is an int here, but you might want to use the string values instead

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
    create_dam boolean	default false,
    existing_dam_cabd_id uuid,
    new_dam_latitude decimal,
    new_dam_longitude decimal,
    driveway_crossing boolean default false,
    reviewer_comments varchar,
    original_point public.geometry(point, 4617) NULL
);
