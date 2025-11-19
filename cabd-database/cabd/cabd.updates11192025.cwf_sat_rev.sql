
alter table stream_crossings.cwf_satellite_review
drop column crossing_type_code;


-- Create type removing values that you want to drop, adding any new values, or rearranging the order
-- Name it something different from the existing type
CREATE TYPE stream_crossings.new_crossing_type_t AS ENUM
    ('open-bottom structure', 'closed-bottom structure', 'multiple closed-bottom structure', 'ford-like structure', 'no crossing', 'removed crossing', 'new dam', 'existing dam', 'NULL');
ALTER TYPE stream_crossings.new_crossing_type_t
    OWNER TO cabd;

-- Intermediate step: remove the instances of the undesired value from all tables that use the enum type
-- For example, in this case, you would need to find all features in stream_crossings.cwf_satellite_review
-- with new_crossing_type = 'dam' and either delete those or change that type to something else 

-- Alter the table using the enum type to use the new enum type
alter table stream_crossings.cwf_satellite_review
alter column new_crossing_type type stream_crossings.new_crossing_type_t
using (new_crossing_type::text::stream_crossings.new_crossing_type_t);

-- Drop the old enum type
drop type stream_crossings.new_crossing_type;

-- Rename the enum type
alter type stream_crossings.new_crossing_type_t rename to new_crossing_type;

alter table stream_crossings.cwf_satellite_review
add column crossing_type_code integer GENERATED ALWAYS AS (
CASE
    WHEN (new_crossing_type = 'open-bottom structure'::stream_crossings.new_crossing_type) THEN 1
    WHEN (new_crossing_type = 'closed-bottom structure'::stream_crossings.new_crossing_type) THEN 2
    WHEN (new_crossing_type = 'multiple closed-bottom structure'::stream_crossings.new_crossing_type) THEN 3
    WHEN (new_crossing_type = 'ford-like structure'::stream_crossings.new_crossing_type) THEN 4
    WHEN (new_crossing_type = 'no crossing'::stream_crossings.new_crossing_type) THEN 5
    WHEN (new_crossing_type = 'removed crossing'::stream_crossings.new_crossing_type) THEN 6
    ELSE NULL::integer
END) STORED;

alter table stream_crossings.cwf_satellite_review
drop column create_dam;