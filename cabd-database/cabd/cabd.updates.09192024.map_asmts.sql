-------------------------
-- site_id had to be removed from certain triggers:  cabd.audit_structure_id_insert(); cabd.audit_structure_id_delete()
-- newpoint in assessment trigger needs to be original_point if null
-- need to assign geometries from nontidal/tidal sites if null in sites due to new point

-- x's used to keep track of portion being run.
-------------------------


----------------------------
-- xx acapsj_master_sheet sites
----------------------------
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,data_source_id
	,cabd_assessment_id
	,original_assessment_id
	,date_assessed
	,lead_assessor
	,stream_name
	,crossing_type_code
	,road_type_code
	,crossing_condition_code
	,flow_condition_code
	,status
	,crossing_comments
	,site_type_code
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.data_source_id
	,sd.cabd_assessment_id
	,sd.site as original_assessment_id
	,sd.date::date as date_assessed
	,sd."note taker" as lead_assessor
	,sd.stream as stream_name
	,sd.crossing_type_code
	,case
		when sd.road_type_code = 8 then 7
		when sd.road_type_code = 5 then 2
		when sd.road_type_code = 7 then 5
		when sd.road_type_code = 6 then 2
		else sd.road_type_code
	end as road_type_code
	,sd.crossing_condition_code
	,sd.flow_condition_code
	,'REQUIRES CLARIFICATION' as status
	,sd.notes
	,2
from
	source_data.acapsj_master_sheet sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
	where include = 'TRUE';

-- select *
-- from stream_crossings.assessment_data;

---------------------------------
-- xx acapsj_master_sheet structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_comments
	,outlet_shape_code
	,inlet_shape_code
	,substrate_type_code
	,outlet_width_m
	,inlet_width_m
	,internal_structures_code
	,inlet_grade_code
	,liner_material_code
	,structure_number
)
select
	a.id
	,sd.notes as structure_comments
	,sd.outlet_shape_code
	,sd.inlet_shape_code
	,sd.substrate_type_code
	,sd.outlet_width_m
	,sd.inlet_width_m
	,sd.internal_structures_code
	,sd.inlet_grade_code
	,sd.material_code
	,1 as structure_number
from
	source_data.acapsj_master_sheet sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id;

-- select *
-- from stream_crossings.assessment_structure_data;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;


-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id;




-------------------------------------
-- xx acapsj_salmon_creek_rothesay sites
-------------------------------------
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,crossing_type_code
	,status
	,crossing_comments
	,site_type_code
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.crossing_type_code
	,'REQUIRES CLARIFICATION' as status
	,sd.notes
	,2 as site_type_code
from
	source_data.acapsj_salmon_creek_rothesay sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where sd.include = 'TRUE';

-- select a.*
-- from stream_crossings.assessment_data a
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

---------------------------------
-- xx acapsj_salmon_creek_rothesay structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,physical_blockages_code
	,structure_number
)
select
	a.id
	,case
		when sd.physical_barriers_code = 3 then array[2]
		when sd.physical_barriers_code = 4 then array[3]
		when sd.physical_barriers_code = 5 then array[4]
		when sd.physical_barriers_code = 6 then array[7]
		when sd.physical_barriers_code = 7 then array[8]
		when sd.physical_barriers_code = 8 then array[10]
		when sd.physical_barriers_code = 9 then array[10]
		else null
	end
	,1 as structure_number
from
	source_data.acapsj_salmon_creek_rothesay sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id;

-- select a.*
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_salmon_creek_rothesay sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;




-------------------------------------
-- xx acapsj_stream_barriers sites
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,data_source_id
	,crossing_type_code
	,stream_name
	,crossing_condition_code
	,flow_condition_code
	,status
	,crossing_comments
	,site_type_code
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.data_source_id
	,sd.crossing_type_code
	,sd.location as stream_name
	,sd.crossing_condition_code
	,sd.flow_condition_code
	,'REQUIRES CLARIFICATION' as status
	,sd.notes as crossing_comments
	,sd.site_type_code
from
	source_data.acapsj_stream_barriers sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where include = 'TRUE'
	and site_type_code = 99;

	
--tidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,data_source_id
	,crossing_type_code
	,stream_name
	,crossing_condition_code
	,flow_condition_code
	,status
	,crossing_comments
	,site_type_code
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.data_source_id
	,sd.crossing_type_code
	,sd.location as stream_name
	,sd.crossing_condition_code
	,sd.flow_condition_code
	,'REQUIRES CLARIFICATION' as status
	,sd.notes as crossing_comments
	,sd.site_type_code
from
	source_data.acapsj_stream_barriers sd
join 
	stream_crossings.tidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where sd.include = 'TRUE';

-- select *
-- from stream_crossings.assessment_data a
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;


-------------------------------------
-- xx acapsj_stream_barriers structures
-------------------------------------

-- nontidal
-- need all the CTEs to increment the structure number
WITH existing_max AS (
    SELECT
        assessment_id,
        MAX(structure_number) AS max_num
    FROM stream_crossings.assessment_structure_data
    GROUP BY assessment_id
),
new_rows AS (
    SELECT
        a.id AS assessment_id,
        sd.stucture_length_m,
        sd.passability_status_code,
        sd.internal_structures_code + 1 AS internal_structures_code,
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.id) AS rn
    FROM source_data.acapsj_stream_barriers sd
    JOIN stream_crossings.nontidal_sites ns
        ON sd.cabd_assessment_id = ns.cabd_assessment_id
    JOIN stream_crossings.assessment_data a
        ON sd.cabd_assessment_id = a.cabd_assessment_id
)
INSERT INTO stream_crossings.assessment_structure_data (
    assessment_id,
    structure_length_m,
    passability_status_code,
    internal_structures_code,
    structure_number
)
SELECT
    n.assessment_id,
    n.stucture_length_m,
    n.passability_status_code,
    n.internal_structures_code,
    COALESCE(e.max_num, 0) + n.rn AS structure_number
FROM new_rows n
LEFT JOIN existing_max e
    ON n.assessment_id = e.assessment_id;

-- tidal
WITH existing_max AS (
    SELECT
        assessment_id,
        MAX(structure_number) AS max_num
    FROM stream_crossings.assessment_structure_data
    GROUP BY assessment_id
),
new_rows AS (
    SELECT
        a.id AS assessment_id,
        sd.stucture_length_m,
        sd.passability_status_code,
        sd.internal_structures_code + 1 AS internal_structures_code,
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.id) AS rn
    FROM source_data.acapsj_stream_barriers sd
    JOIN stream_crossings.tidal_sites ns
        ON sd.cabd_assessment_id = ns.cabd_assessment_id
    JOIN stream_crossings.assessment_data a
        ON sd.cabd_assessment_id = a.cabd_assessment_id
)
INSERT INTO stream_crossings.assessment_structure_data (
    assessment_id,
    structure_length_m,
    passability_status_code,
    internal_structures_code,
    structure_number
)
SELECT
    n.assessment_id,
    n.stucture_length_m,
    n.passability_status_code,
    n.internal_structures_code,
    COALESCE(e.max_num, 0) + n.rn AS structure_number
FROM new_rows n
LEFT JOIN existing_max e
    ON n.assessment_id = e.assessment_id;

-- select *
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.acapsj_stream_barriers sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;



-------------------------------------
-- xx acapsj_stream_crossing_layers sites
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,data_source_id
	,crossing_type_code
	,status
	,crossing_comments
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.data_source_id
	,sd.crossing_type_code
	,'REQUIRES CLARIFICATION' as status
	,sd.notes as crossing_comments
from
	source_data.acapsj_stream_crossing_layers sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where include = 'TRUE';

-- select *
-- from stream_crossings.assessment_data a
-- join source_data.acapsj_stream_crossing_layers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

---------------------------------
-- xx acapsj_stream_crossing_layers structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,substrate_type_code
	,outlet_width_m
	,outlet_height_m
	,liner_material_code
	,physical_blockages_code
	,structure_number
)
select
	a.id
	,sd.substrate_type_code
	,sd.outlet_width_m
	,sd.outlet_height_m
	,sd.material_code
	,case
		when sd.physical_barriers_code = 3 then array[2]
		when sd.physical_barriers_code = 4 then array[3]
		when sd.physical_barriers_code = 5 then array[4]
		when sd.physical_barriers_code = 6 then array[7]
		when sd.physical_barriers_code = 7 then array[8]
		when sd.physical_barriers_code = 8 then array[10]
		when sd.physical_barriers_code = 9 then array[10]
		else null
	end
	,1 as structure_number
from
	source_data.acapsj_stream_crossing_layers sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id;

-- select *
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.acapsj_stream_crossing_layers sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_crossing_layers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.acapsj_stream_barriers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_crossing_layers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.acapsj_stream_crossing_layers sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-------------------------------------
-- xx kwrc_bridges sites
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,original_assessment_id
	,data_source_id
	,status
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd."crossing number" as original_assessment_id
	,sd.data_source_id
	,'REQUIRES CLARIFICATION' as status
from
	source_data.kwrc_bridges sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where sd.latitude is not null
	and sd.longitude is not null;

-- select *
-- from stream_crossings.assessment_data a
-- join source_data.kwrc_bridges sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;


-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_bridges sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.kwrc_bridges sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;




-------------------------------------
-- xx kwrc_current_aug_6 sites
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,original_assessment_id
	,data_source_id
	,date_assessed
	,road_type_code
	,alignment_code
	,site_type_code
	,bankfull_width_upstr_a_m
	,bankfull_width_dnstr_a_m
	,scour_pool_tailwater_code
	,crossing_comments
	,status
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd."crossing number" as original_assessment_id
	,sd.data_source_id
	,sd."date"::date
	,case 
		when sd."road type" ilike '%highway%' then (select code from stream_crossings.road_type_codes where name_en = 'multilane')
		when sd."road type" ilike '%paved%' then (select code from stream_crossings.road_type_codes where name_en = 'paved')
		when sd."road type" in ('dirt road', 'gravel') then (select code from stream_crossings.road_type_codes where name_en = 'unpaved')
		else null
	end as road_type_code
	,case
		when sd."properly aligned (y/n)" like 'yes%' then (select code from stream_crossings.alignment_codes where name_en = 'flow-aligned')
		when sd."properly aligned (y/n)" like 'no' then (select code from stream_crossings.alignment_codes where name_en = 'skewed')
		else null
	end as alignment_code
	,(select code from stream_crossings.site_type_codes where name_en = 'unknown') as site_type_code
	,(select (sd."dry channel width inlet (cm)"::numeric)/100 WHERE "dry channel width inlet (cm)" ~ '[0-9]+') as bankfull_width_upstr_a_m
	,(select (sd."dry channel width outlet (cm)"::numeric)/100 WHERE "dry channel width outlet (cm)" ~ '[0-9]+') as bankfull_width_dnstr_a_m
	,case
		when sd."outflow water depth at scour (cm)" ~ '[0-9]+' then (select code from stream_crossings.scour_pool_codes where name_en = 'yes-extent unknown')
		when sd."outflow water depth at scour (cm)" ilike '%pond%' then (select code from stream_crossings.scour_pool_codes where name_en = 'none')
		else null
	end as scour_pool_tailwater_code
	,case 
		when sd."crossing number" = 'MR-MS-032' then 'CWF: this assessment point is likely in the wrong place; noted as Millstream River watershed but it is positioned in the Upper Kennebecasis'
		else null
	end as crossing_comments
	,'REQUIRES CLARIFICATION' as status
from
	source_data.kwrc_current_aug_6 sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where sd.latitude is not null
	and sd.longitude is not null;

-- select a.*
-- from stream_crossings.assessment_data a
-- join source_data.kwrc_current_aug_6 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;


---------------------------------
-- xx kwrc_current_aug_6 structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,outlet_height_m
	,structure_length_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_grade_code
	,inlet_width_m
	,inlet_water_depth_m
	,structure_slope_pct
	,substrate_type_code
	,water_velocity_matches_stream_code
	,structure_number
)
select
	a.id
	,case
		when sd.armour in ('n', 'no', 'none') then (select code from stream_crossings.armouring_codes where name_en = 'none')
		when sd.armour = 'yes' then (select code from stream_crossings.armouring_codes where name_en = 'yes-extent unknown')
		when sd.cwf_armour_inlet is true and sd.cwf_armour_outlet is null then (select code from stream_crossings.armouring_codes where name_en = 'none')
		else (select code from stream_crossings.armouring_codes where name_en = 'yes-extent unknown')
	end as outlet_armouring_code
	,case
		when sd."undercut outlet (cm)" ~ '[0-9]+' and sd."undercut outlet (cm)"::numeric >= 3 then (select code from stream_crossings.grade_codes where name_en = 'free fall')
		when sd."undercut outlet (cm)" ~ '[0-9]+' and sd."undercut outlet (cm)"::numeric < 3 then (select code from stream_crossings.grade_codes where name_en = 'at stream grade')
		else null
	end as outlet_grade_code
	,(select (sd."structure diameter outlet (cm)"::numeric)/100 where sd."structure diameter outlet (cm)" ~ '[0-9]+') as outlet_width_m
	,(select (sd."rise (cm)"::numeric)/100 where sd."rise (cm)" ~ '[0-9]+') as outlet_height_m
	,(select (sd."facility length (cm)"::numeric)/100) as structure_length_m
	,(select (sd."outflow water depth in culvert (cm)"::numeric)/100 where sd."outflow water depth in culvert (cm)" ~ '[0-9]+') as outlet_water_depth_m
	,(select (sd."undercut outlet (cm)"::numeric)/100 where sd."undercut outlet (cm)" ~ '[0-9]+') as outlet_drop_to_water_surface_m
	,case
		when sd."undercut outlet (cm)" ~ '[0-9]+' and sd."outflow water depths at culvert lip (cm)" ~ '[0-9]+' then (sd."undercut outlet (cm)"::numeric + sd."outflow water depths at culvert lip (cm)"::numeric)/100
		else null
	end as outlet_drop_to_stream_bottom_m
	,case
		when "undercut inlet (cm)" ~ '[0-9]+' and sd."undercut inlet (cm)"::numeric > 0 then (select code from stream_crossings.grade_codes where name_en = 'perched')
		when "undercut inlet (cm)" ~ '[0-9]+' and sd."undercut inlet (cm)"::numeric = 0 then (select code from stream_crossings.grade_codes where name_en = 'at stream grade')
		else null
	end as inlet_grade_code
	,(select ("structure diameter inlet (cm)"::numeric)/100 WHERE "structure diameter inlet (cm)" ~ '[0-9]+') as inlet_width_m
	,(select ("inlet water depth (cm)"::numeric)/100 WHERE "inlet water depth (cm)" ~ '[0-9]+') as inlet_water_depth_m
	,case
		when sd."rise (cm)" ilike '%n/a%' then null
		else (sd."rise (cm)"::double precision / sd."facility length (cm)") * 100
	end as structure_slope_pct
	,CASE GREATEST(
		0,
	    CASE WHEN sd."substrate composition (outlet) bedrock (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) bedrock (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) boulder (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) boulder (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) cobble (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) cobble (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) gravel (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) gravel (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) sand (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) sand (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) fines (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) fines (%)"::numeric ELSE 0 END
		)
		WHEN 0 then (select code from stream_crossings.substrate_type_codes where name_en = 'none')
	    WHEN sd."substrate composition (outlet) bedrock (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'bedrock')
	    WHEN sd."substrate composition (outlet) boulder (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'boulder')
	    WHEN sd."substrate composition (outlet) cobble (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'cobble')
	    WHEN sd."substrate composition (outlet) gravel (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'gravel')
	    WHEN sd."substrate composition (outlet) sand (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'sand')
	    WHEN sd."substrate composition (outlet) fines (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'silt')
	    ELSE NULL
	END AS substrate_type_code
	,(select code from stream_crossings.water_velocity_matches_stream_codes where name_en = 'unknown') as water_velocity_matches_stream_code
	,1 as structure_number
from
	source_data.kwrc_current_aug_6 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id;

-- select a.*
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.kwrc_current_aug_6 sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_current_aug_6 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.kwrc_current_aug_6 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_current_aug_6 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_current_aug_6 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;



-------------------------------------
-- xx kwrc_intermitted_dry_drains
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,original_assessment_id
	,data_source_id
	,crossing_comments
	,status
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd."crossing number"
	,sd.data_source_id
	,case
		when sd.classification = 'WCR' then 'water crossing road'
		else sd.classification
	end as crossing_comments 
	,'REQUIRES CLARIFICATION' as status
from
	source_data.kwrc_intermitted_dry_drains sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where (sd.classification != 'drain'
	or sd.classification is null)
	and sd."crossing number" not in ('Area of concern', 'Sed event', 'Sed event 2', 'Staining on road');

-- select a.*
-- from stream_crossings.assessment_data a
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;


---------------------------------
-- xx kwrc_intermitted_dry_drains structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_number
)
select
	a.id
	,1 as structure_number
from
	source_data.kwrc_intermitted_dry_drains sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id
 and sd.classification != 'drain';

-- select a.*
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

--xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_intermitted_dry_drains sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;




-------------------------------------
-- xx kwrc_master_2 sites
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,data_source_id
	,date_assessed
	,road_type_code
	,site_type_code
	,alignment_code
	,bankfull_width_upstr_a_m
	,bankfull_width_dnstr_a_m
	,scour_pool_tailwater_code
	,status
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.data_source_id
	,sd.date::date
	,case
		when sd."road type" ilike '%highway%' then (select code from stream_crossings.road_type_codes where name_en = 'multilane')
		when sd."road type" ilike '%paved%' then (select code from stream_crossings.road_type_codes where name_en = 'paved') 
		when sd."road type" ilike 'dirt road' then (select code from stream_crossings.road_type_codes where name_en = 'unpaved')
		else null
	end as road_type_code
	,(SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'unknown') as site_type
	,case
		when sd."properly aligned (y/n)" = 'true' then (select code from stream_crossings.alignment_codes where name_en = 'flow-aligned')
		when sd."properly aligned (y/n)" = 'false' then (select code from stream_crossings.alignment_codes where name_en = 'skewed')
		else null
	end as alignment_code
	,(sd."dry channel width inlet (cm)"::numeric)/100 as bankfull_width_upstr_a_m
	,(select (sd."dry channel width outlet (cm)"::numeric)/100 where sd."dry channel width outlet (cm)" != 'na') as bankfull_width_dnstr_a_m
	,(select code from stream_crossings.scour_pool_codes where name_en = 'yes-extent unknown' and sd."outflow water depth at scour (cm)" ~ '[0-9]+') as scour_pool_tailwater_code
	,'REQUIRES CLARIFICATION' as status
from
	source_data.kwrc_master_2 sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id;

-- select a.*
-- from stream_crossings.assessment_data a
-- join source_data.kwrc_master_2 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

---------------------------------
-- xx kwrc_master_2 structures
---------------------------------
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,structure_length_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_grade_code
	,inlet_width_m
	,inlet_water_depth_m
	,structure_slope_pct
	,substrate_type_code
	,water_velocity_matches_stream_code
	,physical_blockages_code
	,structure_number
)
select
	a.id
	,case
		when sd.armour in ('n', 'no') then (select code from stream_crossings.armouring_codes where name_en = 'none')
		when sd.armour in ('yes', 'Yes') then (select code from stream_crossings.armouring_codes where name_en = 'yes-extent unknown')
		when sd.cwf_armour_inlet is true and sd.cwf_armour_outlet is null then (select code from stream_crossings.armouring_codes where name_en = 'none')
		else (select code from stream_crossings.armouring_codes where name_en = 'yes-extent unknown')
	end as outlet_armouring_code
	,case
		when sd."undercut outlet (cm)"::numeric >= 3 then (select code from stream_crossings.grade_codes where name_en = 'free fall')
		when sd."undercut outlet (cm)"::numeric < 3 then (select code from stream_crossings.grade_codes where name_en = 'at stream grade')
		else null
	end as outlet_grade_code
	,(sd."structure diameter outlet (cm)"::numeric)/100 as oulet_width_m
	,(sd."facility length (cm)"::numeric)/100 as structure_length_m
	,(select (sd."outflow water depth in culvert (cm)"::numeric)/100 WHERE sd."outflow water depth in culvert (cm)" != 'N/a') as outlet_water_depth_m
	,(sd."undercut outlet (cm)"::numeric)/100
	,CASE
    	WHEN (sd."outflow water depths at culvert lip (cm)" ~ '[0-9]+') THEN (sd."undercut outlet (cm)"::numeric + sd."outflow water depths at culvert lip (cm)"::numeric) / 100
    	ELSE NULL 
	END as outlet_drop_to_stream_bottom_m
	,CASE
    	WHEN sd."undercut inlet (cm)" = '8 right side' or sd."undercut inlet (cm)"::numeric > 0 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    	WHEN sd."undercut inlet (cm)"::numeric = 0 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    	ELSE NULL 
	END as inlet_grade_code
	,(sd."structure diameter inlet (cm)"::numeric)/100 as inlet_width_m
	,(sd."inlet water depth (cm)"::numeric)/100 as inlet_water_depth_m
	,(
	    CASE 
	        WHEN sd."rise (cm)" ILIKE '%n/a%' THEN NULL
	        ELSE sd."rise (cm)"::double precision
	    END
	    / sd."facility length (cm)"
	) * 100 AS structure_slope_pct
	,CASE GREATEST(
		0,
		CASE WHEN sd."substrate composition (outlet) bedrock (%)" = 'rip rap' 
			THEN sd."substrate composition (outlet) boulder (%)"::numeric ELSE 0 END,
	    CASE WHEN sd."substrate composition (outlet) bedrock (%)" ~ '^[0-9]+(\.[0-9]+)?$' 
       		THEN sd."substrate composition (outlet) bedrock (%)"::numeric ELSE 0 END,
	    sd."substrate composition (outlet) boulder (%)"::numeric,
	    sd."substrate composition (outlet) cobble (%)"::numeric,
	    sd."substrate composition (outlet) gravel (%)"::numeric,
	    sd."substrate composition (outlet) sand (%)"::numeric,
	    sd."substrate composition (outlet) fines (%)"::numeric
		)
		WHEN 0 then (select code from stream_crossings.substrate_type_codes where name_en = 'none')
	    WHEN sd."substrate composition (outlet) bedrock (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'bedrock')
	    WHEN sd."substrate composition (outlet) boulder (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'boulder')
	    WHEN sd."substrate composition (outlet) cobble (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'cobble')
	    WHEN sd."substrate composition (outlet) gravel (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'gravel')
	    WHEN sd."substrate composition (outlet) sand (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'sand')
	    WHEN sd."substrate composition (outlet) fines (%)"::numeric THEN (select code from stream_crossings.substrate_type_codes where name_en = 'silt')
	    ELSE NULL
	END AS substrate_type_code
	,case 
		when "velocity (m3/s)" ~ '[0-9]+' then (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
		else null
	end as water_velocity_matches_stream_code
	,case 
		when sd."obstructions/upstream debris" = 'none' then array[(select code from stream_crossings.physical_barrier_codes where name_en = 'none')]
		when sd."obstructions/upstream debris" = 'yes' then array[(select code from stream_crossings.physical_barrier_codes where name_en = 'other')]
		when sd."obstructions/upstream debris" is not null then array[(select code from stream_crossings.physical_barrier_codes where name_en = 'sediment blockage')]
		when sd."outflow water depth in culvert (cm)" ~ '[0-9]+' and sd."outflow water depth in culvert (cm)"::numeric < 1 then array[(select code from stream_crossings.physical_barrier_codes where name_en = 'dry')]
		when sd."crushed inlet (%)" != '0.00' or sd."crushed outlet (%)" != '0.00' or sd."perforation (%)" != '0.00' then array[(select code from stream_crossings.physical_barrier_codes where name_en = 'deformation')]
		else null
	end as physical_blocakges_code
	,1 as structure_number
from
	source_data.kwrc_master_2 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id;

-- select a.*
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.kwrc_master_2 sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;

-- xx
update stream_crossings.assessment_data
set status = 'NEW'
where status = 'REQUIRES CLARIFICATION';

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_master_2 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.kwrc_master_2 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_master_2 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.kwrc_master_2 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;




-------------------------------------
-- peskotomuhkati_nation_01192023 sites
-- RUN MAPPING SECTION IN SMALL CHUNKS
--xx
-------------------------------------
--nontidal
insert into stream_crossings.assessment_data (
	id
	,cabd_id
	,cabd_assessment_id
	,original_assessment_id
	,data_source_id
	,date_assessed
	,stream_name
	,road_name
	,road_type_code
	,crossing_type_code
	,flow_condition_code
	,crossing_condition_code
	,site_type_code
	,alignment_code
	,road_fill_height_m
	,bankfull_width_dnstr_a_m
	,bankfull_confidence_code
	,scour_pool_tailwater_code
	,crossing_comments
	,status
)
select
	gen_random_uuid() as id
	,ns.cabd_id
	,sd.cabd_assessment_id
	,sd.site_id as original_assessment_id
	,sd.data_source_id
	,sd.date_observed::date
	,case
		when sd.stream_name = 'Unknown' then null
		else sd.stream_name
	end as stream_name
	,case
		when sd.road_name = 'Unknown' then null
		else sd.road_name
	end as road_name
	,case
		when sd.road_type = 'driveway' then (select code from stream_crossings.road_type_codes where name_en = 'driveway')
		when sd.road_type = 'unpaved' then (select code from stream_crossings.road_type_codes where name_en = 'unpaved')
		when sd.road_type = 'paved' then (select code from stream_crossings.road_type_codes where name_en = '1 or 2 lane paved')
		when sd.road_type = 'multilane' then (select code from stream_crossings.road_type_codes where name_en = 'multilane')
		when sd.road_type = 'trail' then (select code from stream_crossings.road_type_codes where name_en = 'trail')
		when sd.road_type = 'railroad' then (select code from stream_crossings.road_type_codes where name_en = 'railroad (active)')
		else null
	end as road_type_code
	,case
		when sd.crossing_type = 'bridge' then (select code from stream_crossings.crossing_type_codes where name_en = 'open-bottom structure')
		when sd.crossing_type = 'bridge_adequate' then (select code from stream_crossings.crossing_type_codes where name_en = 'open-bottom structure')
		when sd.crossing_type = 'buried_stream' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'culvert' then (select code from stream_crossings.crossing_type_codes where name_en = 'closed-bottom structure')
		when sd.crossing_type = 'ford' then (select code from stream_crossings.crossing_type_codes where name_en = 'ford-like structure')
		when sd.crossing_type = 'buried_stream' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'inaccessible' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'multiple_culvert' then (select code from stream_crossings.crossing_type_codes where name_en = 'multiple closed-bottom structures')
		when sd.crossing_type = 'no_crossing' then (select code from stream_crossings.crossing_type_codes where name_en = 'no crossing')
		when sd.crossing_type = 'no_upstream' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'no_upstream_channel' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'partially_unaccessible' then (select code from stream_crossings.crossing_type_codes where name_en = 'unknown')
		when sd.crossing_type = 'removed_crossing' then (select code from stream_crossings.crossing_type_codes where name_en = 'removed crossing')
		else null
	end as crossing_type_code
	,case
		when sd.flow_condition = 'high' then (select code from stream_crossings.flow_condition_codes where name_en = 'high')
		when sd.flow_condition = 'moderate' then (select code from stream_crossings.flow_condition_codes where name_en = 'moderate')
		when sd.flow_condition = 'no_flow' then (select code from stream_crossings.flow_condition_codes where name_en = 'dry')
		when sd.flow_condition = 'typical_low' then (select code from stream_crossings.flow_condition_codes where name_en = 'typical low')
		else null
	end as flow_condition_code
	,case
		when sd.crossing_condition in ('new', 'ok', 'poor', 'unknown') then (select code from stream_crossings.crossing_condition_codes where name_en = sd.crossing_condition)
		else null
	end as crossing_condition_codes
	,case
		when sd.tidal_site = 'no' then (select code from stream_crossings.site_type_codes where name_en = 'nontidal')
		when sd.tidal_site = 'unknown' then (select code from stream_crossings.site_type_codes where name_en = 'unknown')
		else null
	end as site_type_code
	,case
		when sd.alignment = 'aligned' then (select code from stream_crossings.alignment_codes where name_en = 'flow_aligned')
		when sd.alignment = 'floaw_aligned' then (select code from stream_crossings.alignment_codes where name_en = 'flow_aligned')
		when sd.alignment = 'skewed' then (select code from stream_crossings.alignment_codes where name_en = 'skewed')
		when sd.alignment = 'skewed_45_degrees' then (select code from stream_crossings.alignment_codes where name_en = 'skewed')
		else null
	end as alignment_code
	,case
		when sd.road_fill_height ~ '[0-9]+' and sd.road_fill_height::numeric > 100 then null
		when sd.road_fill_height ~ '[0-9]+' then sd.road_fill_height::numeric
		else null
	end as road_fill_height_m
	,case
		when sd.bankfull_width ~ '[0-9]+' THEN sd.bankfull_width::numeric
		else null
	end as bankkfull_width_dnstr_a_m
	,case
		WHEN bankfull_confidence = 'high' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'high')
	    WHEN bankfull_confidence = 'low' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'low')
	    WHEN bankfull_confidence = 'lowestimated' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'low')
		else null
	end as bankfull_confidence_code
	,CASE
	    WHEN tailwater_scour_pool IN ('l', 'large') THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'large')
	    WHEN tailwater_scour_pool IN ('s', 'small') THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'small')
	    WHEN tailwater_scour_pool = 'none' THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'none')
	    ELSE NULL 
	END as scour_pool_tailwater_code
	,case
		when sd.site_id = 'Mag529' then 'No road; ford that was removed?'
		else null
	end as crossing_comments
	,'REQUIRES CLARIFICATION' as status
from
	source_data.peskotomuhkati_nation_01192023 sd
join 
	stream_crossings.nontidal_sites ns on sd.cabd_assessment_id = ns.cabd_assessment_id
where sd.tidal_site in ('no', 'unknown')
	and sd.site_id != 'Mag533' -- duplicate
	;	

-- select a.*
-- from stream_crossings.assessment_data a
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id

---------------------------------
-- peskotomuhkati_nation_01192023 structures
--xx
-- ---------------------------------
-- Structure 1
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_number
	,structure_length_m
	,physical_blockages_code
	,physical_blockage_severity_code
	,outlet_shape_code
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,outlet_height_m
	,outlet_substrate_water_width_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_shape_code
	,inlet_type_code
	,inlet_grade_code
	,inlet_width_m
	,inlet_height_m
	,inlet_substrate_water_width_m
	,inlet_water_depth_m
	,internal_structures_code
 	,substrate_matches_stream_code
	,substrate_type_code
	,substrate_coverage_code
	,water_depth_matches_stream_code
	,water_velocity_matches_stream_code
	,dry_passage_code
	,height_above_dry_passage_m
	,structure_comments
	,passability_status_code
	,liner_material_code
)
select distinct
	a.id
	,1 as structure_number
	,case 
		when sd.structure_length ~'[0-9]+' then sd.structure_length::numeric
		else null
	end as structure_length_m
	,(
		select array_agg(
			(select code
			from stream_crossings.physical_barrier_codes pbc
			where pbc.name_en = case
									when x.val ilike '%debris' then 'debris'
									when x.val ilike 'deformation' then 'deformation'
									when x.val ilike '%dry%' then 'dry'
									when x.val = 'fencing' then 'fencing'
									when x.val in ('FF', 'Free_fall') then 'free falls'
									when x.val ilike 'none' then 'none'
									when x.val ilike 'other' then 'other'
								end)
		)
		from unnest(string_to_array(sd.physical_barriers, ',')) as  x(val)
	) as physical_barriers
	,CASE
	    WHEN sd.severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
	    WHEN sd.severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
	    WHEN sd.severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
	    WHEN sd.severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
		else null
	end as physical_blockage_severity_code
	,case 
		when sd.outlet_shape = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.outlet_shape = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.outlet_shape = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.outlet_shape = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.outlet_shape = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		when sd.outlet_shape = 'box' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape = 'box bridge' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		WHEN sd.outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'closed-bottom pipe arch culvert')
		WHEN sd.outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
		WHEN sd.outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
	else null
	end as outlet_shape_code
	,case
		when sd.outlet_armouring = 'extensive' then (select code from stream_crossings.armouring_codes where name_en = 'extensive')
		WHEN sd.outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
	    WHEN sd.outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
	    WHEN sd.outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive') 
	else null
	end as outlet_armouring_code
	,case
		when sd.outlet_grade = 'cascade' then (select code from stream_crossings.grade_codes where name_en = 'cascade')
		WHEN sd.outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
	    WHEN sd.outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
	    WHEN sd.outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as outlet_grade_code
	,case
		when sd.outlet_width ~'[0-9]+' then sd.outlet_width::numeric
		else null
	end as outlet_width_m
	,case
		when sd.outlet_height ~'[0-9]+' then sd.outlet_height::numeric
		else null
	end as outlet_height_m
	,case
		when sd.outlet_ww ~'[0-9]+' then sd.outlet_ww::numeric
		else null
	end as outlet_substrate_water_width_m
	,case
		when sd.outlet_depth ~'[0-9]+' then sd.outlet_depth::numeric
		else null
	end as outlet_water_depth_m
	,case
		when sd.outlet_drop_to_water_surface ~'[0-9]+' then sd.outlet_drop_to_water_surface::numeric
		else null
	end as outlet_drop_to_water_surface_m
	,case
		when sd.outlet_drop_to_stream_bottom ~'[0-9]+' then sd.outlet_drop_to_stream_bottom::numeric
		else null
	end as outlet_drop_to_stream_bottom_m
	,case 
		when sd.inlet_shape = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.inlet_shape = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.inlet_shape = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.inlet_shape = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.inlet_shape = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.inlet_shape = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
	else null
	end as inlet_shape_code
	,CASE
	    WHEN sd.inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
	    WHEN sd.inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
	    WHEN sd.inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
	    WHEN sd.inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
	    WHEN sd.inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
	    WHEN sd.inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
		else null
	end as inlet_type_code
	,CASE
	    WHEN sd.inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
	    WHEN sd.inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
	    WHEN sd.inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as inlet_grade_code
	,case
		when sd.inlet_width ~'[0-9]+' then sd.inlet_width::numeric
		else null
	end as inlet_width_m
	,case
		when sd.inlet_height ~'[0-9]+' then sd.inlet_height::numeric
		else null
	end as inlet_height_m
	,case
		when sd.inlet_ww ~'[0-9]+' then sd.inlet_ww::numeric
		else null
	end as inlet_substrate_water_width_m
	,case
		when sd.inlet_depth ~'[0-9]+' then sd.inlet_depth::numeric
		else null
	end as inlet_water_depth_m
	,CASE
	    WHEN sd.internal_structures ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
	    WHEN sd.internal_structures = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
		else null
	end as internal_structures_code
	,CASE
	    WHEN sd.structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
	    WHEN sd.structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
	    WHEN sd.structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
	    WHEN sd.structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
		else null
	end as substrate_matches_stream_code
	,CASE
	    WHEN sd.structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
	    WHEN sd.structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
	    WHEN sd.structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
	    WHEN sd.structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
	    WHEN sd.structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
	    WHEN sd.structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'silt')
	    WHEN sd.structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
		else null
	end as substrate_type_code
	,CASE
	    WHEN sd.structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
	    WHEN sd.structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
	    WHEN sd.structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
	    WHEN sd.structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
		else null
	end as substrate_coverage_code
	,CASE
	    WHEN sd.water_depth_matches_stream = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_depth_matches_stream = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_depth_matches_stream = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	end as water_depth_matches_stream_code
	,CASE 
	    WHEN sd.water_velocity_matches_stream = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_velocity_matches_stream = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	END as water_velocity_matches_stream_code
	,CASE
	    WHEN sd.dry_passage_through_structure = 'yes' THEN (select code from cabd.response_Codes where name_en = 'yes')
	    WHEN sd.dry_passage_through_structure = 'no' THEN (select code from cabd.response_Codes where name_en = 'no')
	    ELSE NULL
	end as dry_passage_code
	,case
		when sd.height_above_dry_passage ~'[0-9]+' then sd.height_above_dry_passage::numeric
		else null
	end as height_above_dry_passage_m
	,(select sd.structure_1_comments where sd.structure_1_comments is not null) || 
		'; other physical barriers: ' || 
		(select other_physical_barriers where sd.other_physical_barriers is not null) ||
		'; inlet comments: ' || (select sd.other_inlet_type where sd.other_inlet_type is not null and sd.other_inlet_type not in ('unknown', 'NA'))
	as structure_1_comments
	,case
		when sd.barrier_type = 'c-None' then (select code from cabd.passability_status_codes where name_en = 'Passable')
		else (select code from cabd.passability_status_codes where name_en = 'Barrier')
	end as passability_status_code
	,CASE
        WHEN sd.structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN sd.structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN sd.structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN sd.structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'rock/stone')
        WHEN sd.structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
      END AS liner_material_code
from
	source_data.peskotomuhkati_nation_01192023 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id;

-- Structure 2
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_number
	,structure_length_m
	,physical_blockages_code
	,physical_blockage_severity_code
	,outlet_shape_code
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,outlet_height_m
	,outlet_substrate_water_width_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_shape_code
	,inlet_type_code
	,inlet_grade_code
	,inlet_width_m
	,inlet_height_m
	,inlet_substrate_water_width_m
	,inlet_water_depth_m
	,internal_structures_code
 	,substrate_matches_stream_code
	,substrate_type_code
	,substrate_coverage_code
	,water_depth_matches_stream_code
	,water_velocity_matches_stream_code
	,dry_passage_code
	,height_above_dry_passage_m
	,structure_comments
	,passability_status_code
	,liner_material_code
)
select distinct
	a.id
	,2 as structure_number
	,case 
		when sd.structure_length_1 ~'[0-9]+' then sd.structure_length_1::numeric
		else null
	end as structure_length_m
	,(
		select array_agg(
			(select code
			from stream_crossings.physical_barrier_codes pbc
			where pbc.name_en = case
									when x.val ilike '%debris' then 'debris'
									when x.val ilike 'deformation' then 'deformation'
									when x.val ilike '%dry%' then 'dry'
									when x.val = 'fencing' then 'fencing'
									when x.val in ('FF', 'Free_fall') then 'free falls'
									when x.val ilike 'none' then 'none'
									when x.val ilike 'other' then 'other'
								end)
		)
		from unnest(string_to_array(sd.physical_barriers_1, ',')) as  x(val)
	) as physical_barriers
	,CASE
	    WHEN sd.severity_1 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
	    WHEN sd.severity_1 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
	    WHEN sd.severity_1 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
	    WHEN sd.severity_1 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
		else null
	end as physical_blockage_severity_code
	,case 
		when sd.outlet_shape_1 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.outlet_shape_1 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.outlet_shape_1 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.outlet_shape_1 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_1 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.outlet_shape_1 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		when sd.outlet_shape_1 = 'box' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_1 = 'box bridge' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		WHEN sd.outlet_shape_1 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'closed-bottom pipe arch culvert')
		WHEN sd.outlet_shape_1 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
		WHEN sd.outlet_shape_1 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
	else null
	end as outlet_shape_code
	,case
		when sd.outlet_armouring_1 = 'extensive' then (select code from stream_crossings.armouring_codes where name_en = 'extensive')
		WHEN sd.outlet_armouring_1 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
	    WHEN sd.outlet_armouring_1 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
	    WHEN sd.outlet_armouring_1 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive') 
	else null
	end as outlet_armouring_code
	,case
		when sd.outlet_grade_1 = 'cascade' then (select code from stream_crossings.grade_codes where name_en = 'cascade')
		WHEN sd.outlet_grade_1 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.outlet_grade_1 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
	    WHEN sd.outlet_grade_1 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
	    WHEN sd.outlet_grade_1 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.outlet_grade_1 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as outlet_grade_code
	,case
		when sd.outlet_width_1 ~'[0-9]+' then sd.outlet_width_1::numeric
		else null
	end as outlet_width_m
	,case
		when sd.outlet_height_1 ~'[0-9]+' then sd.outlet_height_1::numeric
		else null
	end as outlet_height_m
	,case
		when sd.outlet_ww_1 ~'[0-9]+' then sd.outlet_ww_1::numeric
		else null
	end as outlet_substrate_water_width_m
	,case
		when sd.outlet_depth_1 ~'[0-9]+' then sd.outlet_depth_1::numeric
		else null
	end as outlet_water_depth_m
	,case
		when sd.outlet_drop_to_water_surface_1 ~'[0-9]+' then sd.outlet_drop_to_water_surface_1::numeric
		else null
	end as outlet_drop_to_water_surface_m
	,case
		when sd.outlet_drop_to_stream_bottom_1 ~'[0-9]+' then sd.outlet_drop_to_stream_bottom_1::numeric
		else null
	end as outlet_drop_to_stream_bottom_m
	,case 
		when sd.inlet_shape_1 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.inlet_shape_1 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.inlet_shape_1 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.inlet_shape_1 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.inlet_shape_1 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.inlet_shape_1 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
	else null
	end as inlet_shape_code
	,CASE
	    WHEN sd.inlet_type_1 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
	    WHEN sd.inlet_type_1 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_1 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_1 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_1 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_1 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
	    WHEN sd.inlet_type_1 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
	    WHEN sd.inlet_type_1 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
	    WHEN sd.inlet_type_1 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
	    WHEN sd.inlet_type_1 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
		else null
	end as inlet_type_code
	,CASE
	    WHEN sd.inlet_grade_1 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.inlet_grade_1 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
	    WHEN sd.inlet_grade_1 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
	    WHEN sd.inlet_grade_1 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.inlet_grade_1 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as inlet_grade_code
	,case
		when sd.inlet_width_1 ~'[0-9]+' then sd.inlet_width_1::numeric
		else null
	end as inlet_width_m
	,case
		when sd.inlet_height_1 ~'[0-9]+' then sd.inlet_height_1::numeric
		else null
	end as inlet_height_m
	,case
		when sd.inlet_ww_1 ~'[0-9]+' then sd.inlet_ww_1::numeric
		else null
	end as inlet_substrate_water_width_m
	,case
		when sd.inlet_depth_1 ~'[0-9]+' then sd.inlet_depth_1::numeric
		else null
	end as inlet_water_depth_m
	,CASE
	    WHEN sd.internal_structures_1 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
	    WHEN sd.internal_structures_1 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
		else null
	end as internal_structures_code
	,CASE
	    WHEN sd.structure_substrate_matches_stream_1 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
	    WHEN sd.structure_substrate_matches_stream_1 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
	    WHEN sd.structure_substrate_matches_stream_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_matches_stream_1 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
	    WHEN sd.structure_substrate_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
		else null
	end as substrate_matches_stream_code
	,CASE
	    WHEN sd.structure_substrate_type_1 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
	    WHEN sd.structure_substrate_type_1 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
	    WHEN sd.structure_substrate_type_1 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
	    WHEN sd.structure_substrate_type_1 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
	    WHEN sd.structure_substrate_type_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_type_1 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
	    WHEN sd.structure_substrate_type_1 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'silt')
	    WHEN sd.structure_substrate_type_1 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
		else null
	end as substrate_type_code
	,CASE
	    WHEN sd.structure_substrate_coverage_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_coverage_1 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
	    WHEN sd.structure_substrate_coverage_1 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
	    WHEN sd.structure_substrate_coverage_1 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
	    WHEN sd.structure_substrate_coverage_1 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
		else null
	end as substrate_coverage_code
	,CASE
	    WHEN sd.water_depth_matches_stream_1 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_depth_matches_stream_1 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_1 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_1 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_1 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_depth_matches_stream_1 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	end as water_depth_matches_stream_code
	,CASE 
	    WHEN sd.water_velocity_matches_stream_1 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_1 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_1 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_1 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_1 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_1 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_velocity_matches_stream_1 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	END as water_velocity_matches_stream_code
	,CASE
	    WHEN sd.dry_passage_through_structure_1 = 'yes' THEN (select code from cabd.response_Codes where name_en = 'yes')
	    WHEN sd.dry_passage_through_structure_1 = 'no' THEN (select code from cabd.response_Codes where name_en = 'no')
	    ELSE NULL
	end as dry_passage_code
	,case
		when sd.height_above_dry_passage_1 ~'[0-9]+' then sd.height_above_dry_passage_1::numeric
		else null
	end as height_above_dry_passage_m
	,(select sd.structure_2_comments where sd.structure_2_comments is not null) || 
		'; other physical barriers: ' || 
		(select other_physical_barriers where sd.other_physical_barriers_1 is not null) ||
		'; inlet comments: ' || (select sd.other_inlet_type_1 where sd.other_inlet_type_1 is not null and sd.other_inlet_type_1 not in ('unknown', 'NA'))
	as structure_comments
	,case
		when sd.barrier_type = 'c-None' then (select code from cabd.passability_status_codes where name_en = 'Passable')
		else (select code from cabd.passability_status_codes where name_en = 'Barrier')
	end as passability_status_code
	,CASE
        WHEN sd.structure_material_1 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_1 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN sd.structure_material_1 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_1 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN sd.structure_material_1 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN sd.structure_material_1 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'rock/stone')
        WHEN sd.structure_material_1 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
      END AS liner_material_code
from
	source_data.peskotomuhkati_nation_01192023 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id
	and ns.structure_count >= 2;

-- Structure 3
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_number
	,structure_length_m
	,physical_blockages_code
	,physical_blockage_severity_code
	,outlet_shape_code
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,outlet_height_m
	,outlet_substrate_water_width_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_shape_code
	,inlet_type_code
	,inlet_grade_code
	,inlet_width_m
	,inlet_height_m
	,inlet_substrate_water_width_m
	,inlet_water_depth_m
	,internal_structures_code
 	,substrate_matches_stream_code
	,substrate_type_code
	,substrate_coverage_code
	,water_depth_matches_stream_code
	,water_velocity_matches_stream_code
	,dry_passage_code
	,height_above_dry_passage_m
	,structure_comments
	,passability_status_code
	,liner_material_code
)
select distinct
	a.id
	,3 as structure_number
	,case 
		when sd.structure_length_2 ~'[0-9]+' then sd.structure_length_2::numeric
		else null
	end as structure_length_m
	,(
		select array_agg(
			(select code
			from stream_crossings.physical_barrier_codes pbc
			where pbc.name_en = case
									when x.val ilike '%debris' then 'debris'
									when x.val ilike 'deformation' then 'deformation'
									when x.val ilike '%dry%' then 'dry'
									when x.val = 'fencing' then 'fencing'
									when x.val in ('FF', 'Free_fall') then 'free falls'
									when x.val ilike 'none' then 'none'
									when x.val ilike 'other' then 'other'
								end)
		)
		from unnest(string_to_array(sd.physical_barriers_2, ',')) as  x(val)
	) as physical_barriers
	,CASE
	    WHEN sd.severity_2 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
	    WHEN sd.severity_2 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
	    WHEN sd.severity_2 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
	    WHEN sd.severity_2 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
		else null
	end as physical_blockage_severity_code
	,case 
		when sd.outlet_shape_2 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.outlet_shape_2 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.outlet_shape_2 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.outlet_shape_2 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_2 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.outlet_shape_2 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		when sd.outlet_shape_2 = 'box' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_2 = 'box bridge' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		WHEN sd.outlet_shape_2 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'closed-bottom pipe arch culvert')
		WHEN sd.outlet_shape_2 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
		WHEN sd.outlet_shape_2 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
	else null
	end as outlet_shape_code
	,case
		when sd.outlet_armouring_2 = 'extensive' then (select code from stream_crossings.armouring_codes where name_en = 'extensive')
		WHEN sd.outlet_armouring_2 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
	    WHEN sd.outlet_armouring_2 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
	    WHEN sd.outlet_armouring_2 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive') 
	else null
	end as outlet_armouring_code
	,case
		when sd.outlet_grade_2 = 'cascade' then (select code from stream_crossings.grade_codes where name_en = 'cascade')
		WHEN sd.outlet_grade_2 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.outlet_grade_2 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
	    WHEN sd.outlet_grade_2 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
	    WHEN sd.outlet_grade_2 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.outlet_grade_2 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as outlet_grade_code
	,case
		when sd.outlet_width_2 ~'[0-9]+' then sd.outlet_width_2::numeric
		else null
	end as outlet_width_m
	,case
		when sd.outlet_height_2 ~'[0-9]+' then sd.outlet_height_2::numeric
		else null
	end as outlet_height_m
	,case
		when sd.outlet_ww_2 ~'[0-9]+' then sd.outlet_ww_2::numeric
		else null
	end as outlet_substrate_water_width_m
	,case
		when sd.outlet_depth_2 ~'[0-9]+' then sd.outlet_depth_2::numeric
		else null
	end as outlet_water_depth_m
	,case
		when sd.outlet_drop_to_water_surface_2 ~'[0-9]+' then sd.outlet_drop_to_water_surface_2::numeric
		else null
	end as outlet_drop_to_water_surface_m
	,case
		when sd.outlet_drop_to_stream_bottom_2 ~'[0-9]+' then sd.outlet_drop_to_stream_bottom_2::numeric
		else null
	end as outlet_drop_to_stream_bottom_m
	,case 
		when sd.inlet_shape_2 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.inlet_shape_2 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.inlet_shape_2 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.inlet_shape_2 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.inlet_shape_2 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.inlet_shape_2 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
	else null
	end as inlet_shape_code
	,CASE
	    WHEN sd.inlet_type_2 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
	    WHEN sd.inlet_type_2 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_2 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_2 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_2 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_2 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
	    WHEN sd.inlet_type_2 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
	    WHEN sd.inlet_type_2 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
	    WHEN sd.inlet_type_2 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
	    WHEN sd.inlet_type_2 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
		else null
	end as inlet_type_code
	,CASE
	    WHEN sd.inlet_grade_2 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.inlet_grade_2 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
	    WHEN sd.inlet_grade_2 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
	    WHEN sd.inlet_grade_2 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.inlet_grade_2 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as inlet_grade_code
	,case
		when sd.inlet_width_2 ~'[0-9]+' then sd.inlet_width_2::numeric
		else null
	end as inlet_width_m
	,case
		when sd.inlet_height_2 ~'[0-9]+' then sd.inlet_height_2::numeric
		else null
	end as inlet_height_m
	,case
		when sd.inlet_ww_2 ~'[0-9]+' then sd.inlet_ww_2::numeric
		else null
	end as inlet_substrate_water_width_m
	,case
		when sd.inlet_depth_2 ~'[0-9]+' then sd.inlet_depth_2::numeric
		else null
	end as inlet_water_depth_m
	,CASE
	    WHEN sd.internal_structures_2 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
	    WHEN sd.internal_structures_2 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
		else null
	end as internal_structures_code
	,CASE
	    WHEN sd.structure_substrate_matches_stream_2 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
	    WHEN sd.structure_substrate_matches_stream_2 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
	    WHEN sd.structure_substrate_matches_stream_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_matches_stream_2 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
	    WHEN sd.structure_substrate_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
		else null
	end as substrate_matches_stream_code
	,CASE
	    WHEN sd.structure_substrate_type_2 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
	    WHEN sd.structure_substrate_type_2 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
	    WHEN sd.structure_substrate_type_2 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
	    WHEN sd.structure_substrate_type_2 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
	    WHEN sd.structure_substrate_type_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_type_2 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
	    WHEN sd.structure_substrate_type_2 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'silt')
	    WHEN sd.structure_substrate_type_2 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
		else null
	end as substrate_type_code
	,CASE
	    WHEN sd.structure_substrate_coverage_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_coverage_2 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
	    WHEN sd.structure_substrate_coverage_2 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
	    WHEN sd.structure_substrate_coverage_2 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
	    WHEN sd.structure_substrate_coverage_2 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
		else null
	end as substrate_coverage_code
	,CASE
	    WHEN sd.water_depth_matches_stream_2 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_depth_matches_stream_2 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_2 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_2 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_2 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_depth_matches_stream_2 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	end as water_depth_matches_stream_code
	,CASE 
	    WHEN sd.water_velocity_matches_stream_2 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_2 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_2 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_2 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_2 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_2 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_velocity_matches_stream_2 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	END as water_velocity_matches_stream_code
	,CASE
	    WHEN sd.dry_passage_through_structure_2 = 'yes' THEN (select code from cabd.response_Codes where name_en = 'yes')
	    WHEN sd.dry_passage_through_structure_2 = 'no' THEN (select code from cabd.response_Codes where name_en = 'no')
	    ELSE NULL
	end as dry_passage_code
	,case
		when sd.height_above_dry_passage_2 ~'[0-9]+' then sd.height_above_dry_passage_2::numeric
		else null
	end as height_above_dry_passage_m
	,(select sd.structure_2_comments where sd.structure_2_comments is not null) || 
		'; other physical barriers: ' || 
		(select other_physical_barriers where sd.other_physical_barriers_2 is not null) ||
		'; inlet comments: ' || (select sd.other_inlet_type_2 where sd.other_inlet_type_2 is not null and sd.other_inlet_type_2 not in ('unknown', 'NA'))
	as structure_comments
	,case
		when sd.barrier_type = 'c-None' then (select code from cabd.passability_status_codes where name_en = 'Passable')
		else (select code from cabd.passability_status_codes where name_en = 'Barrier')
	end as passability_status_code
	,CASE
        WHEN sd.structure_material_2 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_2 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN sd.structure_material_2 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_2 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN sd.structure_material_2 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN sd.structure_material_2 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'rock/stone')
        WHEN sd.structure_material_2 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
      END AS liner_material_code
from
	source_data.peskotomuhkati_nation_01192023 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id
	and ns.structure_count >= 3;

-- Structure 4
insert into stream_crossings.assessment_structure_data (
	assessment_id
	,structure_number
	,structure_length_m
	,physical_blockages_code
	,physical_blockage_severity_code
	,outlet_shape_code
	,outlet_armouring_code
	,outlet_grade_code
	,outlet_width_m
	,outlet_height_m
	,outlet_substrate_water_width_m
	,outlet_water_depth_m
	,outlet_drop_to_water_surface_m
	,outlet_drop_to_stream_bottom_m
	,inlet_shape_code
	,inlet_type_code
	,inlet_grade_code
	,inlet_width_m
	,inlet_height_m
	,inlet_substrate_water_width_m
	,inlet_water_depth_m
	,internal_structures_code
 	,substrate_matches_stream_code
	,substrate_type_code
	,substrate_coverage_code
	,water_depth_matches_stream_code
	,water_velocity_matches_stream_code
	,dry_passage_code
	,height_above_dry_passage_m
	,structure_comments
	,passability_status_code
	,liner_material_code
)
select distinct
	a.id
	,4 as structure_number
	,case 
		when sd.structure_length_3 ~'[0-9]+' then sd.structure_length_3::numeric
		else null
	end as structure_length_m
	,(
		select array_agg(
			(select code
			from stream_crossings.physical_barrier_codes pbc
			where pbc.name_en = case
									when x.val ilike '%debris' then 'debris'
									when x.val ilike 'deformation' then 'deformation'
									when x.val ilike '%dry%' then 'dry'
									when x.val = 'fencing' then 'fencing'
									when x.val in ('FF', 'Free_fall') then 'free falls'
									when x.val ilike 'none' then 'none'
									when x.val ilike 'other' then 'other'
								end)
		)
		from unnest(string_to_array(sd.physical_barriers_3, ',')) as  x(val)
	) as physical_barriers
	,CASE
	    WHEN sd.severity_3 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
	    WHEN sd.severity_3 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
	    WHEN sd.severity_3 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
	    WHEN sd.severity_3 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
		else null
	end as physical_blockage_severity_code
	,case 
		when sd.outlet_shape_3 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.outlet_shape_3 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.outlet_shape_3 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.outlet_shape_3 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_3 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.outlet_shape_3 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		when sd.outlet_shape_3 = 'box' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.outlet_shape_3 = 'box bridge' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
		WHEN sd.outlet_shape_3 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'closed-bottom pipe arch culvert')
		WHEN sd.outlet_shape_3 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
		WHEN sd.outlet_shape_3 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
	else null
	end as outlet_shape_code
	,case
		when sd.outlet_armouring_3 = 'extensive' then (select code from stream_crossings.armouring_codes where name_en = 'extensive')
		WHEN sd.outlet_armouring_3 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
	    WHEN sd.outlet_armouring_3 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
	    WHEN sd.outlet_armouring_3 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive') 
	else null
	end as outlet_armouring_code
	,case
		when sd.outlet_grade_3 = 'cascade' then (select code from stream_crossings.grade_codes where name_en = 'cascade')
		WHEN sd.outlet_grade_3 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.outlet_grade_3 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
	    WHEN sd.outlet_grade_3 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
	    WHEN sd.outlet_grade_3 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.outlet_grade_3 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as outlet_grade_code
	,case
		when sd.outlet_width_3 ~'[0-9]+' then sd.outlet_width_3::numeric
		else null
	end as outlet_width_m
	,case
		when sd.outlet_height_3 ~'[0-9]+' then sd.outlet_height_3::numeric
		else null
	end as outlet_height_m
	,case
		when sd.outlet_ww_3 ~'[0-9]+' then sd.outlet_ww_3::numeric
		else null
	end as outlet_substrate_water_width_m
	,case
		when sd.outlet_depth_3 ~'[0-9]+' then sd.outlet_depth_3::numeric
		else null
	end as outlet_water_depth_m
	,case
		when sd.outlet_drop_to_water_surface_3 ~'[0-9]+' then sd.outlet_drop_to_water_surface_3::numeric
		else null
	end as outlet_drop_to_water_surface_m
	,case
		when sd.outlet_drop_to_stream_bottom_3 ~'[0-9]+' then sd.outlet_drop_to_stream_bottom_3::numeric
		else null
	end as outlet_drop_to_stream_bottom_m
	,case 
		when sd.inlet_shape_3 = '1' then (select code from stream_crossings.shape_codes where name_en = 'round culvert')
		when sd.inlet_shape_3 = '2' then (select code from stream_crossings.shape_codes where name_en = 'closed-bottom pipe arch culvert')
		when sd.inlet_shape_3 = '3' then (select code from stream_crossings.shape_codes where name_en = 'open bottom arch bridge/culvert')
		when sd.inlet_shape_3 = '4' then (select code from stream_crossings.shape_codes where name_en = 'box culvert')
		when sd.inlet_shape_3 = '5' then (select code from stream_crossings.shape_codes where name_en = 'bridge with side slopes')
		when sd.inlet_shape_3 = '6' then (select code from stream_crossings.shape_codes where name_en = 'box/bridge with abutmets')
	else null
	end as inlet_shape_code
	,CASE
	    WHEN sd.inlet_type_3 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
	    WHEN sd.inlet_type_3 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_3 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
	    WHEN sd.inlet_type_3 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_3 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
	    WHEN sd.inlet_type_3 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
	    WHEN sd.inlet_type_3 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
	    WHEN sd.inlet_type_3 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
	    WHEN sd.inlet_type_3 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
	    WHEN sd.inlet_type_3 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
		else null
	end as inlet_type_code
	,CASE
	    WHEN sd.inlet_grade_3 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
	    WHEN sd.inlet_grade_3 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
	    WHEN sd.inlet_grade_3 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
	    WHEN sd.inlet_grade_3 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
	    WHEN sd.inlet_grade_3 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
		else null
	end as inlet_grade_code
	,case
		when sd.inlet_width_3 ~'[0-9]+' then sd.inlet_width_3::numeric
		else null
	end as inlet_width_m
	,case
		when sd.inlet_height_3 ~'[0-9]+' then sd.inlet_height_3::numeric
		else null
	end as inlet_height_m
	,case
		when sd.inlet_ww_3 ~'[0-9]+' then sd.inlet_ww_3::numeric
		else null
	end as inlet_substrate_water_width_m
	,case
		when sd.inlet_depth_3 ~'[0-9]+' then sd.inlet_depth_3::numeric
		else null
	end as inlet_water_depth_m
	,CASE
	    WHEN sd.internal_structures_3 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
	    WHEN sd.internal_structures_3 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
		else null
	end as internal_structures_code
	,CASE
	    WHEN sd.structure_substrate_matches_stream_3 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
	    WHEN sd.structure_substrate_matches_stream_3 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
	    WHEN sd.structure_substrate_matches_stream_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_matches_stream_3 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
	    WHEN sd.structure_substrate_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
		else null
	end as substrate_matches_stream_code
	,CASE
	    WHEN sd.structure_substrate_type_3 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
	    WHEN sd.structure_substrate_type_3 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
	    WHEN sd.structure_substrate_type_3 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
	    WHEN sd.structure_substrate_type_3 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
	    WHEN sd.structure_substrate_type_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_type_3 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
	    WHEN sd.structure_substrate_type_3 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'silt')
	    WHEN sd.structure_substrate_type_3 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
		else null
	end as substrate_type_code
	,CASE
	    WHEN sd.structure_substrate_coverage_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
	    WHEN sd.structure_substrate_coverage_3 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
	    WHEN sd.structure_substrate_coverage_3 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
	    WHEN sd.structure_substrate_coverage_3 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
	    WHEN sd.structure_substrate_coverage_3 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
		else null
	end as substrate_coverage_code
	,CASE
	    WHEN sd.water_depth_matches_stream_3 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_depth_matches_stream_3 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_3 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
	    WHEN sd.water_depth_matches_stream_3 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_3 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
	    WHEN sd.water_depth_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_depth_matches_stream_3 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	end as water_depth_matches_stream_code
	,CASE 
	    WHEN sd.water_velocity_matches_stream_3 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_3 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
	    WHEN sd.water_velocity_matches_stream_3 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_3 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
	    WHEN sd.water_velocity_matches_stream_3 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_3 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
	    WHEN sd.water_velocity_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
	    WHEN sd.water_velocity_matches_stream_3 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
	    ELSE NULL 
	END as water_velocity_matches_stream_code
	,CASE
	    WHEN sd.dry_passage_through_structure_3 = 'yes' THEN (select code from cabd.response_Codes where name_en = 'yes')
	    WHEN sd.dry_passage_through_structure_3 = 'no' THEN (select code from cabd.response_Codes where name_en = 'no')
	    ELSE NULL
	end as dry_passage_code
	,case
		when sd.height_above_dry_passage_3 ~'[0-9]+' then sd.height_above_dry_passage_3::numeric
		else null
	end as height_above_dry_passage_m
	,(select sd.structure_3_comments where sd.structure_3_comments is not null) || 
		'; other physical barriers: ' || 
		(select other_physical_barriers where sd.othe_physical_barriers_3 is not null) ||
		'; inlet comments: ' || (select sd.other_inlet_type_3 where sd.other_inlet_type_3 is not null and sd.other_inlet_type_3 not in ('unknown', 'NA'))
	as structure_comments
	,case
		when sd.barrier_type = 'c-None' then (select code from cabd.passability_status_codes where name_en = 'Passable')
		else (select code from cabd.passability_status_codes where name_en = 'Barrier')
	end as passability_status_code
	,CASE
        WHEN sd.structure_material_3 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_3 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN sd.structure_material_3 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN sd.structure_material_3 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN sd.structure_material_3 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN sd.structure_material_3 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'rock/stone')
        WHEN sd.structure_material_3 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
      END AS liner_material_code
from
	source_data.peskotomuhkati_nation_01192023 sd
join stream_crossings.nontidal_sites ns
		on sd.cabd_assessment_id = ns.cabd_assessment_id
join stream_crossings.assessment_data a
		on sd.cabd_assessment_id = a.cabd_assessment_id
where sd.cabd_assessment_id = ns.cabd_assessment_id
	and ns.structure_count >= 4;

-- select a.*
-- from stream_crossings.assessment_structure_data a
-- join stream_crossings.assessment_data ad
-- 	on a.assessment_id = ad.id
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on ad.cabd_assessment_id = sd.cabd_assessment_id;


-- TEST ON SMALLER LIMIT UNTIL ABLE TO UPDATE IN TIMELY MANNER
update stream_crossings.assessment_data
set status = 'NEW'
where id in (
	select id
	from stream_crossings.assessment_data
	where status = 'REQUIRES CLARIFICATION'
	limit 640);

update stream_crossings.sites s
set original_point = ns.original_point
from stream_crossings.nontidal_sites ns
where s.cabd_id = ns.cabd_id and s.original_point is null and ns.original_point is not null;

-- select s.*
-- from stream_crossings.sites s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.sites_attribute_source s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_id = a.cabd_id
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures s
-- join stream_crossings.assessment_data a
-- 	on s.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;

-- select s.*
-- from stream_crossings.structures_attribute_source s 
-- join stream_crossings.structures st
-- 	on s.structure_id = st.structure_id
-- join stream_crossings.assessment_data a
-- 	on st.cabd_assessment_id = a.cabd_assessment_id
-- join source_data.peskotomuhkati_nation_01192023 sd
-- 	on a.cabd_assessment_id = sd.cabd_assessment_id;