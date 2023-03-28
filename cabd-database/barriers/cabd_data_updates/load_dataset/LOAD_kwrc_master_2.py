import LOAD_crossings_main as main

script = main.LoadingScript("kwrc_master_2")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} RENAME COLUMN original_source TO data_source_name;
ALTER TABLE {script.sourceTable} RENAME COLUMN id TO cabd_assessment_id;
ALTER TABLE {script.sourceTable} ALTER COLUMN cabd_assessment_id TYPE uuid USING cabd_assessment_id::uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id uuid;
UPDATE {script.sourceTable} SET data_source_id = (SELECT id FROM cabd.data_source WHERE name = 'kennebecasis_wrc');
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (cabd_assessment_id);

ALTER TABLE {script.sourceTable} ALTER COLUMN geometry TYPE geometry(POINT, 2953) USING ST_Transform(geometry, 2953);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;

------------------------------------------
--nontidal crossings
------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.nonTidalSites};
CREATE TABLE {script.nonTidalSites} AS
    SELECT
        cabd_assessment_id::uuid,
        "crossing number" AS original_assessment_id,
        data_source_name,
        data_source_id,
        "date",
        "road type",
        "properly aligned (y/n)",
        "channel water depth inlet (cm)",
        "channel water depth outlet (cm)",
        "dry channel width inlet (cm)",
        "dry channel width outlet (cm)",
        "structure diameter inlet (cm)",
        "outflow water depth at scour (cm)",
        geometry
    FROM {script.sourceTable};

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN date_observed date,
    ADD COLUMN road_type_code integer,
    ADD COLUMN road_class varchar,
    ADD COLUMN road_surface varchar,
    ADD COLUMN site_type integer,
    ADD COLUMN alignment_code integer,
    ADD COLUMN upstream_channel_depth_m numeric,
    ADD COLUMN downstream_channel_depth_m numeric,
    ADD COLUMN upstream_bankfull_width_m numeric,
    ADD COLUMN downstream_bankfull_width_m numeric,
    ADD COLUMN constriction_code integer,
    ADD COLUMN tailwater_scour_pool_code integer,
    ADD COLUMN original_point geometry(Point,4617);

ALTER TABLE {script.nonTidalSites} ALTER COLUMN "date" TYPE date USING "date"::date;
UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.nonTidalSites} SET date_observed = "date";
UPDATE {script.nonTidalSites} SET road_type_code =
    CASE
    WHEN "road type" ILIKE '%highway%' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'multilane')
    WHEN "road type" ILIKE '%paved%' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'paved')
    WHEN "road type" = 'dirt road' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'unpaved')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET road_class = CASE WHEN "road type" ILIKE '%highway%' THEN 'highway' ELSE NULL END;
UPDATE {script.nonTidalSites} SET road_surface = CASE WHEN "road type" = 'dirt road' THEN 'dirt' ELSE NULL END;
UPDATE {script.nonTidalSites} SET site_type = (SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'unknown');
UPDATE {script.nonTidalSites} SET alignment_code =
    CASE
    WHEN "properly aligned (y/n)" = 'true' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN "properly aligned (y/n)" = 'false' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET upstream_channel_depth_m = ("channel water depth inlet (cm)"::numeric)/100;
UPDATE {script.nonTidalSites} SET downstream_channel_depth_m = ("channel water depth outlet (cm)"::numeric)/100 WHERE "channel water depth outlet (cm)" != 'na';
UPDATE {script.nonTidalSites} SET upstream_bankfull_width_m = ("dry channel width inlet (cm)"::numeric)/100;
UPDATE {script.nonTidalSites} SET downstream_bankfull_width_m = ("dry channel width outlet (cm)"::numeric)/100 WHERE "dry channel width outlet (cm)" != 'na';
--TO DO: CONFIRM THIS CALCULATION WITH ALEX
UPDATE {script.nonTidalSites} SET constriction_code = 
    CASE
    WHEN ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) < 0.5 THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'severe')
    WHEN ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) >= 0.5 AND ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) <= 0.94 THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'moderate')
    WHEN ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) >= 0.95 AND ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) <= 1 THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans only bankfull/active channel')
    WHEN ("structure diameter inlet (cm)"::numeric)/("dry channel width inlet (cm)"::numeric) >= 1 THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans full channel and banks')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET tailwater_scour_pool_code = (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'yes-extent unknown') WHERE "outflow water depth at scour (cm)" ~ '[0-9]+';

UPDATE {script.nonTidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'master_II' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'master_II' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'master_II' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.nonTidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.nonTidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- nontidal structures
------------------------------------------

DROP TABLE IF EXISTS {script.nonTidalStructures};

CREATE TABLE {script.nonTidalStructures} AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        source.armour,
        source.cwf_armour_inlet,
        source.cwf_armour_outlet,
        source."undercut outlet (cm)",
        source."structure diameter outlet (cm)",
        source."facility length (cm)",
        source."outflow water depth in culvert (cm)",
        source."outflow water depths at culvert lip (cm)",
        source."undercut inlet (cm)",
        source."structure diameter inlet (cm)",
        source."inlet water depth (cm)",
        source."slope (cm)",
        source."substrate composition (outlet) bedrock (%)",
        source."substrate composition (outlet) boulder (%)",
        source."substrate composition (outlet) cobble (%)",
        source."substrate composition (outlet) gravel (%)",
        source."substrate composition (outlet) sand (%)",
        source."substrate composition (outlet) fines (%)",
        source."velocity (m3/s)",
        source."obstructions/upstream debris",
        source."crushed inlet (%)",
        source."crushed outlet (%)",
        source."perforation (%)"
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE {script.nonTidalStructures}
    ADD COLUMN structure_id uuid,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_drop_to_water_surface_m numeric,
    ADD COLUMN outlet_drop_to_stream_bottom_m numeric,
    ADD COLUMN inlet_type_code integer, --TO DO: confirm with Alex if there is anything to be mapped to this field, else remove
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN structure_slope_pct numeric,
    ADD COLUMN structure_slope_confidence_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN water_velocity_matches_stream_code integer,
    ADD COLUMN passability_status_code integer;

UPDATE {script.nonTidalStructures} SET structure_id = gen_random_uuid();
ALTER TABLE {script.nonTidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.nonTidalStructures} ADD PRIMARY KEY (structure_id);

UPDATE {script.nonTidalStructures} SET outlet_armouring_code = 
    CASE
    WHEN armour IN ('n', 'no') THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN armour IN ('yes', 'Yes') THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'yes-extent unknown')
    WHEN cwf_armour_inlet is true AND cwf_armour_outlet is null THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    ELSE (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'yes-extent unknown') END;
UPDATE {script.nonTidalStructures} SET outlet_grade_code =
    CASE
    WHEN "undercut outlet (cm)"::numeric >= 3 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN "undercut outlet (cm)"::numeric < 3 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    ELSE NULL END;
UPDATE {script.nonTidalStructures} SET outlet_width_m = ("structure diameter outlet (cm)"::numeric)/100;
UPDATE {script.nonTidalStructures} SET structure_length_m = ("facility length (cm)"::numeric)/100;
UPDATE {script.nonTidalStructures} SET outlet_water_depth_m = ("outflow water depth in culvert (cm)"::numeric)/100 WHERE "outflow water depth in culvert (cm)" != 'N/a';
UPDATE {script.nonTidalStructures} SET outlet_drop_to_water_surface_m = ("undercut outlet (cm)"::numeric)/100;
UPDATE {script.nonTidalStructures} SET outlet_drop_to_stream_bottom_m =
    CASE
    WHEN ("outflow water depths at culvert lip (cm)" ~ '[0-9]+') THEN ("undercut outlet (cm)"::numeric + "outflow water depths at culvert lip (cm)"::numeric) / 100
    ELSE NULL END;
UPDATE {script.nonTidalStructures} SET "undercut inlet (cm)" = 8.00 WHERE "undercut inlet (cm)" = '8 right side';
UPDATE {script.nonTidalStructures} SET inlet_grade_code =
    CASE
    WHEN "undercut inlet (cm)"::numeric > 0 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN "undercut inlet (cm)"::numeric = 0 THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    ELSE NULL END;
UPDATE {script.nonTidalStructures} SET inlet_width_m = ("structure diameter inlet (cm)"::numeric)/100;
UPDATE {script.nonTidalStructures} SET inlet_water_depth_m = ("inlet water depth (cm)"::numeric)/100;
UPDATE {script.nonTidalStructures} SET structure_slope_pct = "slope (cm)"::numeric WHERE "slope (cm)" != 'N/a';
UPDATE {script.nonTidalStructures} SET structure_slope_confidence_code = CASE WHEN "slope (cm)" != 'N/a' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'high') ELSE NULL END;

UPDATE {script.nonTidalStructures} SET "substrate composition (outlet) bedrock (%)" = 100 WHERE "substrate composition (outlet) bedrock (%)" = 'rip rap';
ALTER TABLE {script.nonTidalStructures} ALTER COLUMN "substrate composition (outlet) bedrock (%)" TYPE double precision USING "substrate composition (outlet) bedrock (%)"::double precision;
ALTER TABLE {script.nonTidalStructures} ADD COLUMN substrate_type varchar;

--NOTE: substrate type calculation below will default to leftmost column if all values are equal
UPDATE {script.nonTidalStructures} SET substrate_type =
    CASE greatest(
	"substrate composition (outlet) bedrock (%)",
	"substrate composition (outlet) boulder (%)",
	"substrate composition (outlet) cobble (%)",
	"substrate composition (outlet) gravel (%)",
	"substrate composition (outlet) sand (%)",
	"substrate composition (outlet) fines (%)")
    WHEN "substrate composition (outlet) bedrock (%)" THEN 'bedrock'
    WHEN "substrate composition (outlet) boulder (%)" THEN 'boulder'
    WHEN "substrate composition (outlet) cobble (%)" THEN 'cobble'
    WHEN "substrate composition (outlet) gravel (%)" THEN 'gravel'
    WHEN "substrate composition (outlet) sand (%)" THEN 'sand'
    WHEN "substrate composition (outlet) fines (%)" THEN 'fines'
    ELSE NULL END;
UPDATE {script.nonTidalStructures} SET substrate_type_code =
    CASE
    WHEN substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN substrate_type = 'fines' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    ELSE NULL END;
ALTER TABLE {script.nonTidalStructures} DROP COLUMN substrate_type;

UPDATE {script.nonTidalStructures} SET water_velocity_matches_stream_code = (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown') WHERE "velocity (m3/s)" ~ '[0-9]+';

DELETE FROM {script.nonTidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM {script.nonTidalStructures});
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN "obstructions/upstream debris" = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN "obstructions/upstream debris" = 'yes' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    ELSE (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'sediment blockage')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM {script.nonTidalStructures}
    WHERE "obstructions/upstream debris" IS NOT NULL;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry') AS physical_barrier_code,
    cabd_assessment_id
    FROM {script.nonTidalStructures}
    WHERE "outflow water depth in culvert (cm)" ~ '[0-9]+' AND "outflow water depth in culvert (cm)"::numeric < 1;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation') AS physical_barrier_code,
    cabd_assessment_id
    FROM {script.nonTidalStructures}
    WHERE "crushed inlet (%)" != '0.00' OR "crushed outlet (%)" != '0.00' OR "perforation (%)" != '0.00';
"""

script.do_work(query)