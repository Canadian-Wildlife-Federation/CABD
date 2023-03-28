import LOAD_crossings_main as main

script = main.LoadingScript("peskotomuhkati_nation_01192023")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} RENAME COLUMN original_source TO data_source_name;
ALTER TABLE {script.sourceTable} RENAME COLUMN id TO cabd_assessment_id;
ALTER TABLE {script.sourceTable} ALTER COLUMN cabd_assessment_id TYPE uuid USING cabd_assessment_id::uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id uuid;
UPDATE {script.sourceTable} SET data_source_id = (SELECT id FROM cabd.data_source WHERE name = 'peskotomuhkati_nation');
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (cabd_assessment_id);

ALTER TABLE {script.sourceTable} ALTER COLUMN geometry TYPE geometry(POINT, 2953) USING ST_Transform(geometry, 2953);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;

--fix no_culverts values
UPDATE {script.sourceTable} SET no_culverts = 
    CASE
    WHEN (no_culverts = 'NA' AND crossing_type = 'multiple_culvert' 
        AND structure_length IS NOT NULL AND structure_length != 'NA'
        AND structure_length_1 IS NOT NULL AND structure_length_1 != 'NA'
        AND structure_length_2 IS NOT NULL AND structure_length_2 != 'NA') THEN '3'
    WHEN (no_culverts = 'NA' AND crossing_type = 'culvert'
        AND structure_length IS NOT NULL AND structure_length != 'NA'
        AND structure_length_1 IS NOT NULL AND structure_length_1 != 'NA') THEN '2'
    ELSE no_culverts END;

------------------------------------------
--nontidal crossings
------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.nonTidalSites};
CREATE TABLE {script.nonTidalSites} AS
    SELECT
        cabd_assessment_id::uuid,
        site_id AS original_assessment_id,
        data_source_name,
        data_source_id,
        date_observed,
        stream_name,
        road_name,
        road_type,
        crossing_type,
        no_culverts,
        flow_condition,
        crossing_condition,
        tidal_site,
        alignment,
        road_fill_height,
        bankfull_width,
        bankfull_confidence,
        tailwater_scour_pool,
        constriction,
        crossing_comments,
        structure_length,
        geometry
    FROM {script.sourceTable}
    WHERE tidal_site IN ('no', 'unknown');

DELETE FROM {script.nonTidalSites} WHERE original_assessment_id = 'Mag533'; --this is a duplicate of another point
UPDATE {script.nonTidalSites} SET crossing_comments = 'No road; ford that was removed?' WHERE original_assessment_id = 'Mag529'; --add comment to the duplicate of Mag533

ALTER TABLE {script.nonTidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.nonTidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.nonTidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.nonTidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN road_type_code integer,
    ADD COLUMN crossing_type_code integer,
    ADD COLUMN structure_count integer,
    ADD COLUMN flow_condition_code integer,
    ADD COLUMN crossing_condition_code integer,
    ADD COLUMN site_type integer,
    ADD COLUMN alignment_code integer,
    ADD COLUMN road_fill_height_m numeric,
    ADD COLUMN upstream_channel_depth_m numeric,
    ADD COLUMN downstream_channel_depth_m numeric,
    ADD COLUMN upstream_bankfull_width_m numeric,
    ADD COLUMN downstream_bankfull_width_m numeric,
    ADD COLUMN upstream_bankfull_width_confidence_code integer,
    ADD COLUMN downstream_bankfull_width_confidence_code integer,
    ADD COLUMN constriction_code integer,
    ADD COLUMN tailwater_scour_pool_code integer,
    ADD COLUMN original_point geometry(Point,4617);

ALTER TABLE {script.nonTidalSites} ALTER COLUMN date_observed TYPE date USING date_observed::date;

UPDATE {script.nonTidalSites} SET stream_name = NULL WHERE stream_name = 'Unknown';
UPDATE {script.nonTidalSites} SET road_name = NULL WHERE road_name = 'Unknown';
UPDATE {script.nonTidalSites} SET road_type_code = 
    CASE
    WHEN road_type = 'driveway' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'driveway')
    WHEN road_type = 'unpaved' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'unpaved')
    WHEN road_type = 'paved' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'paved')
    WHEN road_type = 'multilane' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'multilane')
    WHEN road_type = 'trail' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'trail')
    WHEN road_type = 'railroad' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'railroad')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET crossing_type_code = 
    CASE
    WHEN crossing_type = 'bridge' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'bridge')
    WHEN crossing_type = 'bridge_adequate' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'bridge adequate')
    WHEN crossing_type = 'buried_stream' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'buried stream')
    WHEN crossing_type = 'culvert' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'culvert')
    WHEN crossing_type = 'ford' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'ford')
    WHEN crossing_type = 'inaccessible' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'inaccessible')
    WHEN crossing_type = 'multiple_culvert' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'multiple culvert')
    WHEN crossing_type = 'no_crossing' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no crossing')
    WHEN crossing_type = 'no_upstream' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no upstream channel')
    WHEN crossing_type = 'no_upstream_channel' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no upstream channel')
    WHEN crossing_type = 'partially_inaccessible' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'partially inaccessible')
    WHEN crossing_type = 'removed_crossing' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'removed crossing')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET structure_count = 
    CASE
    WHEN no_culverts ~ '[0-9]+' THEN no_culverts::integer
    WHEN (no_culverts = 'NA' AND structure_length IS NOT NULL AND structure_length != 'NA') THEN 1
    WHEN (no_culverts = 'NA' AND structure_length = 'NA') THEN 1
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET flow_condition_code =
    CASE
    WHEN flow_condition = 'high' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'high flow')
    WHEN flow_condition = 'moderate' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'moderate flow')
    WHEN flow_condition = 'no_flow' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'dewatered')
    WHEN flow_condition = 'typical_low' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'typical low flow')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET crossing_condition_code =
    CASE
    WHEN crossing_condition = 'NA' THEN NULL
    WHEN crossing_condition = 'new' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'new')
    WHEN crossing_condition = 'ok' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'ok')
    WHEN crossing_condition = 'poor' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'poor')
    WHEN crossing_condition = 'unknown' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET site_type =
    CASE
    WHEN tidal_site = 'no' THEN (SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'nontidal')
    WHEN tidal_site = 'unknown' THEN (SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET alignment_code =
    CASE
    WHEN alignment = 'aligned' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN alignment = 'flow_aligned' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN alignment = 'skewed' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    WHEN alignment = 'skewed_45_degrees' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET road_fill_height_m =
    CASE
    WHEN road_fill_height ~ '[0-9]+' THEN road_fill_height::numeric
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET road_fill_height_m = NULL WHERE road_fill_height_m > 100;
UPDATE {script.nonTidalSites} SET downstream_bankfull_width_m =
    CASE
    WHEN bankfull_width ~ '[0-9]+' THEN bankfull_width::numeric
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET downstream_bankfull_width_confidence_code =
    CASE
    WHEN bankfull_confidence = 'high' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'high')
    WHEN bankfull_confidence = 'low' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'low')
    WHEN bankfull_confidence = 'lowestimated' THEN (SELECT code FROM stream_crossings.confidence_codes WHERE name_en = 'low')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET tailwater_scour_pool_code =
    CASE
    WHEN tailwater_scour_pool IN ('l', 'large') THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'large')
    WHEN tailwater_scour_pool IN ('s', 'small') THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'small')
    WHEN tailwater_scour_pool = 'none' THEN (SELECT code FROM stream_crossings.scour_pool_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET constriction_code =
    CASE
    WHEN constriction = 'bankfull' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans only bankfull/active channel')
    WHEN constriction = 'full' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans full channel and banks')
    WHEN constriction = 'moderate' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'moderate')
    WHEN constriction = 'severe' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'severe')
    WHEN constriction = 'spans_full_channel_and_banks' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans full channel and banks')
    WHEN constriction = 'spans_only_bankfull_active_chan' THEN (SELECT code FROM stream_crossings.constriction_codes WHERE name_en = 'spans only bankfull/active channel')
    ELSE NULL END;
UPDATE {script.nonTidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.nonTidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.nonTidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.nonTidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- nontidal - structure 1
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_nontidal_structure_1;
CREATE TABLE featurecopy.temp_nontidal_structure_1 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        1 AS structure_number,
        source.structure_length,
        source.internal_structures,
        source.other_internal_structures,
        source.physical_barriers,
        source.other_physical_barriers,
        source.severity,
        source.dry_passage_through_structure,
        source.height_above_dry_passage,
        source.structure_substrate_matches_stream,
        source.structure_substrate_type,
        source.structure_substrate_coverage,
        source.water_depth_matches_stream,
        source.water_velocity_matches_stream,
        source.structure_material,
        source.outlet_shape,
        source.outlet_armouring,
        source.outlet_grade,
        source.outlet_width,
        source.outlet_height,
        source.outlet_ww,
        source.outlet_depth,
        source.outlet_drop_to_water_surface,
        source.outlet_drop_to_stream_bottom,
        source.inlet_shape,
        source.inlet_type,
        source.other_inlet_type,
        source.inlet_grade,
        source.inlet_width,
        source.inlet_height,
        source.inlet_ww,
        source.inlet_depth,
        source.structure_1_comments,
        barrier_type
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE featurecopy.temp_nontidal_structure_1
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_drop_to_water_surface_m numeric,
    ADD COLUMN outlet_drop_to_stream_bottom_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN internal_structures_code integer,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN water_depth_matches_stream_code integer,
    ADD COLUMN water_velocity_matches_stream_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000),
    ADD COLUMN passability_status_code integer;


UPDATE featurecopy.temp_nontidal_structure_1 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_nontidal_structure_1 SET primary_structure = true;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_drop_to_water_surface_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET outlet_drop_to_stream_bottom_m = CASE WHEN outlet_drop_to_stream_bottom ~ '[0-9]+' THEN outlet_drop_to_stream_bottom::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET internal_structures_code =
    CASE
    WHEN internal_structures ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
    WHEN internal_structures = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET water_depth_matches_stream_code =
    CASE
    WHEN water_depth_matches_stream = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_depth_matches_stream = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_depth_matches_stream = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET water_velocity_matches_stream_code =
    CASE 
    WHEN water_velocity_matches_stream = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_velocity_matches_stream = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_1 SET structure_comments = structure_1_comments WHERE structure_1_comments IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_1 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_1 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');
UPDATE featurecopy.temp_nontidal_structure_1 SET passability_status_code =
    CASE
    WHEN barrier_type = 'c-None' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Passable')
    ELSE (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier') END;

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.nonTidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_1);
INSERT INTO {script.nonTidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_1 WHERE structure_material IS NOT NULL;

DELETE FROM {script.nonTidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_1);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_1
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- nontidal - structure 2
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_nontidal_structure_2;
CREATE TABLE featurecopy.temp_nontidal_structure_2 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        2 AS structure_number,
        source.structure_length_1,
        source.internal_structures_1,
        source.other_internal_structures_1,
        source.physical_barriers_1,
        source.other_physical_barriers_1,
        source.severity_1,
        source.dry_passage_through_structure_1,
        source.height_above_dry_passage_1,
        source.structure_substrate_matches_stream_1,
        source.structure_substrate_type_1,
        source.structure_substrate_coverage_1,
        source.water_depth_matches_stream_1,
        source.water_velocity_matches_stream_1,
        source.structure_material_1,
        source.outlet_shape_1,
        source.outlet_armouring_1,
        source.outlet_grade_1,
        source.outlet_width_1,
        source.outlet_height_1,
        source.outlet_ww_1,
        source.outlet_depth_1,
        source.outlet_drop_to_water_surface_1,
        source.outlet_drop_to_stream_bottom_1,
        source.inlet_shape_1,
        source.inlet_type_1,
        source.other_inlet_type_1,
        source.inlet_grade_1,
        source.inlet_width_1,
        source.inlet_height_1,
        source.inlet_ww_1,
        source.inlet_depth_1,
        source.structure_2_comments
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 2
);

ALTER TABLE featurecopy.temp_nontidal_structure_2
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_drop_to_water_surface_m numeric,
    ADD COLUMN outlet_drop_to_stream_bottom_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN internal_structures_code integer,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN water_depth_matches_stream_code integer,
    ADD COLUMN water_velocity_matches_stream_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000),
    ADD COLUMN passability_status_code integer;

UPDATE featurecopy.temp_nontidal_structure_2 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_nontidal_structure_2 SET primary_structure = false;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_shape_code =
    CASE
    WHEN outlet_shape_1 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape_1 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_1 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_1 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_1 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape_1 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_1 = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_1 = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_1 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_1 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_1 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring_1 = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring_1 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring_1 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring_1 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_grade_code =
    CASE
    WHEN outlet_grade_1 = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade_1 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade_1 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade_1 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade_1 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade_1 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_width_m = CASE WHEN outlet_width_1 ~ '[0-9]+' THEN outlet_width_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_height_m = CASE WHEN outlet_height_1 ~ '[0-9]+' THEN outlet_height_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET structure_length_m = CASE WHEN structure_length_1 ~ '[0-9]+' THEN structure_length_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww_1 ~ '[0-9]+' THEN outlet_ww_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_water_depth_m = CASE WHEN outlet_depth_1 ~ '[0-9]+' THEN outlet_depth_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_drop_to_water_surface_m = CASE WHEN outlet_drop_to_water_surface_1 ~ '[0-9]+' THEN outlet_drop_to_water_surface_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET outlet_drop_to_stream_bottom_m = CASE WHEN outlet_drop_to_stream_bottom_1 ~ '[0-9]+' THEN outlet_drop_to_stream_bottom_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape_1 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape_1 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape_1 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape_1 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape_1 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape_1 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_type_code =
    CASE
    WHEN inlet_type_1 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type_1 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_1 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_1 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_1 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_1 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type_1 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type_1 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type_1 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type_1 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_grade_code =
    CASE
    WHEN inlet_grade_1 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade_1 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade_1 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade_1 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade_1 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_width_m = CASE WHEN inlet_width_1 ~ '[0-9]+' THEN inlet_width_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_height_m = CASE WHEN inlet_height_1 ~ '[0-9]+' THEN inlet_height_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww_1 ~ '[0-9]+' THEN inlet_ww_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET inlet_water_depth_m = CASE WHEN inlet_depth_1 ~ '[0-9]+' THEN inlet_depth_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET internal_structures_code =
    CASE
    WHEN internal_structures_1 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
    WHEN internal_structures_1 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream_1 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream_1 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream_1 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type_1 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type_1 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type_1 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type_1 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type_1 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type_1 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type_1 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage_1 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage_1 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage_1 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage_1 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage_1 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET physical_barrier_severity_code =
    CASE
    WHEN severity_1 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity_1 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity_1 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity_1 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET water_depth_matches_stream_code =
    CASE
    WHEN water_depth_matches_stream_1 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_depth_matches_stream_1 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_1 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_1 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_1 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_depth_matches_stream_1 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET water_velocity_matches_stream_code =
    CASE 
    WHEN water_velocity_matches_stream_1 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_1 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_1 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_1 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_1 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_1 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_1 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_velocity_matches_stream_1 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure_1 = 'yes' THEN true
    WHEN dry_passage_through_structure_1 = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage_1 ~ '[0-9]+' THEN height_above_dry_passage_1::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_2 SET structure_comments = structure_2_comments WHERE structure_2_comments IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_2 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers_1 WHERE other_physical_barriers_1 IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_2 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type_1 WHERE other_inlet_type_1 IS NOT NULL AND other_inlet_type_1 NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.nonTidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_2);
INSERT INTO {script.nonTidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material_1 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_1 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material_1 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_1 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material_1 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material_1 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material_1 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_2 WHERE structure_material_1 IS NOT NULL;

DELETE FROM {script.nonTidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_2);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers_1, ',')) AS physical_barriers_1,
    cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_2
    WHERE physical_barriers_1 IS NOT NULL;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers_1 ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers_1 ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers_1 ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers_1 = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers_1 IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers_1 ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers_1 ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- nontidal - structure 3
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_nontidal_structure_3;
CREATE TABLE featurecopy.temp_nontidal_structure_3 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        3 AS structure_number,
        source.structure_length_2,
        source.internal_structures_2,
        source.other_internal_structures_2,
        source.physical_barriers_2,
        source.other_physical_barriers_2,
        source.severity_2,
        source.dry_passage_through_structure_2,
        source.height_above_dry_passage_2,
        source.structure_substrate_matches_stream_2,
        source.structure_substrate_type_2,
        source.structure_substrate_coverage_2,
        source.water_depth_matches_stream_2,
        source.water_velocity_matches_stream_2,
        source.structure_material_2,
        source.outlet_shape_2,
        source.outlet_armouring_2,
        source.outlet_grade_2,
        source.outlet_width_2,
        source.outlet_height_2,
        source.outlet_ww_2,
        source.outlet_depth_2,
        source.outlet_drop_to_water_surface_2,
        source.outlet_drop_to_stream_bottom_2,
        source.inlet_shape_2,
        source.inlet_type_2,
        source.other_inlet_type_2,
        source.inlet_grade_2,
        source.inlet_width_2,
        source.inlet_height_2,
        source.inlet_ww_2,
        source.inlet_depth_2,
        source.structure_3_comments
    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 3
);

ALTER TABLE featurecopy.temp_nontidal_structure_3
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_drop_to_water_surface_m numeric,
    ADD COLUMN outlet_drop_to_stream_bottom_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN internal_structures_code integer,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN water_depth_matches_stream_code integer,
    ADD COLUMN water_velocity_matches_stream_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000),
    ADD COLUMN passability_status_code integer;

UPDATE featurecopy.temp_nontidal_structure_3 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_nontidal_structure_3 SET primary_structure = false;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_shape_code =
    CASE
    WHEN outlet_shape_2 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape_2 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_2 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_2 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_2 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape_2 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_2 = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_2 = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_2 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_2 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_2 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring_2 = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring_2 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring_2 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring_2 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_grade_code =
    CASE
    WHEN outlet_grade_2 = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade_2 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade_2 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade_2 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade_2 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade_2 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_width_m = CASE WHEN outlet_width_2 ~ '[0-9]+' THEN outlet_width_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_height_m = CASE WHEN outlet_height_2 ~ '[0-9]+' THEN outlet_height_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET structure_length_m = CASE WHEN structure_length_2 ~ '[0-9]+' THEN structure_length_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww_2 ~ '[0-9]+' THEN outlet_ww_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_water_depth_m = CASE WHEN outlet_depth_2 ~ '[0-9]+' THEN outlet_depth_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_drop_to_water_surface_m = CASE WHEN outlet_drop_to_water_surface_2 ~ '[0-9]+' THEN outlet_drop_to_water_surface_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET outlet_drop_to_stream_bottom_m = CASE WHEN outlet_drop_to_stream_bottom_2 ~ '[0-9]+' THEN outlet_drop_to_stream_bottom_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape_2 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape_2 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape_2 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape_2 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape_2 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape_2 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_type_code =
    CASE
    WHEN inlet_type_2 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type_2 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_2 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_2 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_2 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_2 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type_2 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type_2 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type_2 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type_2 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_grade_code =
    CASE
    WHEN inlet_grade_2 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade_2 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade_2 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade_2 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade_2 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_width_m = CASE WHEN inlet_width_2 ~ '[0-9]+' THEN inlet_width_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_height_m = CASE WHEN inlet_height_2 ~ '[0-9]+' THEN inlet_height_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww_2 ~ '[0-9]+' THEN inlet_ww_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET inlet_water_depth_m = CASE WHEN inlet_depth_2 ~ '[0-9]+' THEN inlet_depth_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET internal_structures_code =
    CASE
    WHEN internal_structures_2 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
    WHEN internal_structures_2 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream_2 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream_2 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream_2 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type_2 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type_2 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type_2 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type_2 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type_2 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type_2 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type_2 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage_2 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage_2 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage_2 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage_2 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage_2 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET physical_barrier_severity_code =
    CASE
    WHEN severity_2 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity_2 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity_2 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity_2 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET water_depth_matches_stream_code =
    CASE
    WHEN water_depth_matches_stream_2 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_depth_matches_stream_2 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_2 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_2 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_2 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_depth_matches_stream_2 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET water_velocity_matches_stream_code =
    CASE 
    WHEN water_velocity_matches_stream_2 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_2 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_2 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_2 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_2 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_2 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_2 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_velocity_matches_stream_2 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure_2 = 'yes' THEN true
    WHEN dry_passage_through_structure_2 = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage_2 ~ '[0-9]+' THEN height_above_dry_passage_2::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_3 SET structure_comments = structure_3_comments WHERE structure_3_comments IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_3 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers_2 WHERE other_physical_barriers_2 IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_3 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type_2 WHERE other_inlet_type_2 IS NOT NULL AND other_inlet_type_2 NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.nonTidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_3);
INSERT INTO {script.nonTidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material_2 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_2 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material_2 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_2 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material_2 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material_2 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material_2 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_3 WHERE structure_material_2 IS NOT NULL;

DELETE FROM {script.nonTidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_3);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers_2, ',')) AS physical_barriers_2,
    cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_3
    WHERE physical_barriers_2 IS NOT NULL;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers_2 ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers_2 ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers_2 ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers_2 = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers_2 IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers_2 ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers_2 ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- nontidal - structure 4
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_nontidal_structure_4;
CREATE TABLE featurecopy.temp_nontidal_structure_4 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        4 AS structure_number,
        source.structure_length_3,
        source.internal_structures_3,
        source.other_internal_structures_3,
        source.physical_barriers_3,
        source.othe_physical_barriers_3,
        source.severity_3,
        source.dry_passage_through_structure_3,
        source.height_above_dry_passage_3,
        source.structure_substrate_matches_stream_3,
        source.structure_substrate_type_3,
        source.structure_substrate_coverage_3,
        source.water_depth_matches_stream_3,
        source.water_velocity_matches_stream_3,
        source.structure_3_comments,
        source.structure_material_3,
        source.outlet_shape_3,
        source.outlet_armouring_3,
        source.outlet_grade_3,
        source.outlet_width_3,
        source.outlet_height_3,
        source.outlet_ww_3,
        source.outlet_depth_3,
        source.outlet_drop_to_water_surface_3,
        source.outlet_drop_to_stream_bottom_3,
        source.inlet_shape_3,
        source.inlet_type_3,
        source.other_inlet_type_3,
        source.inlet_grade_3,
        source.inlet_width_3,
        source.inlet_height_3,
        source.inlet_ww_3,
        source.inlet_depth_3,
        source.structure_4_comments

    FROM
        {script.nonTidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 4
);

ALTER TABLE featurecopy.temp_nontidal_structure_4
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_drop_to_water_surface_m numeric,
    ADD COLUMN outlet_drop_to_stream_bottom_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN internal_structures_code integer,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN water_depth_matches_stream_code integer,
    ADD COLUMN water_velocity_matches_stream_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000),
    ADD COLUMN passability_status_code integer;

UPDATE featurecopy.temp_nontidal_structure_4 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_nontidal_structure_4 SET primary_structure = false;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_shape_code =
    CASE
    WHEN outlet_shape_3 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape_3 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_3 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_3 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_3 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape_3 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_3 = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape_3 = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape_3 = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape_3 = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape_3 = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring_3 = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring_3 = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring_3 = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring_3 = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_grade_code =
    CASE
    WHEN outlet_grade_3 = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade_3 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade_3 = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade_3 = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade_3 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade_3 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_width_m = CASE WHEN outlet_width_3 ~ '[0-9]+' THEN outlet_width_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_height_m = CASE WHEN outlet_height_3 ~ '[0-9]+' THEN outlet_height_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET structure_length_m = CASE WHEN structure_length_3 ~ '[0-9]+' THEN structure_length_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww_3 ~ '[0-9]+' THEN outlet_ww_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_water_depth_m = CASE WHEN outlet_depth_3 ~ '[0-9]+' THEN outlet_depth_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_drop_to_water_surface_m = CASE WHEN outlet_drop_to_water_surface_3 ~ '[0-9]+' THEN outlet_drop_to_water_surface_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET outlet_drop_to_stream_bottom_m = CASE WHEN outlet_drop_to_stream_bottom_3 ~ '[0-9]+' THEN outlet_drop_to_stream_bottom_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape_3 = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape_3 = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape_3 = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape_3 = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape_3 = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape_3 = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_type_code =
    CASE
    WHEN inlet_type_3 = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type_3 = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_3 = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type_3 = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_3 = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type_3 = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type_3 = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type_3 = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type_3 = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type_3 = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_grade_code =
    CASE
    WHEN inlet_grade_3 = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade_3 = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade_3 = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade_3 = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade_3 = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_width_m = CASE WHEN inlet_width_3 ~ '[0-9]+' THEN inlet_width_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_height_m = CASE WHEN inlet_height_3 ~ '[0-9]+' THEN inlet_height_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww_3 ~ '[0-9]+' THEN inlet_ww_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET inlet_water_depth_m = CASE WHEN inlet_depth_3 ~ '[0-9]+' THEN inlet_depth_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET internal_structures_code =
    CASE
    WHEN internal_structures_3 ILIKE '%baffles%' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'baffles/weirs')
    WHEN internal_structures_3 = 'none' THEN (SELECT code FROM stream_crossings.internal_structure_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream_3 = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream_3 = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream_3 = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type_3 = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type_3 = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type_3 = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type_3 = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type_3 = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type_3 = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type_3 = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage_3 = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage_3 = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage_3 = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage_3 = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage_3 = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET physical_barrier_severity_code =
    CASE
    WHEN severity_3 = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity_3 = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity_3 = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity_3 = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET water_depth_matches_stream_code =
    CASE
    WHEN water_depth_matches_stream_3 = 'dry' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_depth_matches_stream_3 = 'no_deep' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_3 = 'no_deeper' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-deeper')
    WHEN water_depth_matches_stream_3 = 'no_shallow' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_3 = 'no_shallower' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'no-shallower')
    WHEN water_depth_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_depth_matches_stream_3 = 'yes' THEN (SELECT code FROM stream_crossings.water_depth_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET water_velocity_matches_stream_code =
    CASE 
    WHEN water_velocity_matches_stream_3 = 'dry' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_3 = 'dry_' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'dry')
    WHEN water_velocity_matches_stream_3 = 'no_fast' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_3 = 'no_faster' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-faster')
    WHEN water_velocity_matches_stream_3 = 'no_slow' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_3 = 'no_slower' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'no-slower')
    WHEN water_velocity_matches_stream_3 = 'unknown' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'unknown')
    WHEN water_velocity_matches_stream_3 = 'yes' THEN (SELECT code FROM stream_crossings.water_velocity_matches_stream_codes WHERE name_en = 'yes')
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure_3 = 'yes' THEN true
    WHEN dry_passage_through_structure_3 = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage_3 ~ '[0-9]+' THEN height_above_dry_passage_3::numeric ELSE NULL END;
UPDATE featurecopy.temp_nontidal_structure_4 SET structure_comments = structure_4_comments WHERE structure_4_comments IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_4 SET structure_comments = structure_comments || '; other physical barriers: ' || othe_physical_barriers_3 WHERE othe_physical_barriers_3 IS NOT NULL;
UPDATE featurecopy.temp_nontidal_structure_4 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type_3 WHERE other_inlet_type_3 IS NOT NULL AND other_inlet_type_3 NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.nonTidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_4);
INSERT INTO {script.nonTidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material_3 = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_3 = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material_3 = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material_3 = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material_3 = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material_3 = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material_3 = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_4 WHERE structure_material_3 IS NOT NULL;

DELETE FROM {script.nonTidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_nontidal_structure_4);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers_3, ',')) AS physical_barriers_3,
    cabd_assessment_id
    FROM featurecopy.temp_nontidal_structure_4
    WHERE physical_barriers_3 IS NOT NULL;
INSERT INTO {script.nonTidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers_3 ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers_3 ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers_3 ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers_3 = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers_3 IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers_3 ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers_3 ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- nontidal - combine all structures
------------------------------------------

DROP TABLE IF EXISTS {script.nonTidalStructures};
CREATE TABLE {script.nonTidalStructures} AS TABLE stream_crossings.nontidal_structures WITH NO DATA;

INSERT INTO {script.nonTidalStructures}
    (site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
    )
SELECT
    site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
FROM featurecopy.temp_nontidal_structure_1;

INSERT INTO {script.nonTidalStructures}
    (site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_nontidal_structure_2;

INSERT INTO {script.nonTidalStructures}
    (site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_nontidal_structure_3;

INSERT INTO {script.nonTidalStructures}
    (site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    structure_id,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_drop_to_water_surface_m,
    outlet_drop_to_stream_bottom_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    internal_structures_code,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    water_depth_matches_stream_code,
    water_velocity_matches_stream_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_nontidal_structure_4;

DROP TABLE featurecopy.temp_nontidal_structure_1;
DROP TABLE featurecopy.temp_nontidal_structure_2;
DROP TABLE featurecopy.temp_nontidal_structure_3;
DROP TABLE featurecopy.temp_nontidal_structure_4;

ALTER TABLE {script.nonTidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.nonTidalStructures} ADD PRIMARY KEY (structure_id);

------------------------------------------
--tidal crossings
------------------------------------------

--add information to sites table
DROP TABLE IF EXISTS {script.tidalSites};
CREATE TABLE {script.tidalSites} AS
    SELECT
        cabd_assessment_id::uuid,
        site_id AS original_assessment_id,
        data_source_name,
        data_source_id,
        date_observed,
        stream_name,
        road_name,
        road_type,
        crossing_type,
        no_culverts,
        structure_length,
        flow_condition,
        crossing_condition,
        tidal_site,
        alignment,
        road_fill_height,
        bankfull_width,
        geometry
    FROM {script.sourceTable}
    WHERE tidal_site = 'yes';

ALTER TABLE {script.tidalSites} ALTER COLUMN cabd_assessment_id SET NOT NULL;
ALTER TABLE {script.tidalSites} ADD PRIMARY KEY (cabd_assessment_id);
ALTER TABLE {script.tidalSites} ADD CONSTRAINT {script.datasetname}_data_source_fkey FOREIGN KEY (data_source_id) REFERENCES cabd.data_source (id);

ALTER TABLE {script.tidalSites}
    ADD COLUMN cabd_id uuid,
    ADD COLUMN road_type_code integer,
    ADD COLUMN crossing_type_code integer,
    ADD COLUMN structure_count integer,
    ADD COLUMN flow_condition_code integer,
    ADD COLUMN crossing_condition_code integer,
    ADD COLUMN site_type integer,
    ADD COLUMN alignment_code integer,
    ADD COLUMN road_fill_height_m numeric,
    ADD COLUMN downstream_channel_width_m numeric,
    ADD COLUMN original_point geometry(Point,4617);

ALTER TABLE {script.tidalSites} ALTER COLUMN date_observed TYPE date USING date_observed::date;
UPDATE {script.tidalSites} SET stream_name = NULL WHERE stream_name = 'Unknown';
UPDATE {script.tidalSites} SET road_name = NULL WHERE road_name = 'Unknown';
UPDATE {script.tidalSites} SET road_type_code = 
    CASE
    WHEN road_type = 'driveway' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'driveway')
    WHEN road_type = 'unpaved' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'unpaved')
    WHEN road_type = 'paved' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'paved')
    WHEN road_type = 'multilane' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'multilane')
    WHEN road_type = 'trail' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'trail')
    WHEN road_type = 'railroad' THEN (SELECT code FROM stream_crossings.road_type_codes WHERE name_en = 'railroad')
    ELSE NULL END;
UPDATE {script.tidalSites} SET crossing_type_code = 
    CASE
    WHEN crossing_type = 'bridge' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'bridge')
    WHEN crossing_type = 'bridge_adequate' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'bridge adequate')
    WHEN crossing_type = 'buried_stream' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'buried stream')
    WHEN crossing_type = 'culvert' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'culvert')
    WHEN crossing_type = 'ford' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'ford')
    WHEN crossing_type = 'inaccessible' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'inaccessible')
    WHEN crossing_type = 'multiple_culvert' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'multiple culvert')
    WHEN crossing_type = 'no_crossing' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no crossing')
    WHEN crossing_type = 'no_upstream' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no upstream channel')
    WHEN crossing_type = 'no_upstream_channel' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'no upstream channel')
    WHEN crossing_type = 'partially_inaccessible' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'partially inaccessible')
    WHEN crossing_type = 'removed_crossing' THEN (SELECT code from stream_crossings.crossing_type_codes where name_en = 'removed crossing')
    ELSE NULL END;
UPDATE {script.tidalSites} SET structure_count = 
    CASE
    WHEN no_culverts ~ '[0-9]+' THEN no_culverts::integer
    WHEN (no_culverts = 'NA' AND structure_length IS NOT NULL AND structure_length != 'NA') THEN 1
    WHEN (no_culverts = 'NA' AND structure_length = 'NA') THEN 1
    ELSE NULL END;
UPDATE {script.tidalSites} SET flow_condition_code =
    CASE
    WHEN flow_condition = 'high' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'high flow')
    WHEN flow_condition = 'moderate' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'moderate flow')
    WHEN flow_condition = 'no_flow' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'dewatered')
    WHEN flow_condition = 'typical_low' THEN (SELECT code FROM stream_crossings.flow_condition_codes WHERE name_en = 'typical low flow')
    ELSE NULL END;
UPDATE {script.tidalSites} SET crossing_condition_code =
    CASE
    WHEN crossing_condition = 'NA' THEN NULL
    WHEN crossing_condition = 'new' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'new')
    WHEN crossing_condition = 'ok' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'ok')
    WHEN crossing_condition = 'poor' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'poor')
    WHEN crossing_condition = 'unknown' THEN (SELECT code FROM stream_crossings.crossing_condition_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE {script.tidalSites} SET site_type =
    CASE
    WHEN tidal_site = 'yes' THEN (SELECT code FROM stream_crossings.site_type_codes WHERE name_en = 'tidal')
    ELSE NULL END;
UPDATE {script.tidalSites} SET alignment_code =
    CASE
    WHEN alignment = 'aligned' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN alignment = 'flow_aligned' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'flow-aligned')
    WHEN alignment = 'skewed' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    WHEN alignment = 'skewed_45_degrees' THEN (SELECT code FROM stream_crossings.alignment_codes WHERE name_en = 'skewed')
    ELSE NULL END;
UPDATE {script.tidalSites} SET road_fill_height_m =
    CASE
    WHEN road_fill_height ~ '[0-9]+' THEN road_fill_height::numeric
    ELSE NULL END;
UPDATE {script.tidalSites} SET downstream_channel_width_m =
    CASE
    WHEN bankfull_width ~ '[0-9]+' THEN bankfull_width::numeric
    ELSE NULL END;
UPDATE {script.tidalSites} SET original_point = ST_Transform(geometry, 4617);

UPDATE {script.tidalSites} SET cabd_id = r.modelled_crossing_id::uuid
FROM {script.reviewTable} AS r
WHERE
    (r.source_1 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_1::uuid)
    OR 
    (r.source_2 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_2::uuid)
    OR
    (r.source_3 = 'Peskotomuhkati_Nation_Classified_Barriers' AND cabd_assessment_id = r.id_3::uuid);

ALTER TABLE {script.tidalSites} ADD COLUMN entry_classification varchar;
UPDATE {script.tidalSites} SET entry_classification =
    CASE
    WHEN cabd_id IS NULL THEN 'new feature'
    WHEN cabd_id IS NOT NULL THEN 'update feature'
    ELSE NULL END;

------------------------------------------
-- tidal - structure 1
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_tidal_structure_1;
CREATE TABLE featurecopy.temp_tidal_structure_1 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        1 AS structure_number,
        source.structure_length,
        source.physical_barriers,
        source.other_physical_barriers,
        source.severity,
        source.dry_passage_through_structure,
        source.height_above_dry_passage,
        source.structure_substrate_matches_stream,
        source.structure_substrate_type,
        source.structure_substrate_coverage,
        source.structure_material,
        source.outlet_shape,
        source.outlet_armouring,
        source.outlet_grade,
        source.outlet_width,
        source.outlet_height,
        source.outlet_ww,
        source.outlet_depth,
        source.outlet_drop_to_water_surface,
        source.inlet_shape,
        source.inlet_type,
        source.other_inlet_type,
        source.inlet_grade,
        source.inlet_width,
        source.inlet_height,
        source.inlet_ww,
        source.inlet_depth,
        source.structure_1_comments,
        barrier_type
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
);

ALTER TABLE featurecopy.temp_tidal_structure_1
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_perch_low_tide_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000),
    ADD COLUMN passability_status_code integer;

UPDATE featurecopy.temp_tidal_structure_1 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_tidal_structure_1 SET primary_structure = true;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET outlet_perch_low_tide_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_1 SET structure_comments = structure_1_comments WHERE structure_1_comments IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_1 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_1 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');
UPDATE featurecopy.temp_tidal_structure_1 SET passability_status_code =
    CASE
    WHEN barrier_type = 'c-None' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Passable')
    ELSE (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier') END;

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.tidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_1);
INSERT INTO {script.tidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_1 WHERE structure_material IS NOT NULL;

DELETE FROM {script.tidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_1);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_1
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.tidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- tidal - structure 2
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_tidal_structure_2;
CREATE TABLE featurecopy.temp_tidal_structure_2 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        2 AS structure_number,
        source.structure_length_1 AS structure_length,
        source.physical_barriers_1 AS physical_barriers,
        source.other_physical_barriers_1 AS other_physical_barriers,
        source.severity_1 AS severity,
        source.dry_passage_through_structure_1 AS dry_passage_through_structure,
        source.height_above_dry_passage_1 AS height_above_dry_passage,
        source.structure_substrate_matches_stream_1 AS structure_substrate_matches_stream,
        source.structure_substrate_type_1 AS structure_substrate_type,
        source.structure_substrate_coverage_1 AS structure_substrate_coverage,
        source.structure_material_1 AS structure_material,
        source.outlet_shape_1 AS outlet_shape,
        source.outlet_armouring_1 AS outlet_armouring,
        source.outlet_grade_1 AS outlet_grade,
        source.outlet_width_1 AS outlet_width,
        source.outlet_height_1 AS outlet_height,
        source.outlet_ww_1 AS outlet_ww,
        source.outlet_depth_1 AS outlet_depth,
        source.outlet_drop_to_water_surface_1 AS outlet_drop_to_water_surface,
        source.inlet_shape_1 AS inlet_shape,
        source.inlet_type_1 AS inlet_type,
        source.other_inlet_type_1 AS other_inlet_type,
        source.inlet_grade_1 AS inlet_grade,
        source.inlet_width_1 AS inlet_width,
        source.inlet_height_1 AS inlet_height,
        source.inlet_ww_1 AS inlet_ww,
        source.inlet_depth_1 AS inlet_depth,
        source.structure_2_comments
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 2
);

ALTER TABLE featurecopy.temp_tidal_structure_2
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_perch_low_tide_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000);

UPDATE featurecopy.temp_tidal_structure_2 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_tidal_structure_2 SET primary_structure = false;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET outlet_perch_low_tide_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_2 SET structure_comments = structure_2_comments WHERE structure_2_comments IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_2 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_2 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.tidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_2);
INSERT INTO {script.tidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_2 WHERE structure_material IS NOT NULL;

DELETE FROM {script.tidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_2);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_2
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.tidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- tidal - structure 3
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_tidal_structure_3;
CREATE TABLE featurecopy.temp_tidal_structure_3 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        3 AS structure_number,
        source.structure_length_2 AS structure_length,
        source.physical_barriers_2 AS physical_barriers,
        source.other_physical_barriers_2 AS other_physical_barriers,
        source.severity_2 AS severity,
        source.dry_passage_through_structure_2 AS dry_passage_through_structure,
        source.height_above_dry_passage_2 AS height_above_dry_passage,
        source.structure_substrate_matches_stream_2 AS structure_substrate_matches_stream,
        source.structure_substrate_type_2 AS structure_substrate_type,
        source.structure_substrate_coverage_2 AS structure_substrate_coverage,
        source.structure_material_2 AS structure_material,
        source.outlet_shape_2 AS outlet_shape,
        source.outlet_armouring_2 AS outlet_armouring,
        source.outlet_grade_2 AS outlet_grade,
        source.outlet_width_2 AS outlet_width,
        source.outlet_height_2 AS outlet_height,
        source.outlet_ww_2 AS outlet_ww,
        source.outlet_depth_2 AS outlet_depth,
        source.outlet_drop_to_water_surface_2 AS outlet_drop_to_water_surface,
        source.inlet_shape_2 AS inlet_shape,
        source.inlet_type_2 AS inlet_type,
        source.other_inlet_type_2 AS other_inlet_type,
        source.inlet_grade_2 AS inlet_grade,
        source.inlet_width_2 AS inlet_width,
        source.inlet_height_2 AS inlet_height,
        source.inlet_ww_2 AS inlet_ww,
        source.inlet_depth_2 AS inlet_depth,
        source.structure_3_comments
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 3
);

ALTER TABLE featurecopy.temp_tidal_structure_3
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_perch_low_tide_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000);

UPDATE featurecopy.temp_tidal_structure_3 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_tidal_structure_3 SET primary_structure = false;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET outlet_perch_low_tide_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_3 SET structure_comments = structure_3_comments WHERE structure_3_comments IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_3 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_3 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.tidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_3);
INSERT INTO {script.tidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_3 WHERE structure_material IS NOT NULL;

DELETE FROM {script.tidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_3);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_3
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.tidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- tidal - structure 4
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_tidal_structure_4;
CREATE TABLE featurecopy.temp_tidal_structure_4 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        4 AS structure_number,
        source.structure_length_3 AS structure_length,
        source.physical_barriers_3 AS physical_barriers,
        source.othe_physical_barriers_3 AS other_physical_barriers,
        source.severity_3 AS severity,
        source.dry_passage_through_structure_3 AS dry_passage_through_structure,
        source.height_above_dry_passage_3 AS height_above_dry_passage,
        source.structure_substrate_matches_stream_3 AS structure_substrate_matches_stream,
        source.structure_substrate_type_3 AS structure_substrate_type,
        source.structure_substrate_coverage_3 AS structure_substrate_coverage,
        source.structure_material_3 AS structure_material,
        source.outlet_shape_3 AS outlet_shape,
        source.outlet_armouring_3 AS outlet_armouring,
        source.outlet_grade_3 AS outlet_grade,
        source.outlet_width_3 AS outlet_width,
        source.outlet_height_3 AS outlet_height,
        source.outlet_ww_3 AS outlet_ww,
        source.outlet_depth_3 AS outlet_depth,
        source.outlet_drop_to_water_surface_3 AS outlet_drop_to_water_surface,
        source.inlet_shape_3 AS inlet_shape,
        source.inlet_type_3 AS inlet_type,
        source.other_inlet_type_3 AS other_inlet_type,
        source.inlet_grade_3 AS inlet_grade,
        source.inlet_width_3 AS inlet_width,
        source.inlet_height_3 AS inlet_height,
        source.inlet_ww_3 AS inlet_ww,
        source.inlet_depth_3 AS inlet_depth,
        source.structure_4_comments
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 4
);

ALTER TABLE featurecopy.temp_tidal_structure_4
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_perch_low_tide_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000);

UPDATE featurecopy.temp_tidal_structure_4 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_tidal_structure_4 SET primary_structure = false;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET outlet_perch_low_tide_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_4 SET structure_comments = structure_4_comments WHERE structure_4_comments IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_4 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_4 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.tidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_4);
INSERT INTO {script.tidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_4 WHERE structure_material IS NOT NULL;

DELETE FROM {script.tidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_4);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_4
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.tidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- tidal - structure 5
------------------------------------------

DROP TABLE IF EXISTS featurecopy.temp_tidal_structure_5;
CREATE TABLE featurecopy.temp_tidal_structure_5 AS (
    SELECT
        site.cabd_id AS site_id,
        source.data_source_id,
        source.cabd_assessment_id,
        site.structure_count,
        5 AS structure_number,
        source.structure_length_4 AS structure_length,
        source.physical_barriers_4 AS physical_barriers,
        source.other_physical_barriers_4 AS other_physical_barriers,
        source.severity_4 AS severity,
        source.dry_passage_through_structure_4 AS dry_passage_through_structure,
        source.height_above_dry_passage_4 AS height_above_dry_passage,
        source.stucture_substrate_matches_stream_4 AS structure_substrate_matches_stream,
        source.structure_substrate_type_4 AS structure_substrate_type,
        source.structure_substrate_coverage_4 AS structure_substrate_coverage,
        source.structure_material_4 AS structure_material,
        source.outlet_shape_4 AS outlet_shape,
        source.outlet_armouring_4 AS outlet_armouring,
        source.outlet_grade_4 AS outlet_grade,
        source.outlet_width_4 AS outlet_width,
        source.outlet_height_4 AS outlet_height,
        source.outlet_ww_4 AS outlet_ww,
        source.outlet_depth_4 AS outlet_depth,
        source.outlet_drop_to_water_surface_4 AS outlet_drop_to_water_surface,
        source.inlet_shape_4 AS inlet_shape,
        source.inlet_type_4 AS inlet_type,
        source.other_inlet_type_4 AS other_inlet_type,
        source.inlet_grade_4 AS inlet_grade,
        source.inlet_width_4 AS inlet_width,
        source.inlet_height_4 AS inlet_height,
        source.inlet_ww_4 AS inlet_ww,
        source.inlet_depth_4 AS inlet_depth,
        source.structure_5_comments
    FROM
        {script.tidalSites} AS site,
        {script.sourceTable} AS source
    WHERE 
        source.cabd_assessment_id = site.cabd_assessment_id
        AND site.structure_count >= 4
);

ALTER TABLE featurecopy.temp_tidal_structure_5
    ADD COLUMN structure_id uuid,
    ADD COLUMN primary_structure boolean,
    ADD COLUMN outlet_shape_code integer,
    ADD COLUMN outlet_armouring_code integer,
    ADD COLUMN outlet_grade_code integer,
    ADD COLUMN outlet_width_m numeric,
    ADD COLUMN outlet_height_m numeric,
    ADD COLUMN structure_length_m numeric,
    ADD COLUMN outlet_substrate_water_width_m numeric,
    ADD COLUMN outlet_water_depth_m numeric,
    ADD COLUMN outlet_perch_low_tide_m numeric,
    ADD COLUMN inlet_shape_code integer,
    ADD COLUMN inlet_type_code integer,
    ADD COLUMN inlet_grade_code integer,
    ADD COLUMN inlet_width_m numeric,
    ADD COLUMN inlet_height_m numeric,
    ADD COLUMN inlet_substrate_water_width_m numeric,
    ADD COLUMN inlet_water_depth_m numeric,
    ADD COLUMN substrate_matches_stream_code integer,
    ADD COLUMN substrate_type_code integer,
    ADD COLUMN substrate_coverage_code integer,
    ADD COLUMN physical_barrier_severity_code integer,
    ADD COLUMN dry_passage boolean,
    ADD COLUMN height_above_dry_passage_m numeric,
    ADD COLUMN structure_comments character varying(100000);

UPDATE featurecopy.temp_tidal_structure_5 SET structure_id = gen_random_uuid();
UPDATE featurecopy.temp_tidal_structure_5 SET primary_structure = false;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_shape_code =
    CASE
    WHEN outlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN outlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN outlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'box' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN outlet_shape = 'box bridge' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    WHEN outlet_shape = 'elliptical' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN outlet_shape = 'open arc' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN outlet_shape = 'round' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_armouring_code =
    CASE
    WHEN outlet_armouring = 'extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'extensive')
    WHEN outlet_armouring = 'none' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'none')
    WHEN outlet_armouring = 'notextensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    WHEN outlet_armouring = 'not_extensive' THEN (SELECT code FROM stream_crossings.armouring_codes WHERE name_en = 'not extensive')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_grade_code =
    CASE
    WHEN outlet_grade = 'cascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'cascade')
    WHEN outlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN outlet_grade = 'FF' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall')
    WHEN outlet_grade = 'Ffcascade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'free fall onto cascade')
    WHEN outlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN outlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_width_m = CASE WHEN outlet_width ~ '[0-9]+' THEN outlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_height_m = CASE WHEN outlet_height ~ '[0-9]+' THEN outlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET structure_length_m = CASE WHEN structure_length ~ '[0-9]+' THEN structure_length::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_substrate_water_width_m = CASE WHEN outlet_ww ~ '[0-9]+' THEN outlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_water_depth_m = CASE WHEN outlet_depth ~ '[0-9]+' THEN outlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET outlet_perch_low_tide_m = CASE WHEN outlet_drop_to_water_surface ~ '[0-9]+' THEN outlet_drop_to_water_surface::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_shape_code = 
    CASE
    WHEN inlet_shape = '1' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'round culvert')
    WHEN inlet_shape = '2' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'pipe arch/elliptical culvert')
    WHEN inlet_shape = '3' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'open bottom arch bridge/culvert')
    WHEN inlet_shape = '4' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box culvert')
    WHEN inlet_shape = '5' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'bridge with side slopes')
    WHEN inlet_shape = '6' THEN (SELECT code FROM stream_crossings.shape_codes WHERE name_en = 'box/bridge with abutments')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_type_code =
    CASE
    WHEN inlet_type = 'headwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall')
    WHEN inlet_type = 'headwall_and_wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'head_wing' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'headwall and wingwalls')
    WHEN inlet_type = 'mitered' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'mitered_to_slope' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'mitered to slope')
    WHEN inlet_type = 'none' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'flush')
    WHEN inlet_type = 'other' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'other')
    WHEN inlet_type = 'projecting' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'projecting')
    WHEN inlet_type = 'wingwall' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    WHEN inlet_type = 'wingwalls' THEN (SELECT code FROM stream_crossings.inlet_type_codes WHERE name_en = 'wingwalls')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_grade_code =
    CASE
    WHEN inlet_grade = 'CCS' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'clogged/collapsed/submerged')
    WHEN inlet_grade = 'inlet_drop' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'inlet drop')
    WHEN inlet_grade = 'perched' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'perched')
    WHEN inlet_grade = 'stream_grade' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'at stream grade')
    WHEN inlet_grade = 'unknown' THEN (SELECT code FROM stream_crossings.grade_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_width_m = CASE WHEN inlet_width ~ '[0-9]+' THEN inlet_width::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_height_m = CASE WHEN inlet_height ~ '[0-9]+' THEN inlet_height::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_substrate_water_width_m = CASE WHEN inlet_ww ~ '[0-9]+' THEN inlet_ww::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET inlet_water_depth_m = CASE WHEN inlet_depth ~ '[0-9]+' THEN inlet_depth::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET substrate_matches_stream_code =
    CASE
    WHEN structure_substrate_matches_stream = 'comparable' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'comparable')
    WHEN structure_substrate_matches_stream = 'contrasting' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'contrasting')
    WHEN structure_substrate_matches_stream = 'none' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'none')
    WHEN structure_substrate_matches_stream = 'notAppropriate' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'not appropriate')
    WHEN structure_substrate_matches_stream = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_matches_stream_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET substrate_type_code =
    CASE
    WHEN structure_substrate_type = 'bedrock' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'bedrock')
    WHEN structure_substrate_type = 'boulder' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'boulder')
    WHEN structure_substrate_type = 'cobble' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'cobble')
    WHEN structure_substrate_type = 'gravel' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'gravel')
    WHEN structure_substrate_type = 'none' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'none')
    WHEN structure_substrate_type = 'sand' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'sand')
    WHEN structure_substrate_type = 'silt' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'muck/silt')
    WHEN structure_substrate_type = 'unknown' THEN (SELECT code FROM stream_crossings.substrate_type_codes WHERE name_en = 'unknown')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET substrate_coverage_code =
    CASE
    WHEN structure_substrate_coverage = 'none' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = 'none')
    WHEN structure_substrate_coverage = '25' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '25%-49%')
    WHEN structure_substrate_coverage = '50' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '50%-74%')
    WHEN structure_substrate_coverage = '75' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '75%-99%')
    WHEN structure_substrate_coverage = '100' THEN (SELECT code FROM stream_crossings.substrate_coverage_codes WHERE name_en = '100%')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET physical_barrier_severity_code =
    CASE
    WHEN severity = 'minor' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'minor')
    WHEN severity = 'moderate' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'moderate')
    WHEN severity = 'severe' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'severe')
    WHEN severity = 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_severity_codes WHERE name_en = 'none')
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET dry_passage = 
    CASE
    WHEN dry_passage_through_structure = 'yes' THEN true
    WHEN dry_passage_through_structure = 'no' THEN false
    ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET height_above_dry_passage_m = CASE WHEN height_above_dry_passage ~ '[0-9]+' THEN height_above_dry_passage::numeric ELSE NULL END;
UPDATE featurecopy.temp_tidal_structure_5 SET structure_comments = structure_5_comments WHERE structure_5_comments IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_5 SET structure_comments = structure_comments || '; other physical barriers: ' || other_physical_barriers WHERE other_physical_barriers IS NOT NULL;
UPDATE featurecopy.temp_tidal_structure_5 SET structure_comments = structure_comments || '; inlet comments: ' || other_inlet_type WHERE other_inlet_type IS NOT NULL AND other_inlet_type NOT IN ('Unknown', 'NA');

--insert into material mapping and physical barrier mapping tables
DELETE FROM {script.tidalMaterialMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_5);
INSERT INTO {script.tidalMaterialMappingTable} (structure_id, material_code, cabd_assessment_id)
    SELECT
        structure_id,
        CASE
        WHEN structure_material = 'combination' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'concrete' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'concrete')
        WHEN structure_material = 'fiberglass' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'other')
        WHEN structure_material = 'metal' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'metal')
        WHEN structure_material = 'plastic' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'plastic')
        WHEN structure_material = 'rock' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'stone')
        WHEN structure_material = 'wood' THEN (SELECT code FROM stream_crossings.material_codes WHERE name_en = 'wood')
        END AS material_code,
        cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_5 WHERE structure_material IS NOT NULL AND structure_material != 'NA';

DELETE FROM {script.tidalPhysicalBarrierMappingTable} WHERE cabd_assessment_id IN (SELECT cabd_assessment_id FROM featurecopy.temp_tidal_structure_5);
DROP TABLE IF EXISTS featurecopy.temp;
CREATE TABLE featurecopy.temp AS
    SELECT structure_id,
    UNNEST(STRING_TO_ARRAY(physical_barriers, ',')) AS physical_barriers,
    cabd_assessment_id
    FROM featurecopy.temp_tidal_structure_5
    WHERE physical_barriers IS NOT NULL;
INSERT INTO {script.tidalPhysicalBarrierMappingTable} (structure_id, physical_barrier_code, cabd_assessment_id)
    SELECT structure_id,
    CASE
    WHEN physical_barriers ILIKE '%debris%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'debris')
    WHEN physical_barriers ILIKE 'deformation' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'deformation')
    WHEN physical_barriers ILIKE '%dry%' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'dry')
    WHEN physical_barriers = 'fencing' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'fencing')
    WHEN physical_barriers IN ('FF', 'Free_fall') THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'free falls')
    WHEN physical_barriers ILIKE 'none' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'none')
    WHEN physical_barriers ILIKE 'other' THEN (SELECT code FROM stream_crossings.physical_barrier_codes WHERE name_en = 'other')
    END AS physical_barrier_code,
    cabd_assessment_id
    FROM featurecopy.temp;
DROP TABLE featurecopy.temp;

------------------------------------------
-- tidal - combine all structures
------------------------------------------

DROP TABLE IF EXISTS {script.tidalStructures};
CREATE TABLE {script.tidalStructures} AS TABLE stream_crossings.tidal_structures WITH NO DATA;

INSERT INTO {script.tidalStructures}
    (site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
    )
SELECT
    site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments,
    passability_status_code
FROM featurecopy.temp_tidal_structure_1;

INSERT INTO {script.tidalStructures}
    (site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_tidal_structure_2;

INSERT INTO {script.tidalStructures}
    (site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_tidal_structure_3;

INSERT INTO {script.tidalStructures}
    (site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_tidal_structure_4;

INSERT INTO {script.tidalStructures}
    (site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
    )
SELECT
    site_id,
    structure_id,
    data_source_id,
    cabd_assessment_id,
    structure_number,
    primary_structure,
    outlet_shape_code,
    outlet_armouring_code,
    outlet_grade_code,
    outlet_width_m,
    outlet_height_m,
    structure_length_m,
    outlet_substrate_water_width_m,
    outlet_water_depth_m,
    outlet_perch_low_tide_m,
    inlet_shape_code,
    inlet_type_code,
    inlet_grade_code,
    inlet_width_m,
    inlet_height_m,
    inlet_substrate_water_width_m,
    inlet_water_depth_m,
    substrate_matches_stream_code,
    substrate_type_code,
    substrate_coverage_code,
    physical_barrier_severity_code,
    dry_passage,
    height_above_dry_passage_m,
    structure_comments
FROM featurecopy.temp_tidal_structure_5;

DROP TABLE featurecopy.temp_tidal_structure_1;
DROP TABLE featurecopy.temp_tidal_structure_2;
DROP TABLE featurecopy.temp_tidal_structure_3;
DROP TABLE featurecopy.temp_tidal_structure_4;
DROP TABLE featurecopy.temp_tidal_structure_5;

ALTER TABLE {script.tidalStructures} ALTER COLUMN structure_id SET NOT NULL;
ALTER TABLE {script.tidalStructures} ADD PRIMARY KEY (structure_id);

"""

script.do_work(query)