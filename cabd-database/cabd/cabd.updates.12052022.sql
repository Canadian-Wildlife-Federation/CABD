--Data structure changes made Dec 2, 2022 prior to audit log implementation
--No column values for individual features have been changed as part of this script

--change column names
ALTER TABLE dams.dams RENAME COLUMN expected_life TO expected_end_of_life;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN expected_life_ds TO expected_end_of_life_ds;

ALTER TABLE dams.dams RENAME COLUMN construction_type_code TO structure_type_code;
ALTER TABLE dams.dams_attribute_source RENAME COLUMN construction_type_code_ds TO structure_type_code_ds;


--add new columns
ALTER TABLE dams.dams ADD COLUMN construction_material_code int2;
ALTER TABLE dams.dams_attribute_source ADD COLUMN construction_material_code_ds uuid;

ALTER TABLE dams.dams ADD COLUMN use_conservation_code int2;
ALTER TABLE dams.dams_attribute_source ADD COLUMN use_conservation_code_ds uuid;


--modify coded value tables
ALTER TABLE dams.construction_type_codes RENAME TO structure_type_codes;

INSERT INTO dams.dam_use_codes (code, name_en) VALUES (12, 'Wildlife Conservation');

CREATE TABLE IF NOT EXISTS dams.construction_material_codes (
    code smallint NOT NULL,
    name_en character varying(32) COLLATE pg_catalog."default" NOT NULL,
    description_en text COLLATE pg_catalog."default",
    name_fr character varying(64) COLLATE pg_catalog."default",
    description_fr text COLLATE pg_catalog."default",
    CONSTRAINT dam_construction_materials_pk PRIMARY KEY (code)
    );
ALTER TABLE IF EXISTS dams.construction_material_codes
    OWNER to cabd;
COMMENT ON TABLE dams.construction_material_codes
    IS 'Reference table for structure construction materials.';
COMMENT ON COLUMN dams.construction_material_codes.code
    IS 'Code referencing the primary construction material of the structure.';
COMMENT ON COLUMN dams.construction_material_codes.name_en
    IS 'Primary construction material of the structure.';

INSERT INTO dams.construction_material_codes (code, name_en, name_fr)
VALUES 
    (1, 'Concrete', 'Concrete'),
    (2, 'Masonry', 'Masonry'),
    (3, 'Earth', 'Earth'),
    (4, 'Rock', 'Rock'),
    (5, 'Timber', 'Timber'),
    (6, 'Steel', 'Steel'),
    (7, 'Other', 'Other'),
    (99, 'Unknown', 'Unknown');


--update views

--english
DROP VIEW cabd.dams_view_en;

CREATE OR REPLACE VIEW cabd.dams_view_en
AS
SELECT d.cabd_id,
    'dams'::text AS feature_type,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.facility_name_en,
    d.facility_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_en AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name_en AS ownership_type,
    d.municipality,
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
    d.operating_status_code,
    os.name_en AS operating_status,
    d.removed_year,
    d.use_code,
    duc.name_en AS dam_use,
    d.use_irrigation_code,
    c1.name_en AS use_irrigation,
    d.use_electricity_code,
    c2.name_en AS use_electricity,
    d.use_supply_code,
    c3.name_en AS use_supply,
    d.use_floodcontrol_code,
    c4.name_en AS use_floodcontrol,
    d.use_recreation_code,
    c5.name_en AS use_recreation,
    d.use_navigation_code,
    c6.name_en AS use_navigation,
    d.use_fish_code,
    c7.name_en AS use_fish,
    d.use_pollution_code,
    c8.name_en AS use_pollution,
    d.use_invasivespecies_code,
    c9.name_en AS use_invasivespecies,
    d.use_conservation_code,
    c10.name_en AS use_conservation,
    d.use_other_code,
    c11.name_en AS use_other,
    d.lake_control_code,
    lk.name_en AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_end_of_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_en AS function_name,
    d.condition_code,
    dc.name_en AS dam_condition,
    d.structure_type_code,
    dst.name_en AS structure_type,
    d.construction_material_code,
    dcm.name_en AS construction_material,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name_en AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name_en AS spillway_type,
    d.reservoir_present,
    d.reservoir_area_skm,
    d.reservoir_depth_m,
    d.storage_capacity_mcm,
    d.avg_rate_of_discharge_ls,
    d.degree_of_regulation_pc,
    d.provincial_flow_req,
    d.federal_flow_req,
    d.catchment_area_skm,
    d.hydro_peaking_system,
    d.upstream_linear_km,
    d.generating_capacity_mwh,
    d.turbine_number,
    d.turbine_type_code,
    dt.name_en AS turbine_type,
    d.up_passage_type_code,
    up.name_en AS up_passage_type,
    d.down_passage_route_code,
    down.name_en AS down_passage_route,
    d.last_modified,
    d.use_analysis,
    d.comments,
    d.passability_status_code,
    ps.name_en AS passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name_en AS complete_level,
    d.snapped_point AS geometry
FROM dams.dams d
    JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
    LEFT JOIN cabd.barrier_ownership_type_codes ow ON ow.code = d.ownership_type_code
    LEFT JOIN dams.operating_status_codes os ON os.code = d.operating_status_code
    LEFT JOIN dams.dam_use_codes duc ON duc.code = d.use_code
    LEFT JOIN dams.use_codes c1 ON c1.code = d.use_irrigation_code
    LEFT JOIN dams.use_codes c2 ON c2.code = d.use_electricity_code
    LEFT JOIN dams.use_codes c3 ON c3.code = d.use_supply_code
    LEFT JOIN dams.use_codes c4 ON c4.code = d.use_floodcontrol_code
    LEFT JOIN dams.use_codes c5 ON c5.code = d.use_recreation_code
    LEFT JOIN dams.use_codes c6 ON c6.code = d.use_navigation_code
    LEFT JOIN dams.use_codes c7 ON c7.code = d.use_fish_code
    LEFT JOIN dams.use_codes c8 ON c8.code = d.use_pollution_code
    LEFT JOIN dams.use_codes c9 ON c9.code = d.use_invasivespecies_code
    LEFT JOIN dams.use_codes c10 ON c10.code = d.use_conservation_code
    LEFT JOIN dams.use_codes c11 ON c11.code = d.use_other_code
    LEFT JOIN dams.function_codes f ON f.code = d.function_code
    LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
    LEFT JOIN dams.structure_type_codes dst ON dst.code = d.structure_type_code
    LEFT JOIN dams.construction_material_codes dcm ON dcm.code = d.construction_material_code
    LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
    LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
    LEFT JOIN dams.turbine_type_codes dt ON dt.code = d.turbine_type_code
    LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
    LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
    LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
    LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
    LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code;

GRANT ALL ON TABLE cabd.dams_view_en TO cabd;

--french
DROP VIEW cabd.dams_view_fr;

CREATE OR REPLACE VIEW cabd.dams_view_fr
AS
SELECT d.cabd_id,
    'dams'::text AS feature_type,
    'features/datasources/'::text || d.cabd_id AS datasource_url,
    st_y(d.snapped_point) AS latitude,
    st_x(d.snapped_point) AS longitude,
    d.dam_name_en,
    d.dam_name_fr,
    d.facility_name_en,
    d.facility_name_fr,
    d.waterbody_name_en,
    d.waterbody_name_fr,
    d.reservoir_name_en,
    d.reservoir_name_fr,
    d.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    d.province_territory_code,
    pt.name_fr AS province_territory,
    d.owner,
    d.ownership_type_code,
    ow.name_fr AS ownership_type,
    d.municipality,
    d.provincial_compliance_status,
    d.federal_compliance_status,
    d.operating_notes,
    d.operating_status_code,
    os.name_fr AS operating_status,
    d.removed_year,
    d.use_code,
    duc.name_fr AS dam_use,
    d.use_irrigation_code,
    c1.name_fr AS use_irrigation,
    d.use_electricity_code,
    c2.name_fr AS use_electricity,
    d.use_supply_code,
    c3.name_fr AS use_supply,
    d.use_floodcontrol_code,
    c4.name_fr AS use_floodcontrol,
    d.use_recreation_code,
    c5.name_fr AS use_recreation,
    d.use_navigation_code,
    c6.name_fr AS use_navigation,
    d.use_fish_code,
    c7.name_fr AS use_fish,
    d.use_pollution_code,
    c8.name_fr AS use_pollution,
    d.use_invasivespecies_code,
    c9.name_fr AS use_invasivespecies,
    d.use_conservation_code,
    c10.name_fr AS use_conservation,
    d.use_other_code,
    c11.name_fr AS use_other,
    d.lake_control_code,
    lk.name_fr AS lake_control,
    d.construction_year,
    d.assess_schedule,
    d.expected_end_of_life,
    d.maintenance_last,
    d.maintenance_next,
    d.function_code,
    f.name_fr AS function_name,
    d.condition_code,
    dc.name_fr AS dam_condition,
    d.structure_type_code,
    dst.name_fr AS structure_type,
    d.construction_material_code,
    dcm.name_fr AS construction_material,
    d.height_m,
    d.length_m,
    d.size_class_code,
    ds.name_fr AS size_class,
    d.spillway_capacity,
    d.spillway_type_code,
    dsp.name_fr AS spillway_type,
    d.reservoir_present,
    d.reservoir_area_skm,
    d.reservoir_depth_m,
    d.storage_capacity_mcm,
    d.avg_rate_of_discharge_ls,
    d.degree_of_regulation_pc,
    d.provincial_flow_req,
    d.federal_flow_req,
    d.catchment_area_skm,
    d.hydro_peaking_system,
    d.upstream_linear_km,
    d.generating_capacity_mwh,
    d.turbine_number,
    d.turbine_type_code,
    dt.name_fr AS turbine_type,
    d.up_passage_type_code,
    up.name_fr AS up_passage_type,
    d.down_passage_route_code,
    down.name_fr AS down_passage_route,
    d.last_modified,
    d.use_analysis,
    d.comments,
    d.passability_status_code,
    ps.name_fr AS passability_status,
    d.passability_status_note,
    d.complete_level_code,
    cl.name_fr AS complete_level,
    d.snapped_point AS geometry
FROM dams.dams d
    JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
    LEFT JOIN cabd.barrier_ownership_type_codes ow ON ow.code = d.ownership_type_code
    LEFT JOIN dams.operating_status_codes os ON os.code = d.operating_status_code
    LEFT JOIN dams.dam_use_codes duc ON duc.code = d.use_code
    LEFT JOIN dams.use_codes c1 ON c1.code = d.use_irrigation_code
    LEFT JOIN dams.use_codes c2 ON c2.code = d.use_electricity_code
    LEFT JOIN dams.use_codes c3 ON c3.code = d.use_supply_code
    LEFT JOIN dams.use_codes c4 ON c4.code = d.use_floodcontrol_code
    LEFT JOIN dams.use_codes c5 ON c5.code = d.use_recreation_code
    LEFT JOIN dams.use_codes c6 ON c6.code = d.use_navigation_code
    LEFT JOIN dams.use_codes c7 ON c7.code = d.use_fish_code
    LEFT JOIN dams.use_codes c8 ON c8.code = d.use_pollution_code
    LEFT JOIN dams.use_codes c9 ON c9.code = d.use_invasivespecies_code
    LEFT JOIN dams.use_codes c10 ON c10.code = d.use_conservation_code
    LEFT JOIN dams.use_codes c11 ON c11.code = d.use_other_code
    LEFT JOIN dams.function_codes f ON f.code = d.function_code
    LEFT JOIN dams.condition_codes dc ON dc.code = d.condition_code
    LEFT JOIN dams.structure_type_codes dst ON dst.code = d.structure_type_code
    LEFT JOIN dams.construction_material_codes dcm ON dcm.code = d.construction_material_code
    LEFT JOIN dams.size_codes ds ON ds.code = d.size_class_code
    LEFT JOIN dams.spillway_type_codes dsp ON dsp.code = d.spillway_type_code
    LEFT JOIN dams.turbine_type_codes dt ON dt.code = d.turbine_type_code
    LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
    LEFT JOIN dams.downstream_passage_route_codes down ON down.code = d.down_passage_route_code
    LEFT JOIN dams.dam_complete_level_codes cl ON cl.code = d.complete_level_code
    LEFT JOIN dams.lake_control_codes lk ON lk.code = d.lake_control_code
    LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = d.nhn_watershed_id::text
    LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code;

GRANT ALL ON TABLE cabd.dams_view_fr TO cabd;

--update feature type metadata table
UPDATE cabd.feature_type_metadata
SET 
    field_name = 'expected_end_of_life',
    name_en = 'Expected End of Life',
    name_fr = 'Expected End of Life',
    description_en = 'The year the structure will reach its expected end of life.',
    description_fr = 'The year the structure will reach its expected end of life.'
WHERE field_name = 'expected_life';

UPDATE cabd.feature_type_metadata
SET
    field_name = 'structure_type_code',
    name_en = 'Structure Type Code',
    name_fr = 'Structure Type Code',
    description_en = 'The type of structure.',
    description_fr = 'The type of structure.'
WHERE field_name = 'construction_type_code';

UPDATE cabd.feature_type_metadata
SET
    field_name = 'structure_type',
    name_en = 'Structure Type',
    name_fr = 'Structure Type',
    description_en = 'The type of structure.',
    description_fr = 'The type of structure.'
WHERE field_name = 'construction_type';

--change vw_all_order values so we can insert our new values

UPDATE cabd.feature_type_metadata
SET vw_all_order = vw_all_order + 2
WHERE view_name = 'cabd.dams_view'
AND vw_all_order BETWEEN 31 and 96;

UPDATE cabd.feature_type_metadata
SET vw_all_order = vw_all_order + 2
WHERE view_name = 'cabd.dams_view'
AND vw_all_order BETWEEN 63 and 98;

INSERT INTO cabd.feature_type_metadata (
    view_name,
    field_name,
    name_en,
    description_en,
    is_link,
    data_type,
    vw_simple_order,
    vw_all_order,
    include_vector_tile,
    value_options_reference,
    name_fr,
    description_fr,
    is_name_search)
VALUES
    ('cabd.dams_view',
    'construction_material_code',
    'Construction Material Code',
    'The primary construction material of the structure.',
    false,
    'integer',
    NULL,
    31,
    false,
    'dams.construction_material_codes;code;name;description',
    'Construction Material Code',
    'The primary construction material of the structure.',
    false
    ),

    ('cabd.dams_view',
    'construction_material',
    'Construction Material',
    'The primary construction material of the structure.',
    false,
    'varchar(32)',
    NULL,
    32,
    false,
    'dams.construction_material_codes;;code;name;description',
    'Construction Material',
    'The primary construction material of the structure.',
    false
    ),

    ('cabd.dams_view',
    'use_conservation_code',
    'Use Wildlife Conservation Code',
    'Indicates the structure is used for wildlife conservation purposes, and the extent to which wildlife conservation is a planned use.',
    false,
    'integer',
    NULL,
    63,
    false,
    'dams.use_codes;code;name;description',
    'Use Wildlife Conservation Code',
    'Indicates the structure is used for wildlife conservation purposes, and the extent to which wildlife conservation is a planned use.',
    false
    ),

    ('cabd.dams_view',
    'use_conservation',
    'Use Wildlife Conservation',
    'Indicates the structure is used for wildlife conservation purposes, and the extent to which wildlife conservation is a planned use.',
    false,
    'varchar(32)',
    NULL,
    64,
    false,
    'dams.use_codes;;name;description',
    'Use Wildlife Conservation',
    'Indicates the structure is used for wildlife conservation purposes, and the extent to which wildlife conservation is a planned use.',
    false
    )
;

update cabd.feature_type_metadata set value_options_reference = 'dams.structure_type_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'structure_type';

update cabd.feature_type_metadata set value_options_reference = 'dams.structure_type_codes;code;name;description' where view_name = 'cabd.dams_view' and field_name = 'structure_type_code';

update cabd.feature_type_metadata set value_options_reference = 'dams.construction_material_codes;;name;description' where view_name = 'cabd.dams_view' and field_name = 'construction_material'

-- check your work
-- SELECT * FROM cabd.feature_type_metadata
-- WHERE view_name = 'cabd.dams_view'
-- ORDER BY vw_all_order ASC;