--This script should be run as the last step after populating attributes.
--This script updates DAMS only.

--Ensure various fields are populated with most up-to-date info after mapping from multiple sources
BEGIN;

UPDATE
    featurecopy.dams
SET 
    size_class_code =
    CASE
    WHEN "height_m" < 5 THEN 1
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN 2
    WHEN "height_m" >= 15 THEN 3
    ELSE 4 END,

    reservoir_present =
    CASE
    WHEN reservoir_area_skm IS NOT NULL OR reservoir_depth_m IS NOT NULL THEN TRUE
    ELSE reservoir_present END;

UPDATE featurecopy.dams AS cabd SET passability_status_code = 2 FROM fishways.fishways AS f WHERE cabd.cabd_id = f.dam_id;
UPDATE featurecopy.dams SET passability_status_code = 1 WHERE passability_status_code IS NULL;

--Various spatial joins/queries to populate fields
--TO DO: Change original_point, snapped_point to EPSG:4326, populate lat/long fields, drop column geometry
UPDATE featurecopy.dams AS dams SET original_point = ST_Transform(dams.original_point, 4326);
UPDATE featurecopy.dams AS dams SET snapped_point = ST_Transform(dams.snapped_point, 4326);
UPDATE featurecopy.dams AS dams SET longitude = ST_X(dams.snapped_point);
UPDATE featurecopy.dams AS dams SET latitude = ST_Y(dams.snapped_point);
UPDATE featurecopy.dams AS dams DROP INDEX IF EXISTS dams_geometry_geom_idx;
UPDATE featurecopy.dams AS dams DROP COLUMN geometry;
UPDATE featurecopy.dams AS dams REINDEX INDEX dams_idx;
UPDATE featurecopy.dams AS dams SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, dams.snapped_point);
UPDATE featurecopy.dams AS dams SET nhn_workunit_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dams.snapped_point);
UPDATE featurecopy.dams AS dams SET sub_sub_drainage_name = n.sub_sub_drainage_area FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, dams.snapped_point);
UPDATE featurecopy.dams AS dams SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, dams.snapped_point);

--TO DO: Add foreign table to reference ecatchment and eflowpath tables, make sure 2 lines below work
--Should waterbody name simply be overwritten here as long as we have a value from the chyf networks?
--UPDATE featurecopy.dams AS cabd SET waterbody_name_en = c.name FROM fpoutput.ecatchment AS c WHERE st_contains(c.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;
--UPDATE featurecopy.dams AS cabd SET waterbody_name_en = f.name FROM fpoutput.eflowpath AS f WHERE st_contains(f.geometry, cabd.geometry) AND waterbody_name_en IS NOT NULL;

COMMIT;

--Change null values to "unknown" for user benefit
BEGIN;

UPDATE featurecopy.dams SET construction_type_code = 9 WHERE construction_type_code IS NULL;
UPDATE featurecopy.dams SET ownership_type_code = 7 WHERE ownership_type_code IS NULL;
UPDATE featurecopy.dams SET use_code = 11 WHERE use_code IS NULL;
UPDATE featurecopy.dams SET function_code = 13 WHERE function_code IS NULL;
UPDATE featurecopy.dams SET turbine_type_code = 5 WHERE turbine_type_code IS NULL;
UPDATE featurecopy.dams SET operating_status_code = 5 WHERE operating_status_code IS NULL;
UPDATE featurecopy.dams SET up_passage_type_code = 9 WHERE up_passage_type_code IS NULL;

COMMIT;

--Set completeness level code
BEGIN;

UPDATE featurecopy.dams SET complete_level_code = 
    CASE 
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> 5
        AND use_code <> 11
        AND function_code <> 13
        AND construction_type_code <> 9
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> 7)
        AND condition_code IS NOT NULL 
        AND reservoir_present IS NOT NULL 
        AND expected_life IS NOT NULL
        AND up_passage_type_code <> 9
        AND down_passage_route_code IS NOT NULL
        AND length_m IS NOT NULL)
        THEN 4
    WHEN
        ((dam_name_en IS NOT NULL OR dam_name_fr IS NOT NULL)
        AND (waterbody_name_en IS NOT NULL OR waterbody_name_fr IS NOT NULL) 
        AND operating_status_code <> 5
        AND use_code <> 11
        AND function_code <> 13
        AND construction_year IS NOT NULL 
        AND height_m IS NOT NULL
        AND ("owner" IS NOT NULL OR ownership_type_code <> 7))
        THEN 3
    WHEN
        (((dam_name_en IS NULL AND dam_name_fr IS NULL)
        OR (waterbody_name_en IS NULL AND waterbody_name_fr IS NULL)
        OR operating_status_code = 5
        OR height_m IS NULL 
        OR use_code = 11) 
        AND (function_code <> 13 
        OR construction_type_code <> 9 
        OR construction_year IS NOT NULL 
        OR ("owner" IS NOT NULL OR ownership_type_code <> 7)))
        THEN 2
    ELSE 1 END;

COMMIT;

--drop original fields we no longer need
BEGIN;

ALTER TABLE featurecopy.dams
    DROP COLUMN fid,
    DROP COLUMN data_source,
    DROP COLUMN data_source_text,
    DROP COLUMN data_source_id,
    DROP COLUMN decommissioned_yn,
    DROP COLUMN duplicates_yn,
    DROP COLUMN fishway_yn,
    DROP COLUMN canfishpass_id,
    DROP COLUMN barrier_ind,
    DROP COLUMN IF EXISTS dups_ab_basefeatures,
    DROP COLUMN IF EXISTS dups_canvec_manmade,
    DROP COLUMN IF EXISTS dups_cdai,
    DROP COLUMN IF EXISTS dups_cehq,
    DROP COLUMN IF EXISTS dups_cgndb,
    DROP COLUMN IF EXISTS dups_fao,
    DROP COLUMN IF EXISTS dups_fishwerks,
    DROP COLUMN IF EXISTS dups_fwa,
    DROP COLUMN IF EXISTS dups_gfielding,
    DROP COLUMN IF EXISTS dups_goodd,
    DROP COLUMN IF EXISTS dups_grand,
    DROP COLUMN IF EXISTS dups_lsds,
    DROP COLUMN IF EXISTS dups_nbhn,
    DROP COLUMN IF EXISTS dups_ncc,
    DROP COLUMN IF EXISTS dups_nhn,
    DROP COLUMN IF EXISTS dups_nlprov,
    DROP COLUMN IF EXISTS dups_npdp,
    DROP COLUMN IF EXISTS dups_nswf,
    DROP COLUMN IF EXISTS dups_odi,
    DROP COLUMN IF EXISTS dups_ohn,
    DROP COLUMN IF EXISTS dups_publicdamskml,
    DROP COLUMN IF EXISTS dups_wrispublicdams,
    DROP COLUMN reference_url,
    DROP COLUMN reviewer_classification,
    DROP COLUMN reviewer_comments,
    DROP COLUMN multipoint_yn;

COMMIT;