CREATE SCHEMA IF NOT EXISTS modelled_crossings;

-- CODED TABLES SETUP --

drop table if exists modelled_crossings.crossing_type_codes;

create table modelled_crossings.crossing_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.crossing_type_codes (code, name_en, description_en) values
(1, 'open-bottom structure', 'Open-bottom structures may include bridges, bottomless arch structures, and open bottom box culverts. Open-bottom structures generally have less impact on aquatic habitat than closed-bottom structures.'),
(2, 'closed-bottom structure', 'Closed-bottom structures may include box culverts, pipe arch culverts, and pipe culverts. They may be used on public roads, forest roads, driveways and in areas where difficult terrain limits other watercourse crossing options.'),
(3, 'ford-like structure', 'Ford-like structures are low-profile crossings typically used for low-traffic or intermittent use roads to facilitate crossing a stream.'),
(4, 'no crossing', 'There is no crossing where anticipated, usually because of incorrect road or stream location on maps.'),
(99, 'unknown', 'The crossing type is unknown.');

update modelled_crossings.crossing_type_codes SET name_fr = name_en, description_fr = description_en;

drop table if exists modelled_crossings.crossing_subtype_codes;

create table modelled_crossings.crossing_subtype_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.crossing_subtype_codes (code, name_en, description_en) values
(1, 'bridge', 'A bridge has a deck supported by abutments (or stream banks). It may have more than one cell or section separated by one or more piers.'),
(2, 'culvert', 'A culvert is a tunnel carrying a stream under a road or railway. A culvert may act as a bridge for traffic to pass on it.'),
(3, 'multiple culvert', 'A multiple culvert consists of two or more culverts at a site. Multiple culverts may be installed on wide channels or in areas with high velocities at the outlet.'),
(4, 'ford', 'A ford is a shallow, open stream crossing, in which vehicles pass through the water. Fords may be armored to decrease erosion, and may include pipes to allow flow through the ford.'),
(5, 'no upstream channel', 'A site where water crosses a road through a culvert but no road-stream crossing occurs because there is no channel up-gradient of the road. This can occur at the very headwaters of a stream or where a road crosses a wetland that lacks a stream channel.'),
(99, 'unknown', 'The crossing subtype is unknown.');

update modelled_crossings.crossing_subtype_codes SET name_fr = name_en, description_fr = description_en;

drop table if exists modelled_crossings.addressed_status_codes;

create table modelled_crossings.addressed_status_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.addressed_status_codes (code, name_en, description_en) values
(1, 'Decommissioned/ Removed', 'The crossing has been decommissioned or removed.'),
(2, 'Replaced', 'The crossing has been replaced with a fully passable structure.'),
(3, 'Fish Ladder', 'A fish ladder has been added to the crossing to improve fish passage.'),
(4, 'Baffling', 'Baffling has been installed at the crossing to reduce water velocity and provide resting areas for fish.'),
(5, 'Backwatering', 'Backwatering has been added at the crossing to reduce water velocity and turbulence and raise water levels.'),
(6, 'Other', 'The crossing has been addressed in some way to improve fish passage, but the measure is different than those defined in this table.'),
(99, 'Unknown', 'It is unknown whether the crossing has been addressed to improve fish passage.');

update modelled_crossings.addressed_status_codes SET name_fr = name_en, description_fr = description_en;

drop table if exists modelled_crossings.assessment_type_codes;

create table modelled_crossings.assessment_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.assessment_type_codes (code, name_en, description_en) values
(1, 'Rapid', 'A rapid assessment has been conducted at the crossing.'),
(2, 'Full', 'A full assessment has been conducted at the crossing.'),
(3, 'Other', 'An assessment has been conducted at the crossing, but the type of assessment is different than those defined in this table.'),
(4, 'Imagery', 'The crossing has been reviewed through available satellite imagery, but has not been visited in the field.'),
(5, 'Modelled', 'The crossing has not been visited in the field or reviewed in satellite imagery.');

update modelled_crossings.assessment_type_codes SET name_fr = name_en, description_fr = description_en;

-- ADD NEW DATA SOURCES --

INSERT INTO cabd.data_source (
    id,
    name,
    source,
    source_type,
    full_name,
    organization_name,
    data_source_category,
    geographic_coverage,
    source_id_field,
    licence)
SELECT
    gen_random_uuid(),
    name,
    source,
    source_type,
    full_name,
    organization_name,
    data_source_category,
    geographic_coverage,
    source_id_field,
    licence
FROM source_data.modelled_crossing_data_sources;

-- ADD EMPTY FEATURE SOURCE TABLE --

CREATE TABLE IF NOT EXISTS modelled_crossings.feature_source
(
    cabd_id uuid NOT NULL,
    datasource_id uuid NOT NULL,
    datasource_feature_id character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT modelled_feature_source_pkey PRIMARY KEY (cabd_id, datasource_feature_id),
    CONSTRAINT modelled_feature_source_cabd_id_fkey FOREIGN KEY (cabd_id)
        REFERENCES modelled_crossings.modelled_crossings (cabd_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT modelled_data_source_id_fk FOREIGN KEY (datasource_id)
        REFERENCES cabd.data_source (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE RESTRICT,
    CONSTRAINT modelled_feature_source_cabd_id_fk FOREIGN KEY (cabd_id)
        REFERENCES modelled_crossings.modelled_crossings (cabd_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

ALTER TABLE IF EXISTS modelled_crossings.feature_source
    OWNER to cabd;
REVOKE ALL ON TABLE modelled_crossings.feature_source FROM cwf_user;
GRANT ALL ON TABLE modelled_crossings.feature_source TO cabd;
GRANT SELECT ON TABLE modelled_crossings.feature_source TO cwf_user;

-- MAIN TABLE SETUP --

UPDATE modelled_crossings.modelled_crossings AS a SET crossing_type = ct.code FROM modelled_crossings.crossing_type_codes ct WHERE ct.name_en = crossing_type;
UPDATE modelled_crossings.modelled_crossings AS a SET crossing_subtype = ct.code FROM modelled_crossings.crossing_subtype_codes ct WHERE ct.name_en = crossing_subtype;
UPDATE modelled_crossings.modelled_crossings AS a SET passability_status = p.code FROM cabd.passability_status_codes p WHERE p.name_en = passability_status;

ALTER TABLE modelled_crossings.modelled_crossings ALTER COLUMN crossing_type TYPE int2 USING crossing_type::int2;
ALTER TABLE modelled_crossings.modelled_crossings ALTER COLUMN crossing_subtype TYPE int2 USING crossing_subtype::int2;
ALTER TABLE modelled_crossings.modelled_crossings ALTER COLUMN passability_status TYPE int2 USING passability_status::int2;

ALTER TABLE modelled_crossings.modelled_crossings RENAME COLUMN crossing_type TO crossing_type_code;
ALTER TABLE modelled_crossings.modelled_crossings RENAME COLUMN crossing_subtype TO crossing_subtype_code;
ALTER TABLE modelled_crossings.modelled_crossings RENAME COLUMN passability_status TO passability_status_code;

UPDATE modelled_crossings.crossing_type_codes SET name_en = initcap(name_en), name_fr = initcap(name_fr);
UPDATE modelled_crossings.crossing_subtype_codes SET name_en = initcap(name_en), name_fr = initcap(name_fr);

ALTER TABLE modelled_crossings.modelled_crossings DROP COLUMN IF EXISTS new_crossing_subtype;
ALTER TABLE modelled_crossings.modelled_crossings DROP COLUMN IF EXISTS reviewer_comments;

ALTER TABLE modelled_crossings.modelled_crossings 
    ADD CONSTRAINT modelled_crossings_pt_code FOREIGN KEY (province_territory_code)
        REFERENCES cabd.province_territory_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    ADD CONSTRAINT modelled_crossings_ps_code FOREIGN KEY (passability_status_code)
        REFERENCES cabd.passability_status_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    ADD CONSTRAINT modelled_crossings_crossing_type_code_fkey FOREIGN KEY (crossing_type_code)
        REFERENCES modelled_crossings.crossing_type_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    ADD CONSTRAINT modelled_crossings_crossing_subtype_code_fkey FOREIGN KEY (crossing_subtype_code)
        REFERENCES modelled_crossings.crossing_subtype_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    ADD CONSTRAINT modelled_crossings_addressed_status_code_fkey FOREIGN KEY (addressed_status_code)
        REFERENCES modelled_crossings.addressed_status_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    ADD CONSTRAINT modelled_crossings_assessment_type_code_fkey FOREIGN KEY (assessment_type_code)
        REFERENCES modelled_crossings.assessment_type_codes (code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
    ;

-- ENGLISH VIEW --

DROP VIEW IF EXISTS cabd.modelled_crossings_view_en;

CREATE OR REPLACE VIEW cabd.modelled_crossings_view_en
 AS
 SELECT m.cabd_id,
    'modelled_crossings'::text AS feature_type,
    st_y(m.geometry) AS latitude,
    st_x(m.geometry) AS longitude,
    m.crossing_type_code,
    ct.name_en AS crossing_type,
    m.crossing_subtype_code,
    cst.name_en AS crossing_subtype,
    m.crossing_type_source,
    m.passability_status_code,
    ps.name_en AS passability_status,
    m.addressed_status_code,
    ast.name_en AS addressed_status,
    m.assessment_type_code,
    at.name_en AS assessment_type,
    CASE
        WHEN m.addressed_status_code != (SELECT code FROM modelled_crossings.addressed_status_codes WHERE name_en = 'Unknown') THEN true
        ELSE false
        END AS crossing_addressed,
    CASE
        WHEN m.assessment_type_code IN (SELECT code FROM modelled_crossings.assessment_type_codes WHERE name_en IN ('Rapid', 'Full', 'Other')) THEN true
        ELSE false
        END AS crossing_confirmed,
    m.municipality,
    m.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    m.province_territory_code,
    pt.name_en AS province_territory,
    m.stream_name_1,
    m.stream_order,
    m.transport_feature_name,
    m.transport_feature_type,
    m.transport_feature_owner,
    m.railway_operator,
    m.railway_status,
    m.roadway_type,
    m.comments,
    m.use_analysis,
    m.geometry AS geometry
   FROM modelled_crossings.modelled_crossings m
     JOIN cabd.province_territory_codes pt ON m.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = m.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = m.passability_status_code
     LEFT JOIN modelled_crossings.crossing_type_codes ct ON ct.code = m.crossing_type_code
     LEFT JOIN modelled_crossings.crossing_subtype_codes cst ON cst.code = m.crossing_subtype_code
     LEFT JOIN modelled_crossings.addressed_status_codes ast ON ast.code = m.addressed_status_code
     LEFT JOIN modelled_crossings.assessment_type_codes at ON at.code = m.assessment_type_code
    ;

ALTER TABLE cabd.modelled_crossings_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.modelled_crossings_view_en TO cabd;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO egouge;

-- FRENCH VIEW --

DROP VIEW IF EXISTS cabd.modelled_crossings_view_fr;

CREATE OR REPLACE VIEW cabd.modelled_crossings_view_fr
 AS
 SELECT m.cabd_id,
    'modelled_crossings'::text AS feature_type,
    st_y(m.geometry) AS latitude,
    st_x(m.geometry) AS longitude,
    m.crossing_type_code,
    ct.name_fr AS crossing_type,
    m.crossing_subtype_code,
    cst.name_fr AS crossing_subtype,
    m.crossing_type_source,
    m.passability_status_code,
    ps.name_fr AS passability_status,
    m.addressed_status_code,
    ast.name_fr AS addressed_status,
    m.assessment_type_code,
    at.name_fr AS assessment_type,
    CASE
        WHEN m.addressed_status_code != (SELECT code FROM modelled_crossings.addressed_status_codes WHERE name_en = 'Unknown') THEN true
        ELSE false
        END AS crossing_addressed,
    CASE
        WHEN m.assessment_type_code IN (SELECT code FROM modelled_crossings.assessment_type_codes WHERE name_en IN ('Rapid', 'Full', 'Other')) THEN true
        ELSE false
        END AS crossing_confirmed,
    m.municipality,
    m.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    m.province_territory_code,
    pt.name_fr AS province_territory,
    m.stream_name_1,
    m.stream_order,
    m.transport_feature_name,
    m.transport_feature_type,
    m.transport_feature_owner,
    m.railway_operator,
    m.railway_status,
    m.roadway_type,
    m.comments,
    m.use_analysis,
    m.geometry AS geometry
   FROM modelled_crossings.modelled_crossings m
     JOIN cabd.province_territory_codes pt ON m.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = m.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = m.passability_status_code
     LEFT JOIN modelled_crossings.crossing_type_codes ct ON ct.code = m.crossing_type_code
     LEFT JOIN modelled_crossings.crossing_subtype_codes cst ON cst.code = m.crossing_subtype_code
     LEFT JOIN modelled_crossings.addressed_status_codes ast ON ast.code = m.addressed_status_code
     LEFT JOIN modelled_crossings.assessment_type_codes at ON at.code = m.assessment_type_code
    ;
ALTER TABLE cabd.modelled_crossings_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.modelled_crossings_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO egouge;

-- ALL FEATURES VIEW - ENGLISH --

CREATE OR REPLACE VIEW cabd.all_features_view_en
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_en AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_en AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            fishways.structure_name_en,
            fishways.structure_name_fr,
            fishways.province_territory_code,
            fishways.nhn_watershed_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::smallint AS int2,
            NULL::boolean AS "boolean",
            fishways.original_point
           FROM fishways.fishways
        UNION
         SELECT nontidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            nontidal.province_territory_code,
            nontidal.nhn_watershed_id,
            nontidal.municipality::character varying(512) AS municipality,
            nontidal.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN nts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE nts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
             LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure IS TRUE
        UNION
         SELECT tidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            tidal.province_territory_code,
            tidal.nhn_watershed_id,
            tidal.municipality::character varying(512) AS municipality,
            tidal.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
             LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure IS TRUE
        UNION
            SELECT c.cabd_id,
            'modelled_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code,
            c.nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name_1::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.passability_status_code,
            c.use_analysis,
            c.geometry AS snapped_point
        FROM modelled_crossings.modelled_crossings c
             ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.all_features_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.all_features_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.all_features_view_en TO cabd;
GRANT SELECT ON TABLE cabd.all_features_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.all_features_view_en TO egouge;

-- ALL FEATURES VIEW - FRENCH --

CREATE OR REPLACE VIEW cabd.all_features_view_fr
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name_fr AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name_fr AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS barrier_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            fishways.structure_name_en,
            fishways.structure_name_fr,
            fishways.province_territory_code,
            fishways.nhn_watershed_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::smallint AS int2,
            NULL::boolean AS "boolean",
            fishways.original_point
           FROM fishways.fishways
        UNION
         SELECT nontidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            nontidal.province_territory_code,
            nontidal.nhn_watershed_id,
            nontidal.municipality::character varying(512) AS municipality,
            nontidal.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN nts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE nts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
             LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure IS TRUE
        UNION
         SELECT tidal.cabd_id,
            'stream_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            tidal.province_territory_code,
            tidal.nhn_watershed_id,
            tidal.municipality::character varying(512) AS municipality,
            tidal.stream_name::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN ( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text)
                    ELSE ts.passability_status_code::smallint
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
             LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure IS TRUE
        UNION
            SELECT c.cabd_id,
            'modelled_crossings'::text AS barrier_type,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.province_territory_code,
            c.nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name_1::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.passability_status_code,
            c.use_analysis,
            c.geometry AS snapped_point
        FROM modelled_crossings.modelled_crossings c
             ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.all_features_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.all_features_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.all_features_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.all_features_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.all_features_view_fr TO egouge;

-- BARRIERS VIEW - ENGLISH --

CREATE OR REPLACE VIEW cabd.barriers_view_en
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en::character varying(512) AS name_en,
    barriers.name_fr::character varying(512) AS name_fr,
    barriers.province_territory_code,
    pt.name_en AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_en AS nhn_watershed_name,
    barriers.municipality::character varying(512) AS municipality,
    barriers.waterbody_name_en::character varying(512) AS waterbody_name_en,
    barriers.waterbody_name_fr::character varying(512) AS waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code::smallint AS passability_status_code,
    ps.name_en AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS feature_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS feature_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT nontidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.province_territory_code,
            nontidal.nhn_watershed_id,
            nontidal.municipality,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
                CASE
                    WHEN nts.passability_status_code IS NULL THEN (( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text))::integer
                    ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
             LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure IS TRUE
        UNION
         SELECT tidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.province_territory_code,
            tidal.nhn_watershed_id,
            tidal.municipality,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN (( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text))::integer
                    ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
             LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure IS TRUE
        UNION
            SELECT c.cabd_id,
            'modelled_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            c.province_territory_code,
            c.nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name_1::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.passability_status_code,
            c.use_analysis,
            c.geometry AS snapped_point
            FROM modelled_crossings.modelled_crossings c
        ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.barriers_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.barriers_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.barriers_view_en TO cabd;
GRANT SELECT ON TABLE cabd.barriers_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.barriers_view_en TO egouge;

-- BARRIERS VIEW - FRENCH --

CREATE OR REPLACE VIEW cabd.barriers_view_fr
 AS
 SELECT barriers.cabd_id,
    'features/datasources/'::text || barriers.cabd_id AS datasource_url,
    barriers.feature_type,
    barriers.name_en::character varying(512) AS name_en,
    barriers.name_fr::character varying(512) AS name_fr,
    barriers.province_territory_code,
    pt.name_fr AS province_territory,
    barriers.nhn_watershed_id,
    nhn.name_fr AS nhn_watershed_name,
    barriers.municipality::character varying(512) AS municipality,
    barriers.waterbody_name_en::character varying(512) AS waterbody_name_en,
    barriers.waterbody_name_fr::character varying(512) AS waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code::smallint AS passability_status_code,
    ps.name_fr AS passability_status,
    barriers.use_analysis,
        CASE
            WHEN up.cabd_id IS NOT NULL THEN true
            ELSE false
        END AS updates_pending,
    barriers.snapped_point AS geometry
   FROM ( SELECT dams.cabd_id,
            'dams'::text AS feature_type,
            dams.dam_name_en AS name_en,
            dams.dam_name_fr AS name_fr,
            dams.province_territory_code,
            dams.nhn_watershed_id,
            dams.municipality,
            dams.waterbody_name_en,
            dams.waterbody_name_fr,
            dams.reservoir_name_en,
            dams.reservoir_name_fr,
            dams.passability_status_code,
            dams.use_analysis,
            dams.snapped_point
           FROM dams.dams
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS feature_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.nhn_watershed_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.use_analysis,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT nontidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            nontidal.province_territory_code,
            nontidal.nhn_watershed_id,
            nontidal.municipality,
            nontidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
                CASE
                    WHEN nts.passability_status_code IS NULL THEN (( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text))::integer
                    ELSE nts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            nontidal.snapped_point
           FROM stream_crossings.nontidal_sites nontidal
             LEFT JOIN stream_crossings.nontidal_structures nts ON nts.site_id = nontidal.cabd_id AND nts.primary_structure IS TRUE
        UNION
         SELECT tidal.cabd_id,
            'stream_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            tidal.province_territory_code,
            tidal.nhn_watershed_id,
            tidal.municipality,
            tidal.stream_name,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
                CASE
                    WHEN ts.passability_status_code IS NULL THEN (( SELECT passability_status_codes.code
                       FROM cabd.passability_status_codes
                      WHERE passability_status_codes.name_en::text = 'Unknown'::text))::integer
                    ELSE ts.passability_status_code
                END AS passability_status_code,
            NULL::boolean AS "boolean",
            tidal.snapped_point
           FROM stream_crossings.tidal_sites tidal
             LEFT JOIN stream_crossings.tidal_structures ts ON ts.site_id = tidal.cabd_id AND ts.primary_structure IS TRUE
        UNION
            SELECT c.cabd_id,
            'modelled_crossings'::text AS feature_type,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            c.province_territory_code,
            c.nhn_watershed_id,
            c.municipality::character varying(512) AS municipality,
            c.stream_name_1::character varying(512) AS stream_name,
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            NULL::character varying(512) AS "varchar",
            c.passability_status_code,
            c.use_analysis,
            c.geometry AS snapped_point
            FROM modelled_crossings.modelled_crossings c
        ) barriers
     LEFT JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = barriers.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code
     LEFT JOIN cabd.updates_pending up ON up.cabd_id = barriers.cabd_id;

ALTER TABLE cabd.barriers_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.barriers_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.barriers_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.barriers_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.barriers_view_fr TO egouge;

-- FEATURE TYPES TABLE --

INSERT INTO cabd.feature_types (
    type,
    data_view,
    name_en,
    name_fr,
    data_version,
    description,
    data_table,
    feature_source_table
)
VALUES
    ('modelled_crossings',
    'cabd.modelled_crossings_view',
    'Modelled Crossings',
    'Modelled Crossings',
    '1.0',
    'Modelled stream crossings based on locations where a stream is crossed by a road, railway, or trail.',
    '{modelled_crossings.modelled_crossings}',
    'modelled_crossings.feature_source')
;

-- FEATURE TYPE VERSION HISTORY TABLE --

INSERT INTO cabd.feature_type_version_history (type, version, start_date, end_date)
VALUES
('modelled_crossings', '1.0', NULL, NULL);

-- FEATURE TYPE METADATA TABLE --

INSERT INTO cabd.feature_type_metadata (
    view_name,
    field_name,
    shape_field_name,
    name_en,
    description_en,
    data_type,
    vw_simple_order
)
VALUES
(   'cabd.modelled_crossings_view',
    'cabd_id',
    'cabd_id',
    'Barrier Identifier',
    'Unique identifier for the stream crossing',
    'uuid',
    1
),
(   'cabd.modelled_crossings_view',
    'feature_type',
    'feat_type',
    'Feature Type',
    'The type of feature the data point represents',
    'text',
    2
),
(   'cabd.modelled_crossings_view',
    'latitude',
    'lat_dd',
    'Latitude',
    'Latitude of point location of stream crossing in decimal degrees; the point location is only an approximation of the actual stream crossing location.',
    'double precision',
    3
),
(   'cabd.modelled_crossings_view',
    'longitude',
    'long_dd',
    'Longitude',
    'Longitude of point location of stream crossing in decimal degrees; the point location is only an approximation of the actual stream crossing location.',
    'double precision',
    4
),
(   'cabd.modelled_crossings_view',
    'crossing_type_code',
    'cr_typ_c',
    'Crossing Type Code',
    'The crossing type',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'crossing_type',
    'cr_typ',
    'Crossing Type',
    'The crossing type',
    'varchar(32)',
    5
),

(   'cabd.modelled_crossings_view',
    'crossing_type_source',
    'cr_typ_s',
    'Crossing Type Source',
    'The source of the crossing type',
    'varchar',
    6
),
(   'cabd.modelled_crossings_view',
    'crossing_subtype_code',
    'cr_styp_c',
    'Crossing Subtype Code',
    'The crossing subtype',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'crossing_subtype',
    'cr_styp',
    'Crossing Subtype',
    'The crossing subtype',
    'varchar(32)',
    7
),
(   'cabd.modelled_crossings_view',
    'passability_status_code',
    'passstat_c',
    'Passability Status Code',
    'The degree to which the stream crossing acts as a barrier to fish in the upstream direction.',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'passability_status',
    'passstat',
    'Passability Status',
    'The degree to which the stream crossing acts as a barrier to fish in the upstream direction.',
    'varchar(32)',
    8
),
(   'cabd.modelled_crossings_view',
    'addressed_status_code',
    'addstat_c',
    'Addressed Status Code',
    'Indicates whether the crossing has been rehabilitated in some way to improve fish passage.',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'addressed_status',
    'addstat',
    'Addressed Status',
    'Indicates whether the crossing has been rehabilitated in some way to improve fish passage.',
    'varchar(32)',
    9
),
(   'cabd.modelled_crossings_view',
    'assessment_type_code',
    'atype_c',
    'Assessment Type Code',
    'The type of assessment carried out at the stream crossing.',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'assessment_type',
    'atype',
    'Assessment Type',
    'The type of assessment carried out at the stream crossing.',
    'varchar(32)',
    10
),
(   'cabd.modelled_crossings_view',
    'crossing_addressed',
    'cross_add',
    'Crossing Addressed',
    'If true, the stream crossing has been rehabilitated in some way to improve fish passage.',
    'boolean',
    11
),
(   'cabd.modelled_crossings_view',
    'crossing_confirmed',
    'cross_conf',
    'Crossing Confirmed',
    'If true, the existence of the stream crossing has been confirmed through a field visit.',
    'boolean',
    12
),

(   'cabd.modelled_crossings_view',
    'municipality',
    'muni_name',
    'Municipality',
    'The municipality the stream crossing is located in.',
    'varchar(512)',
    13
),
(   'cabd.modelled_crossings_view',
    'nhn_watershed_id',
    'nhnws_id',
    'NHN Watershed ID',
    'A code referencing the work unit ''Dataset Name'' from the National Hydrographic Network (NHN) that the feature is located in.',
    'varchar(7)',
    14
),
(   'cabd.modelled_crossings_view',
    'nhn_watershed_name',
    'nhnws_name',
    'NHN Watershed Name',
    'The name of the NHN sub-sub drainage area.',
    'varchar(500)',
    15
),
(   'cabd.modelled_crossings_view',
    'province_territory_code',
    'pr_terr_c',
    'Province/Territory Code',
    'The province or territory the stream crossing is located in.',
    'varchar(2)',
    NULL
),
(   'cabd.modelled_crossings_view',
    'province_territory',
    'pr_terr',
    'Province/Territory Name',
    'The province or territory the stream crossing is located in.',
    'varchar(32)',
    16
),
(   'cabd.modelled_crossings_view',
    'stream_name_1',
    'streamname',
    'Stream Name',
    'Name of river/stream in which the stream crossing is recorded.',
    'varchar(512)',
    17
),
(   'cabd.modelled_crossings_view',
    'stream_order',
    'stream_ord',
    'Stream Order',
    'Strahler stream order of the stream in which the stream crossing is recorded.',
    'integer',
    18
),
(   'cabd.modelled_crossings_view',
    'transport_feature_name',
    'tr_f_name',
    'Transport Feature Name',
    'Name of the road or other transportation infrastructure that uses the stream crossing.',
    'varchar',
    19
),
(   'cabd.modelled_crossings_view',
    'transport_feature_type',
    'tr_f_type',
    'Transport Feature Type',
    'Type of road or other transportation infrastructure that uses the stream crossing.',
    'varchar',
    20
),
(   'cabd.modelled_crossings_view',
    'transport_feature_owner',
    'tr_f_owner',
    'Transport Feature Owner',
    'Owner of the transportation infrastructure that uses the stream crossing.',
    'varchar',
    21
),
(   'cabd.modelled_crossings_view',
    'railway_operator',
    'rail_oper',
    'Railway Operator',
    'Operator of the railway that uses the stream crossing.',
    'varchar',
    22
),
(   'cabd.modelled_crossings_view',
    'railway_status',
    'rail_stat',
    'Railway Status',
    'Operating status of the railway that uses the stream crossing.',
    'varchar',
    23
),
(   'cabd.modelled_crossings_view',
    'roadway_type',
    'road_type',
    'Roadway Type',
    'Classification of the road, typically describing the type of service the road is intended to provide.',
    'varchar',
    24
),
(   'cabd.modelled_crossings_view',
    'comments',
    'comments',
    'Comments',
    'Unstructured comments about the stream crossing.',
    'varchar',
    25
),
(   'cabd.modelled_crossings_view',
    'use_analysis',
    'useanalysi',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes.',
    'boolean',
    26
),
(   'cabd.modelled_crossings_view',
    'geometry',
    'geometry',
    'Location',
    NULL,
    'geometry',
    NULL
);

UPDATE cabd.feature_type_metadata SET
    name_fr = name_en,
    description_fr = description_en,
    vw_all_order = vw_simple_order,
    vw_mobile_order = vw_simple_order
    WHERE view_name = 'cabd.modelled_crossings_view' AND name_fr IS NULL AND description_fr IS NULL AND vw_all_order IS NULL;

UPDATE cabd.feature_type_metadata SET value_options_reference = 'cabd.province_territory_codes;;name;'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'province_territory';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'cabd.province_territory_codes;code;name;'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'province_territory_code';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'cabd.passability_status_codes;;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'passability_status';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'cabd.passability_status_codes;code;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'passability_status_code';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.crossing_type_codes;;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'crossing_type';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.crossing_type_codes;code;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'crossing_type_code';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.crossing_subtype_codes;;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'crossing_subtype';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.crossing_subtype_codes;code;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'crossing_subtype_code';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.addressed_status_codes;;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'addressed_status';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.addressed_status_codes;code;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'addressed_status_code';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.assessment_type_codes;;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'assessment_type';

UPDATE cabd.feature_type_metadata SET value_options_reference = 'modelled_crossings.assessment_type_codes;code;name;description'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name = 'assessment_type_code';

UPDATE cabd.feature_type_metadata SET include_vector_tile = 'true'
WHERE view_name = 'cabd.modelled_crossings_view' AND field_name IN (
    'cabd_id',
    'province_territory',
    'province_territory_code',
    'passability_status',
    'passability_status_code'
    'use_analysis',
    'nhn_watershed_id',
    'nhn_watershed_name',
    'crossing_type',
    'crossing_type_code',
    'crossing_subtype',
    'crossing_subtype_code',
    'addressed_status',
    'addressed_status_code',
    'assessment_type',
    'assessment_type_code',
    'crossing_addressed',
    'crossing_confirmed'
);

TRUNCATE cabd.vector_tile_cache;
