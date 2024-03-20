CREATE SCHEMA IF NOT EXISTS modelled_crossings;

-- CODED TABLES SETUP --

create table modelled_crossings.crossing_type_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.crossing_type_codes (code, name_en) values
(1, 'open-bottom structure'),
(2, 'closed-bottom structure'),
(3, 'ford-like structure'),
(99, 'unknown');

create table modelled_crossings.crossing_subtype_codes(
  code integer primary key,
  name_en varchar(2056),
  name_fr varchar(2056),
  description_en varchar (100000),
  description_fr varchar (100000)
);

insert into modelled_crossings.crossing_subtype_codes (code, name_en) values
(1, 'bridge'),
(2, 'culvert'),
(3, 'multiple culvert'),
(4, 'ford'),
(5, 'no crossing'),
(6, 'removed crossing'),
(7, 'buried stream'),
(8, 'inaccessible'),
(9, 'partially inaccessible'),
(10, 'no upstream channel'),
(11, 'bridge adequate'),
(99, 'unknown');

-- MAIN TABLE SETUP --

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
        ON DELETE NO ACTION
    ;

-- ENGLISH VIEW --

DROP VIEW cabd.modelled_crossings_view_en;

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
    m.crossing_subtype_source,
    m.passability_status_code,
    ps.name_en AS passability_status,
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
    m.snapped_point AS geometry
   FROM modelled_crossings.modelled_crossings m
     JOIN cabd.province_territory_codes pt ON m.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = m.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = m.passability_status_code
     LEFT JOIN modelled_crossings.crossing_type_codes ct ON ct.code = m.crossing_type_code
     LEFT JOIN modelled_crossings.crossing_subtype_codes cst ON cst.code = m.crossing_subtype_code;

ALTER TABLE cabd.modelled_crossings_view_en
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO PUBLIC;
GRANT ALL ON TABLE cabd.modelled_crossings_view_en TO cabd;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO cwf_user;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_en TO egouge;

-- FRENCH VIEW --

DROP VIEW cabd.modelled_crossings_view_fr;

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
    m.crossing_subtype_source,
    m.passability_status_code,
    ps.name_fr AS passability_status,
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
    m.snapped_point AS geometry
   FROM modelled_crossings.modelled_crossings m
     JOIN cabd.province_territory_codes pt ON m.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id::text = m.nhn_watershed_id::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = m.passability_status_code
     LEFT JOIN modelled_crossings.crossing_type_codes ct ON ct.code = m.crossing_type_code
     LEFT JOIN modelled_crossings.crossing_subtype_codes cst ON cst.code = m.crossing_subtype_code;

ALTER TABLE cabd.modelled_crossings_view_fr
    OWNER TO cabd;

GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO PUBLIC;
GRANT ALL ON TABLE cabd.modelled_crossings_view_fr TO cabd;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO cwf_user;
GRANT SELECT ON TABLE cabd.modelled_crossings_view_fr TO egouge;

-- ALL FEATURES VIEW - ENGLISH --

DROP VIEW cabd.all_features_view_en;

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
            c.passability_status_code AS passability_status_code,
            c.use_analysis,
            c.geometry
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

DROP VIEW cabd.all_features_view_fr;

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
            c.passability_status_code AS passability_status_code,
            c.use_analysis,
            c.geometry
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

DROP VIEW cabd.barriers_view_en;

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
            c.passability_status_code AS passability_status_code,
            c.use_analysis,
            c.geometry
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

DROP VIEW cabd.barriers_view_fr;

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
            c.passability_status_code AS passability_status_code,
            c.use_analysis,
            c.geometry
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
    data_table
)
VALUES
    ('modelled_crossings',
    'cabd.modelled_crossings_view',
    'Modelled Crossings',
    'Modelled Crossings',
    '1.0',
    'Modelled stream crossings based on locations where a stream is crossed by a road, railway, or trail.',
    '{modelled_crossings.modelled_crossings}')
;

-- FEATURE TYPE METADATA TABLE --

INSERT INTO cabd.feature_type_metadata (
    view_name,
    field_name,
    name_en,
    description_en,
    data_type,
    vw_simple_order
)
VALUES
(   'cabd.modelled_crossings_view',
    'cabd_id',
    'Barrier Identifier',
    'Unique identifier for the stream crossing',
    'uuid',
    1
),
(   'cabd.modelled_crossings_view',
    'feature_type',
    'Feature Type',
    'The type of feature the data point represents',
    'text',
    2
),
(   'cabd.modelled_crossings_view',
    'latitude',
    'Latitude',
    'Latitude of point location of stream crossing in decimal degrees; the point location is only an approximation of the actual stream crossing location.',
    'double precision',
    3
),
(   'cabd.modelled_crossings_view',
    'longitude',
    'Longitude',
    'Longitude of point location of stream crossing in decimal degrees; the point location is only an approximation of the actual stream crossing location.',
    'double precision',
    4
),
(   'cabd.modelled_crossings_view',
    'crossing_type_code',
    'Crossing Type Code',
    'The crossing type',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'crossing_type',
    'Crossing Type',
    'The crossing type',
    'varchar(32)',
    5
),
(   'cabd.modelled_crossings_view',
    'crossing_subtype_code',
    'Crossing Subtype Code',
    'The crossing subtype',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'crossing_subtype',
    'Crossing Subtype',
    'The crossing subtype',
    'varchar(32)',
    6
),
(   'cabd.modelled_crossings_view',
    'crossing_subtype_source',
    'Crossing Subtype Source',
    'The source of the crossing subtype',
    'varchar',
    7
),
(   'cabd.modelled_crossings_view',
    'passability_status_code',
    'Passability Status Code',
    'The degree to which the stream crossing acts as a barrier to fish in the upstream direction.',
    'integer',
    NULL
),
(   'cabd.modelled_crossings_view',
    'passability_status',
    'Passability Status',
    'The degree to which the stream crossing acts as a barrier to fish in the upstream direction.',
    'varchar(32)',
    8
),
(   'cabd.modelled_crossings_view',
    'municipality',
    'Municipality',
    'The municipality the stream crossing is located in.',
    'varchar(512)',
    9
),
(   'cabd.modelled_crossings_view',
    'nhn_watershed_id',
    'NHN Watershed ID',
    'A code referencing the work unit ''Dataset Name'' from the National Hydrographic Network (NHN) that the feature is located in.',
    'varchar(7)',
    10
),
(   'cabd.modelled_crossings_view',
    'nhn_watershed_name',
    'NHN Watershed Name',
    'The name of the NHN sub-sub drainage area.',
    'varchar(500)',
    11
),
(   'cabd.modelled_crossings_view',
    'province_territory_code',
    'Province/Territory Code',
    'The province or territory the stream crossing is located in.',
    'varchar(2)',
    NULL
),
(   'cabd.modelled_crossings_view',
    'province_territory',
    'Province/Territory Name',
    'The province or territory the stream crossing is located in.',
    'varchar(32)',
    12
),
(   'cabd.modelled_crossings_view',
    'stream_name_1',
    'Stream Name',
    'Name of river/stream in which the stream crossing is recorded.',
    'varchar(512)',
    13
),
(   'cabd.modelled_crossings_view',
    'stream_order',
    'Stream Order',
    'Strahler stream order of the stream in which the stream crossing is recorded.',
    'integer',
    14
),
(   'cabd.modelled_crossings_view',
    'transport_feature_name',
    'Transport Feature Name',
    'Name of the road or other transportation infrastructure that uses the stream crossing.',
    'varchar',
    15
),
(   'cabd.modelled_crossings_view',
    'transport_feature_type',
    'Transport Feature Type',
    'Type of road or other transportation infrastructure that uses the stream crossing.',
    'varchar',
    16
),
(   'cabd.modelled_crossings_view',
    'transport_feature_owner',
    'Transport Feature Owner',
    'Owner of the transportation infrastructure that uses the stream crossing.',
    'varchar',
    17
),
(   'cabd.modelled_crossings_view',
    'railway_operator',
    'Railway Operator',
    'Operator of the railway that uses the stream crossing.',
    'varchar',
    18
),
(   'cabd.modelled_crossings_view',
    'railway_status',
    'Railway Status',
    'Operating status of the railway that uses the stream crossing.',
    'varchar',
    19
),
(   'cabd.modelled_crossings_view',
    'roadway_type',
    'Roadway Type',
    'Classification of the road, typically describing the type of service the road is intended to provide.',
    'varchar',
    20
),
(   'cabd.modelled_crossings_view',
    'comments',
    'Comments',
    'Unstructured comments about the stream crossing.',
    'varchar',
    21
),
(   'cabd.modelled_crossings_view',
    'use_analysis',
    'Use for Network Analysis',
    'If true, the data point representing this feature is/should be snapped to hydrographic networks for analysis purposes.',
    'boolean',
    22
),
(   'cabd.modelled_crossings_view',
    'geometry',
    'Location',
    NULL,
    'geometry',
    NULL
);

UPDATE cabd.feature_type_metadata SET
    name_fr = name_en,
    description_fr = description_en,
    vw_all_order = vw_simple_order 
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
    'crossing_subtype_code'
);

-- ATTRIBUTE SETS --