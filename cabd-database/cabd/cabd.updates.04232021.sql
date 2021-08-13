--move passability status to cabd schema
alter table dams.passability_status_codes set schema cabd;

update cabd.passability_status_codes set name = 'Barrier' where code = 1;
update cabd.passability_status_codes set name = 'Partial Barrier' where code = 2;
update cabd.passability_status_codes set name = 'Passable' where code = 3;
insert into cabd.passability_status_codes (code, name, description) values (4, 'Unknown', 'Unknown');

--add passability status to waterfalls
alter table waterfalls.waterfalls add column passability_status_code int2 null;
COMMENT ON COLUMN waterfalls.waterfalls.passability_status_code IS 'Code referencing the degree to which the structure acts as a barrier to fish in the upstream direction';
ALTER TABLE waterfalls.waterfalls ADD CONSTRAINT waterfalls_fk_4 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes(code);

update cabd.feature_type_metadata set vw_all_order = vw_all_order + 2 where view_name = 'cabd.waterfalls_view' and vw_all_order >= 11; 
update cabd.feature_type_metadata set vw_simple_order = vw_simple_order + 1 where view_name = 'cabd.waterfalls_view' and vw_simple_order >= 7; 
insert into cabd.feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.waterfalls_view', 'passability_status_code', 'Passability Status Code','Code referencing the degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'integer', null, 11)
insert into cabd.feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.waterfalls_view', 'passability_status', 'Passability Status','The degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'varchar(32)', 7,  12)


DROP VIEW cabd.waterfalls_view;
CREATE VIEW cabd.waterfalls_view
AS SELECT w.cabd_id,
    'waterfalls'::text AS feature_type,
    st_y(w.snapped_point) as latitude,
    st_x(w.snapped_point) as longitude,
    w.fall_name_en,
    w.fall_name_fr,
    w.waterbody_name_en,
    w.waterbody_name_fr,
    w.watershed_group_code,
    wg.name AS watershed_group_name,
    w.nhn_workunit_id,
    w.province_territory_code,
    pt.name AS province_territory,
    w.municipality,
    w.fall_height_m,
    w.capture_date,
    w.last_update,
    w.comments,
    w.complete_level_code,
    cl.name AS complete_level,
    w.data_source_id,
    w.data_source,
    w.passability_status_code,
    ps.name as passability_status,
    w.snapped_point as geometry
   FROM waterfalls.waterfalls w
     JOIN cabd.province_territory_codes pt ON w.province_territory_code::text = pt.code::text
     LEFT JOIN waterfalls.waterfall_complete_level_codes cl ON cl.code = w.complete_level_code
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = w.watershed_group_code::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = w.passability_status_code;

     
grant all privileges on cabd.waterfalls_view to cabd;


--update barriers view and associated metadata
DROP VIEW cabd.barriers_view;
CREATE OR REPLACE VIEW cabd.barriers_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.watershed_group_code,
    wg.name AS watershed_group_name,
    barriers.nhn_workunit_id,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name as passability_status,
    barriers.snapped_point as geometry
   FROM ( SELECT dams_medium_large.cabd_id,
            'dams_medium_large'::text AS barrier_type,
            dams_medium_large.dam_name_en AS name_en,
            dams_medium_large.dam_name_fr AS name_fr,
            dams_medium_large.province_territory_code,
            dams_medium_large.watershed_group_code,
            dams_medium_large.nhn_workunit_id,
            dams_medium_large.municipality,
            dams_medium_large.waterbody_name_en,
            dams_medium_large.waterbody_name_fr,
            dams_medium_large.reservoir_name_en,
            dams_medium_large.reservoir_name_fr,
            dams_medium_large.passability_status_code,
            dams_medium_large.snapped_point
           FROM dams.dams_medium_large
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.watershed_group_code,
            waterfalls.nhn_workunit_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
grant all privileges on cabd.barriers_view to cabd;   

update cabd.feature_type_metadata set vw_all_order = vw_all_order + 2 where view_name = 'cabd.barriers_view' and vw_all_order >= 14; 
insert into cabd.feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.barriers_view', 'passability_status_code', 'Passability Status Code','Code referencing the degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'integer', null, 14);

insert into feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.barriers_view', 'passability_status', 'Passability Status','The degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'varchar(32)', 6,  15);


--update all features view and associated metadata
DROP VIEW cabd.all_features_view;

CREATE VIEW cabd.all_features_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.watershed_group_code,
    wg.name AS watershed_group_name,
    barriers.nhn_workunit_id,
    barriers.municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.passability_status_code,
    ps.name as passability_status,
    barriers.snapped_point as geometry
   FROM ( SELECT dams_medium_large.cabd_id,
            'dams_medium_large'::text AS barrier_type,
            dams_medium_large.dam_name_en AS name_en,
            dams_medium_large.dam_name_fr AS name_fr,
            dams_medium_large.province_territory_code,
            dams_medium_large.watershed_group_code,
            dams_medium_large.nhn_workunit_id,
            dams_medium_large.municipality,
            dams_medium_large.waterbody_name_en,
            dams_medium_large.waterbody_name_fr,
            dams_medium_large.reservoir_name_en,
            dams_medium_large.reservoir_name_fr,
            dams_medium_large.passability_status_code,
            dams_medium_large.snapped_point
           FROM dams.dams_medium_large
        UNION
         SELECT waterfalls.cabd_id,
            'waterfalls'::text AS barrier_type,
            waterfalls.fall_name_en AS name_en,
            waterfalls.fall_name_fr AS name_fr,
            waterfalls.province_territory_code,
            waterfalls.watershed_group_code,
            waterfalls.nhn_workunit_id,
            waterfalls.municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.passability_status_code,
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            NULL::varchar(512) AS name_en,
            NULL::varchar(512) AS name_fr,
            fishways.province_territory_code,
            fishways.watershed_group_code,
            fishways.nhn_workunit_id,
            fishways.municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            NULL,
            fishways.snapped_point
           FROM fishways.fishways
          ) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = barriers.passability_status_code;
     
grant all privileges on cabd.all_features_view to cabd;


update cabd.feature_type_metadata set vw_all_order = vw_all_order + 2 where view_name = 'cabd.all_features_view' and vw_all_order >= 14; 
insert into cabd.feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.all_features_view', 'passability_status_code', 'Passability Status Code','Code referencing the degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'integer', null, 14);

insert into cabd.feature_type_metadata(view_name, field_name, name, description, is_link, data_type, vw_simple_order,vw_all_order)
values ('cabd.all_features_view', 'passability_status', 'Passability Status','The degree to which the structure acts as a barrier to fish in the upstream direction.', false, 'varchar(32)', 5,  15);


