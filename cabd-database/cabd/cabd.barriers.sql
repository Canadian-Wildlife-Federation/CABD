delete from cabd.feature_types where type = 'barriers';
delete from cabd.feature_type_metadata where view_name = 'cabd.barriers_view';

INSERT INTO cabd.feature_types ("type",data_view, name) VALUES
	 ('barriers','cabd.barriers_view', 'All Barriers');
	 
INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link, data_type, vw_simple_order,vw_all_order) VALUES
	 ('cabd.barriers_view','cabd_id','Barrier Identifier','Unique identifier for each barrier.',false,'varchar(512)',null,1),
	 ('cabd.barriers_view','name_en','Barrier Name (English)','English given or known name of the barrier',false,'varchar(512)',1,2),
	 ('cabd.barriers_view','name_fr','Barrier Name (French)','French given or known name of the barrier.',false,'varchar(512)',null,3),
	 ('cabd.barriers_view','waterbody_name_en','Waterbody Name (English)','English name of waterbody in which the barrier is recorded.',false,'varchar(512)',3,4),
	 ('cabd.barriers_view','waterbody_name_fr','Waterbody Name (French)','French name of waterbody in which the barrier is recorded.',false,'varchar(512)',null,5),
	 ('cabd.barriers_view','reservoir_name_en','Reservoir Name (English)','English name of reservoir in which the barrier is recorded.',false,'varchar(512)',4,6),
	 ('cabd.barriers_view','reservoir_name_fr','Reservoir Name (French)','French name of reservoir in which the barrier is recorded.',false,'varchar(512)',null,7),
	 ('cabd.barriers_view','province_territory','Province/Territory Name',NULL,false,'varchar(32)',5,8),
	 ('cabd.barriers_view','province_territory_code','Province/Territory Code',NULL,false,'varchar(2)',null,9),
	 ('cabd.barriers_view','watershed_group_code','Watershed Group Code',NULL,false,'varchar(512)',null,10),
	 ('cabd.barriers_view','watershed_group_name','Watershed Group Name',NULL,false,'varchar(512)',null,11),
	 ('cabd.barriers_view','nhn_workunit_id','NHN Work Unit',NULL,false,'varchar(7)',null,12),
	 ('cabd.barriers_view','municipality','Nearest Municipality','Name of nearest municipality (i.e., a city or town that has corporate status and local government).',false,'varchar(512)',null,13),
	 ('cabd.barriers_view','barrier_type','Feature Type', NULL, false, 'text', null,14),
	 ('cabd.barriers_view','geometry','Location', NULL, false, 'geometry',null,null);

	 
	 
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
            waterfalls.snapped_point
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text;
     
grant all privileges on cabd.barriers_view to cabd;     