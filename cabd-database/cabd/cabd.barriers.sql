delete from cabd.feature_types where type = 'barriers';
delete from cabd.feature_type_metadata where view_name = 'cabd.barriers_view';

INSERT INTO cabd.feature_types ("type",data_view) VALUES
	 ('barriers','cabd.barriers_view');
	 
INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link) VALUES
	 ('cabd.barriers_view','geometry','Location',NULL,false),
	 ('cabd.barriers_view','province_territory','Province/Territory Name',NULL,false),
	 ('cabd.barriers_view','province_territory_code','Province/Territory Code',NULL,false),
	 ('cabd.barriers_view','watershed_group_code','Watershed Group Code',NULL,false),
	 ('cabd.barriers_view','watershed_group_name','Watershed Group Name',NULL,false),
	 ('cabd.barriers_view','nhn_workunit_id','NHN Work Unit',NULL,false),
	 ('cabd.barriers_view','nearest_municipality','Nearest Municipality','Name of nearest municipality (i.e., a city or town that has corporate status and local government).',false),
	 ('cabd.barriers_view','waterbody_name_en','Waterbody Name (English)','English name of waterbody in which the barrier is recorded.',false),
	 ('cabd.barriers_view','waterbody_name_fr','Waterbody Name (French)','French name of waterbody in which the barrier is recorded.',false),
	 ('cabd.barriers_view','reservoir_name_en','Reservoir Name (English)','English name of reservoir in which the barrier is recorded.',false),
	 ('cabd.barriers_view','reservoir_name_fr','Reservoir Name (French)','French name of reservoir in which the barrier is recorded.',false),
	 ('cabd.barriers_view','cabd_id','Barrier Identifier','Unique identifier for each barrier.',false),
	 ('cabd.barriers_view','name_en','Barrier Name (English)','English given or known name of the barrier',false),
	 ('cabd.barriers_view','name_fr','Barrier Name (French)','French given or known name of the barrier.',false),
	 ('cabd.barriers_view','barrier_type','Feature Type',NULL,false);

	 
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
    barriers.nearest_municipality,
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
            dams_medium_large.nearest_municipality,
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
            waterfalls.nearest_municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.snapped_point
           FROM waterfalls.waterfalls) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text;
     
grant all privileges on cabd.barriers_view to cabd;     