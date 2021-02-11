delete from cabd.feature_type_metadata where view_name = 'cabd.all_features_view';

	 
INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link) VALUES
	 ('cabd.all_features_view','feature_type','Feature Type',NULL,false),
	 ('cabd.all_features_view','province_territory','Province/Territory Name',NULL,false),
	 ('cabd.all_features_view','province_territory_code','Province/Territory Code',NULL,false),
	 ('cabd.all_features_view','watershed_group_code','Watershed Group Code',NULL,false),
	 ('cabd.all_features_view','watershed_group_name','Watershed Group Name',NULL,false),
	 ('cabd.all_features_view','nearest_municipality','Nearest Municipality','Name of nearest municipality (i.e., a city or town that has corporate status and local government).',false),
	 ('cabd.all_features_view','waterbody_name_en','Waterbody Name (English)','English name of waterbody in which the barrier is recorded.',false),
	 ('cabd.all_features_view','waterbody_name_fr','Waterbody Name (French)','French name of waterbody in which the barrier is recorded.',false),
	 ('cabd.all_features_view','reservoir_name_en','Reservoir Name (English)','English name of reservoir in which the barrier is recorded.',false),
	 ('cabd.all_features_view','reservoir_name_fr','Reservoir Name (French)','French name of reservoir in which the barrier is recorded.',false),
	 ('cabd.all_features_view','point','Location',NULL,false),
	 ('cabd.all_features_view','name_en','Feature Name (English)','English given or known name of the feature',false),
	 ('cabd.all_features_view','name_fr','Feature Name (French)','French given or known name of the feature.',false),
	 ('cabd.all_features_view','cabd_id','Feature Identifier','Unique identifier for each feature.',false);
	 
	 
CREATE OR REPLACE VIEW cabd.all_features_view
AS SELECT barriers.cabd_id,
    barriers.barrier_type AS feature_type,
    barriers.name_en,
    barriers.name_fr,
    barriers.province_territory_code,
    pt.name AS province_territory,
    barriers.watershed_group_code,
    wg.name AS watershed_group_name,
    barriers.nearest_municipality,
    barriers.waterbody_name_en,
    barriers.waterbody_name_fr,
    barriers.reservoir_name_en,
    barriers.reservoir_name_fr,
    barriers.snapped_point
   FROM ( SELECT dams_medium_large.cabd_id,
            'dams_medium_large'::text AS barrier_type,
            dams_medium_large.dam_name_en AS name_en,
            dams_medium_large.dam_name_fr AS name_fr,
            dams_medium_large.province_territory_code,
            dams_medium_large.watershed_group_code,
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
            waterfalls.nearest_municipality,
            waterfalls.waterbody_name_en,
            waterfalls.waterbody_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            waterfalls.snapped_point
           FROM waterfalls.waterfalls
        UNION
         SELECT fishways.cabd_id,
            'fishways'::text AS barrier_type,
            NULL::varchar(512) AS name_en,
            NULL::varchar(512) AS name_fr,
            fishways.province_territory_code,
            fishways.watershed_group_code,
            fishways.nearest_municipality,
            fishways.river_name_en,
            fishways.river_name_fr,
            NULL::character varying AS "varchar",
            NULL::character varying AS "varchar",
            fishways.snapped_point
           FROM fishways.fishways
          ) barriers
     JOIN cabd.province_territory_codes pt ON barriers.province_territory_code::text = pt.code::text
     LEFT JOIN cabd.watershed_groups wg ON wg.code::text = barriers.watershed_group_code::text;