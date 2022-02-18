delete from cabd.feature_type_metadata where view_name = 'cabd.medium_view';
delete from cabd.feature_types where type = 'medium';

drop schema if exists medium cascade;
GRANT ALL PRIVILEGES ON schema medium to cabd;

insert into cabd.feature_types (type, data_view, name, attribute_source_table)
values ('medium', 'cabd.medium_view', 'medium Features', null);

create schema medium;

CREATE TABLE medium.code_attribute_1 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_1 to cabd;

INSERT INTO medium.code_attribute_1 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null);

	 
CREATE TABLE medium.code_attribute_2 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_2 to cabd;

INSERT INTO medium.code_attribute_2 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null),
	 (6,'Test Value 6',null),
	 (7,'Test Value 7',null),
	 (8,'Test Value 8',null);
	 
CREATE TABLE medium.code_attribute_3 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_3 to cabd;
INSERT INTO medium.code_attribute_3 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null);
	 
CREATE TABLE medium.code_attribute_4 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_4 to cabd;
INSERT INTO medium.code_attribute_4 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null);	 
	 
CREATE TABLE medium.code_attribute_5 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_5 to cabd;
INSERT INTO medium.code_attribute_5 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null),
	 (6,'Test Value 6',null),
	 (7,'Test Value 7',null);
	 
CREATE TABLE medium.code_attribute_6 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_6 to cabd;
INSERT INTO medium.code_attribute_6 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null);	 	 

	 
CREATE TABLE medium.code_attribute_7 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_7 to cabd;
INSERT INTO medium.code_attribute_7 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null);	 	 
	 

CREATE TABLE medium.code_attribute_8 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_8 to cabd;
INSERT INTO medium.code_attribute_8 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null),
	 (6,'Test Value 6',null);
	 
	 
CREATE TABLE medium.code_attribute_9 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_9 to cabd;
INSERT INTO medium.code_attribute_9 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null),
	 (4,'Test Value 4',null),
	 (5,'Test Value 5',null);	 	 
	 
	 
CREATE TABLE medium.code_attribute_10 (
	code int2 NOT NULL, 
	"name" varchar(32) NOT NULL, 
	description text NULL,
	PRIMARY KEY (code)
);
GRANT ALL PRIVILEGES ON medium.code_attribute_10 to cabd;
INSERT INTO medium.code_attribute_10 (code,"name",description) VALUES
	 (1,'Test Value 1',null),
	 (2,'Test Value 2',null),
	 (3,'Test Value 3',null);	 	 

CREATE TABLE medium.medium (
	cabd_id uuid NOT NULL, 
	name_en varchar(512) NULL, 
	name_fr varchar(512) NULL, 
	
	nhn_workunit_id varchar(7) NULL,
	province_territory_code varchar(2) NOT NULL,
		
	passability_status_code int2 NULL, 
	up_passage_type_code int2 NULL,
	
	code_attribute_1 int2 NULL,
	code_attribute_2 int2 NULL,
	code_attribute_3 int2 NULL,
	code_attribute_4 int2 NULL,
	code_attribute_5 int2 NULL,
	code_attribute_6 int2 NULL,
	code_attribute_7 int2 NULL,
	code_attribute_8 int2 NULL,
	code_attribute_9 int2 NULL,
	code_attribute_10 int2 NULL,
	
	number_attribute_1 float8,
	number_attribute_2 float8,
	number_attribute_3 float8,
	number_attribute_4 float8,
	number_attribute_5 float8,
	number_attribute_6 float8,
	number_attribute_7 float8,
	number_attribute_8 float8,
	number_attribute_9 float8,
	number_attribute_10 float8,
	
	text_attribute_1 varchar(32),
	text_attribute_2 varchar(32),
	text_attribute_3 varchar(32),
	text_attribute_4 varchar(32),
	text_attribute_5 varchar(32),
	text_attribute_6 varchar(32),
	text_attribute_7 varchar(32),
	text_attribute_8 varchar(32),
	text_attribute_9 varchar(32),
	text_attribute_10 varchar(32),

	date_attribute_1 date,
	date_attribute_2 date,
	date_attribute_3 date,
	date_attribute_4 date,
	date_attribute_5 date,
	date_attribute_6 date,
	date_attribute_7 date,
	date_attribute_8 date,
	date_attribute_9 date,
	date_attribute_10 date,


	original_point geometry(POINT, 4617) NULL,
	snapped_point geometry(POINT, 4617) NULL,
	CONSTRAINT dams_medium_large_pk PRIMARY KEY (cabd_id)
);
CREATE INDEX medium_point_idx ON medium.medium USING gist (snapped_point);


ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_2 FOREIGN KEY (nhn_workunit_id) REFERENCES cabd.nhn_workunit(id);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_3 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_4 FOREIGN KEY (up_passage_type_code) REFERENCES cabd.upstream_passage_type_codes(code);

ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_5 FOREIGN KEY (code_attribute_1) REFERENCES medium.code_attribute_1(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_6 FOREIGN KEY (code_attribute_2) REFERENCES medium.code_attribute_2(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_7 FOREIGN KEY (code_attribute_3) REFERENCES medium.code_attribute_3(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_8 FOREIGN KEY (code_attribute_4) REFERENCES medium.code_attribute_4(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_9 FOREIGN KEY (code_attribute_5) REFERENCES medium.code_attribute_5(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_10 FOREIGN KEY (code_attribute_6) REFERENCES medium.code_attribute_6(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_11 FOREIGN KEY (code_attribute_7) REFERENCES medium.code_attribute_7(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_12 FOREIGN KEY (code_attribute_8) REFERENCES medium.code_attribute_8(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_13 FOREIGN KEY (code_attribute_9) REFERENCES medium.code_attribute_9(code);
ALTER TABLE medium.medium ADD CONSTRAINT medium_largest_fk_14 FOREIGN KEY (code_attribute_10) REFERENCES medium.code_attribute_10(code);
GRANT ALL PRIVILEGES ON medium.medium to cabd;


CREATE OR REPLACE VIEW cabd.medium_view
AS SELECT 
    'medium'::text AS feature_type,
    d.cabd_id,
    st_y(d.snapped_point) as latitude,
    st_x(d.snapped_point) as longitude,

    d.name_en,
    d.name_fr,
   
    d.nhn_workunit_id,
    nhn.sub_sub_drainage_area AS sub_sub_drainage_area,

    d.province_territory_code,
    pt.name AS province_territory,
    
    d.passability_status_code,
    ps.name as passability_status,
    
    d.up_passage_type_code,
    up.name AS up_passage_type,
   
    d.code_attribute_1,
    d1.name as code_attribute_1_name,
    d.code_attribute_2,
    d2.name as code_attribute_2_name,
    d.code_attribute_3,
    d3.name as code_attribute_3_name,
    d.code_attribute_4,
    d4.name as code_attribute_4_name,
    d.code_attribute_5,
    d5.name as code_attribute_5_name,
    d.code_attribute_6,
    d6.name as code_attribute_6_name,
    d.code_attribute_7,
    d7.name as code_attribute_7_name,
    d.code_attribute_8,
    d8.name as code_attribute_8_name,
    d.code_attribute_9,
    d9.name as code_attribute_9_name,
    d.code_attribute_10,
    d10.name as code_attribute_10_name,
    
    d.number_attribute_1,
    d.number_attribute_2,
    d.number_attribute_3,
    d.number_attribute_4,
    d.number_attribute_5,
    d.number_attribute_6,
    d.number_attribute_7,
    d.number_attribute_8,
    d.number_attribute_9,
    d.number_attribute_10,
    
    d.text_attribute_1,
    d.text_attribute_2,
    d.text_attribute_3,
    d.text_attribute_4,
    d.text_attribute_5,
    d.text_attribute_6,
    d.text_attribute_7,
    d.text_attribute_8,
    d.text_attribute_9,
    d.text_attribute_10,
    
    d.date_attribute_1,
    d.date_attribute_2,
    d.date_attribute_3,
    d.date_attribute_4,
    d.date_attribute_5,
    d.date_attribute_6,
    d.date_attribute_7,
    d.date_attribute_8,
    d.date_attribute_9,
    d.date_attribute_10,
        
    d.snapped_point AS geometry
   FROM medium.medium d
     JOIN cabd.province_territory_codes pt ON pt.code::text = d.province_territory_code::text
     LEFT JOIN cabd.upstream_passage_type_codes up ON up.code = d.up_passage_type_code
     LEFT JOIN cabd.nhn_workunit nhn ON nhn.id = d.nhn_workunit_id
     LEFT JOIN cabd.passability_status_codes ps ON ps.code = d.passability_status_code
     LEFT JOIN medium.code_attribute_1 d1 ON d1.code = d.code_attribute_1
     LEFT JOIN medium.code_attribute_2 d2 ON d2.code = d.code_attribute_2
     LEFT JOIN medium.code_attribute_3 d3 ON d3.code = d.code_attribute_3
     LEFT JOIN medium.code_attribute_4 d4 ON d4.code = d.code_attribute_4
     LEFT JOIN medium.code_attribute_5 d5 ON d5.code = d.code_attribute_5
     LEFT JOIN medium.code_attribute_6 d6 ON d6.code = d.code_attribute_6
     LEFT JOIN medium.code_attribute_7 d7 ON d7.code = d.code_attribute_7
     LEFT JOIN medium.code_attribute_8 d8 ON d8.code = d.code_attribute_8
     LEFT JOIN medium.code_attribute_9 d9 ON d9.code = d.code_attribute_9
     LEFT JOIN medium.code_attribute_10 d10 ON d10.code = d.code_attribute_10;
     
INSERT INTO cabd.feature_type_metadata (view_name,field_name,"name",description,is_link, data_type, vw_simple_order, vw_all_order,include_vector_tile, value_options_reference) VALUES
	 ('cabd.medium_view','cabd_id','Barrier Identifier','Unique Identifier for feature', false, 'uuid', null, 1, true, null),
	 ('cabd.medium_view','latitude','Latitude','', false, 'double', null, 2, false, null),
	 ('cabd.medium_view','longitude','Longitude','', false, 'double', null, 3, false, null),
	 ('cabd.medium_view','feature_type','Feature Type','', false, 'text', 1, 4, true, null),
	 ('cabd.medium_view','name_en','Name EN','', false, 'text', 2, 5, true, null),
	 ('cabd.medium_view','name_fr','Name FR','', false, 'text', 3, 6, true, null),
	 ('cabd.medium_view','nhn_workunit_id','NHN Work Unit','', false, 'varchar(7)', 4, 7, true, 'cabd.nhn_workunit;id;sub_sub_drainage_area;'),
	 ('cabd.medium_view','sub_sub_drainage_area','Sub sub drainage area','', false, 'varchar(500)', 5, 8, false, null),
	 
	 ('cabd.medium_view','province_territory_code','province_territory_code','', false, 'text', null, 9, true, 'cabd.province_territory_codes;code;name;'),
	 ('cabd.medium_view','province_territory','province territory','', false, 'text', 6, 10, true, 'cabd.province_territory_codes;name;name;'),
	 ('cabd.medium_view','passability_status_code','passability_status_code','', false, 'text', null, 11, true, 'cabd.passability_status_codes;code;name;description'),
	 ('cabd.medium_view','passability_status','passability status','', false, 'text', 7, 12, true, 'cabd.passability_status_codes;name;name;description'),
	 ('cabd.medium_view','up_passage_type_code','up_passage_type_code','', false, 'text', null, 13, true, 'cabd.upstream_passage_type_codes;code;name;description'),
	 ('cabd.medium_view','up_passage_type','up passage type','', false, 'text', 8, 14, true, 'cabd.upstream_passage_type_codes;name;name;description'),
	 
	 ('cabd.medium_view','code_attribute_1','code attribute 1','', false, 'integer', null, 15, true, 'medium.code_attribute_1;code;name;description'),
	 ('cabd.medium_view','code_attribute_1_name','code attribute 1 name','', false, 'varchar(32)', null, 16, true, 'medium.code_attribute_1;name;name;description'),
	 ('cabd.medium_view','code_attribute_2','code attribute 2','', false, 'integer', null, 17, false, 'medium.code_attribute_2;code;name;description'),
	 ('cabd.medium_view','code_attribute_2_name','code attribute 2 name','', false, 'varchar(32)', null, 18, false, 'medium.code_attribute_2;name;name;description'),
	 ('cabd.medium_view','code_attribute_3','code attribute 3','', false, 'integer', null, 19, false, 'medium.code_attribute_3;code;name;description'),
	 ('cabd.medium_view','code_attribute_3_name','code attribute 3 name','', false, 'varchar(32)', null, 20, false, 'medium.code_attribute_3;name;name;description'),
	 ('cabd.medium_view','code_attribute_4','code attribute 4','', false, 'integer', null, 21, false, 'medium.code_attribute_4;code;name;description'),
	 ('cabd.medium_view','code_attribute_4_name','code attribute 4 name','', false, 'varchar(32)', null, 22, false, 'medium.code_attribute_4;name;name;description'),
	 ('cabd.medium_view','code_attribute_5','code attribute 5','', false, 'integer', null, 23, false, 'medium.code_attribute_5;code;name;description'),
	 ('cabd.medium_view','code_attribute_5_name','code attribute 5 name','', false, 'varchar(32)', null, 24, false, 'medium.code_attribute_5;name;name;description'),
	 ('cabd.medium_view','code_attribute_6','code attribute 6','', false, 'integer', null, 25, false, 'medium.code_attribute_6;code;name;description'),
	 ('cabd.medium_view','code_attribute_6_name','code attribute 6 name','', false, 'varchar(32)', null, 26, false, 'medium.code_attribute_6;name;name;description'),
	 ('cabd.medium_view','code_attribute_7','code attribute 7','', false, 'integer', null, 27, false, 'medium.code_attribute_7;code;name;description'),
	 ('cabd.medium_view','code_attribute_7_name','code attribute 7 name','', false, 'varchar(32)', null, 28, false, 'medium.code_attribute_7;name;name;description'),
	 ('cabd.medium_view','code_attribute_8','code attribute 8','', false, 'integer', null, 29, false, 'medium.code_attribute_8;code;name;description'),
	 ('cabd.medium_view','code_attribute_8_name','code attribute 8 name','', false, 'varchar(32)', null, 30, false, 'medium.code_attribute_8;name;name;description'),
	 ('cabd.medium_view','code_attribute_9','code attribute 9','', false, 'integer', null, 31, false, 'medium.code_attribute_9;code;name;description'),
	 ('cabd.medium_view','code_attribute_9_name','code attribute 9 name','', false, 'varchar(32)', null, 32, false, 'medium.code_attribute_9;name;name;description'),
	 ('cabd.medium_view','code_attribute_10','code attribute 10','', false, 'integer', null, 33, false, 'medium.code_attribute_10;code;name;description'),
	 ('cabd.medium_view','code_attribute_10_name','code attribute 10 name','', false, 'varchar(32)', null, 34, false, 'medium.code_attribute_10;name;name;description'),
	 
	 ('cabd.medium_view','text_attribute_1','text attribute 1','', false, 'text', null, 35, true, null),
     ('cabd.medium_view','text_attribute_2','text attribute 2','', false, 'text', null, 36, true, null),
     ('cabd.medium_view','text_attribute_3','text attribute 3','', false, 'text', null, 37, false, null),
     ('cabd.medium_view','text_attribute_4','text attribute 4','', false, 'text', null, 38, false, null),
     ('cabd.medium_view','text_attribute_5','text attribute 5','', false, 'text', null, 39, false, null),
     ('cabd.medium_view','text_attribute_6','text attribute 6','', false, 'text', null, 40, false, null),
     ('cabd.medium_view','text_attribute_7','text attribute 7','', false, 'text', null, 41, false, null),
     ('cabd.medium_view','text_attribute_8','text attribute 8','', false, 'text', null, 42, false, null),
     ('cabd.medium_view','text_attribute_9','text attribute 9','', false, 'text', null, 43, false, null),
     ('cabd.medium_view','text_attribute_10','text attribute 10','', false, 'text', null, 44, false, null),
     
     ('cabd.medium_view','number_attribute_1','number attribute 1','', false, 'double', null, 45, true, null),
     ('cabd.medium_view','number_attribute_2','number attribute 2','', false, 'double', null, 46, true, null),
     ('cabd.medium_view','number_attribute_3','number attribute 3','', false, 'double', null, 47, false, null),
     ('cabd.medium_view','number_attribute_4','number attribute 4','', false, 'double', null, 48, false, null),
     ('cabd.medium_view','number_attribute_5','number attribute 5','', false, 'double', null, 49, false, null),
     ('cabd.medium_view','number_attribute_6','number attribute 6','', false, 'double', null, 50, false, null),
     ('cabd.medium_view','number_attribute_7','number attribute 7','', false, 'double', null, 51, false, null),
     ('cabd.medium_view','number_attribute_8','number attribute 8','', false, 'double', null, 52, false, null),
     ('cabd.medium_view','number_attribute_9','number attribute 9','', false, 'double', null, 53, false, null),
     ('cabd.medium_view','number_attribute_10','number attribute 10','', false, 'double', null, 54, false, null),
	 
     ('cabd.medium_view','date_attribute_1','date attribute 1','', false, 'date', null, 55, true, null),
     ('cabd.medium_view','date_attribute_2','date attribute 2','', false, 'date', null, 56, true, null),
     ('cabd.medium_view','date_attribute_3','date attribute 3','', false, 'date', null, 57, false, null),
     ('cabd.medium_view','date_attribute_4','date attribute 4','', false, 'date', null, 58, false, null),
     ('cabd.medium_view','date_attribute_5','date attribute 5','', false, 'date', null, 59, false, null),
     ('cabd.medium_view','date_attribute_6','date attribute 6','', false, 'date', null, 60, false, null),
     ('cabd.medium_view','date_attribute_7','date attribute 7','', false, 'date', null, 61, false, null),
     ('cabd.medium_view','date_attribute_8','date attribute 8','', false, 'date', null, 62, false, null),
     ('cabd.medium_view','date_attribute_9','date attribute 9','', false, 'date', null, 63, false, null),
     ('cabd.medium_view','date_attribute_10','date attribute 10','', false, 'date', null, 64, false, null),
	 
     ('cabd.medium_view','geometry','Location','', false, 'geometry', null, null, false, null);
     
GRANT ALL PRIVILEGES ON cabd.medium_view to cabd;


with geometries as
(
select foo.id as nhnid, b.code as provcode, foo.geom as geom
from 
(
   select id, (st_dump(st_generatepoints(polygon, 42))).geom as geom
   from cabd.nhn_workunit n 
   ) foo, cabd.province_territory_codes b
where st_contains(b.geometry, foo.geom)
)
insert into medium.medium(
 cabd_id, name_en, name_fr,  nhn_workunit_id, province_territory_code,
 passability_status_code, up_passage_type_code,	
 code_attribute_1, code_attribute_2, code_attribute_3, code_attribute_4, code_attribute_5,
 code_attribute_6, code_attribute_7, code_attribute_8, code_attribute_9, code_attribute_10,
 number_attribute_1, number_attribute_2, number_attribute_3, number_attribute_4, 
 number_attribute_5, number_attribute_6, number_attribute_7, number_attribute_8, 
 number_attribute_9, number_attribute_10,
 text_attribute_1, text_attribute_2, text_attribute_3, text_attribute_4, text_attribute_5, 
 text_attribute_6, text_attribute_7, text_attribute_8, text_attribute_9, text_attribute_10,
 date_attribute_1, date_attribute_2, date_attribute_3, date_attribute_4, date_attribute_5, 
 date_attribute_6, date_attribute_7, date_attribute_8, date_attribute_9, date_attribute_10, 
 original_point, snapped_point)
 select
 uuid_generate_v4(), 'feature name en', 'feature name fr', geometries.nhnid, geometries.provcode,
 floor(random() * 4 + 1)::int, floor(random() * 0 + 1)::int, 
 floor(random() * 5 + 1)::int, floor(random() * 8 + 1)::int, floor(random() * 5 + 1)::int, 
 floor(random() * 4 + 1)::int, floor(random() * 7 + 1)::int, floor(random() * 4 + 1)::int, 
 floor(random() * 5 + 1)::int, floor(random() * 6 + 1)::int, floor(random() * 5 + 1)::int, 
 floor(random() * 3 + 1)::int, 
 random() * 100, random() * 100, random() * 100, random() * 100, 
 random() * 100, random() * 100, random() * 100, random() * 100, 
 random() * 100, random() * 100, 
 'one', 'two', 'three', 'four',
 'five', 'six', 'seven', 'eight', 
 'nine', 'ten',
 now()::date - floor(random() * 1000)::int,now()::date - floor(random() * 1000)::int,
 now()::date - floor(random() * 1000)::int,now()::date - floor(random() * 1000)::int,
 now()::date - floor(random() * 1000)::int,now()::date - floor(random() * 1000)::int,
 now()::date - floor(random() * 1000)::int,now()::date - floor(random() * 1000)::int,
 now()::date - floor(random() * 1000)::int,now()::date - floor(random() * 1000)::int,
 geometries.geom, geometries.geom
 from geometries;