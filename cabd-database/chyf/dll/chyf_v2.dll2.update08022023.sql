alter table chyf2.eflowpath rename column name_id to rivernameid1;
alter table chyf2.eflowpath add column rivernameid2 uuid;
alter table chyf2.eflowpath drop constraint eflowpath_name_id_fkey;
alter table chyf2.eflowpath add CONSTRAINT eflowpath_rivernameid1_fkey FOREIGN KEY (rivernameid1) REFERENCES chyf2.names(name_id);
alter table chyf2.eflowpath add CONSTRAINT eflowpath_rivernameid2_fkey FOREIGN KEY (rivernameid2) REFERENCES chyf2.names(name_id);

alter table chyf2.ecatchment rename column name_id to lakenameid1;
alter table chyf2.ecatchment add column lakenameid2 uuid;
alter table chyf2.ecatchment add column rivernameid1 uuid;
alter table chyf2.ecatchment add column rivernameid2 uuid;
alter table chyf2.ecatchment drop constraint ecatchment_name_id_fkey;
alter table chyf2.ecatchment add CONSTRAINT ecatchment_lakenameid1_fkey FOREIGN KEY (lakenameid1) REFERENCES chyf2.names(name_id);
alter table chyf2.ecatchment add CONSTRAINT ecatchment_lakenameid2_fkey FOREIGN KEY (lakenameid2) REFERENCES chyf2.names(name_id);
alter table chyf2.ecatchment add CONSTRAINT ecatchment_rivernameid1_fkey FOREIGN KEY (rivernameid1) REFERENCES chyf2.names(name_id);
alter table chyf2.ecatchment add CONSTRAINT ecatchment_rivernameid2_fkey FOREIGN KEY (rivernameid2) REFERENCES chyf2.names(name_id);


alter table chyf2.names add column geodbname varchar;
update chyf2.names set geodbname = 'CGNDB';
alter table chyf2.names rename column cgndb_id to geodb_id;
alter table chyf2.names add column geodb_id2 varchar;
update chyf2.names set geodb_id2 = geodb_id::varchar;
alter table chyf2.names drop column geodb_id;
alter table chyf2.names rename column geodb_id2 to geodb_id;
alter table chyf2.names add constraint names_geodb_id_unq unique(geodb_id);

alter table chyf2.terminal_point add column rivernameid1 uuid;
alter table chyf2.terminal_point add column rivernameid2 uuid;
alter table chyf2.terminal_point add constraint terminal_point_rivernameid1_fkey foreign key (rivernameid1) references chyf2.names(name_id);
alter table chyf2.terminal_point add constraint terminal_point_rivernameid2_fkey foreign key (rivernameid2) references chyf2.names(name_id);

alter table fpoutput.eflowpath rename column name_id to rivernameid1;
alter table fpoutput.eflowpath rename column name to rivername1;
alter table fpoutput.eflowpath add column rivernameid2 varchar(32);
alter table fpoutput.eflowpath add column rivername2 varchar;

alter table fpoutput.ecatchment rename column name_id to lakenameid1;
alter table fpoutput.ecatchment rename column name to lakename1;
alter table fpoutput.ecatchment add column lakenameid2 varchar(32);
alter table fpoutput.ecatchment add column lakename2 varchar;
alter table fpoutput.ecatchment add column rivernameid1 varchar(32);
alter table fpoutput.ecatchment add column rivername1 varchar;
alter table fpoutput.ecatchment add column rivernameid2 varchar(32);
alter table fpoutput.ecatchment add column rivername2 varchar;

alter table fpoutput.terminal_node add column rivernameid1 varchar(32);
alter table fpoutput.terminal_node add column rivername1 varchar;
alter table fpoutput.terminal_node add column rivernameid2 varchar(32);
alter table fpoutput.terminal_node add column rivername2 varchar;
alter table fpoutput.terminal_node add column geodbname varchar;

alter table fpoutput.construction_points add column rivernameids varchar;

alter table fpinput.eflowpath rename column name_id to rivernameid1;
alter table fpinput.eflowpath rename column name to rivername1;
alter table fpinput.eflowpath add column rivernameid2 varchar(32);
alter table fpinput.eflowpath add column rivername2 varchar;

alter table fpinput.ecatchment rename column name_id to lakenameid1;
alter table fpinput.ecatchment rename column name to lakename1;
alter table fpinput.ecatchment add column lakenameid2 varchar(32);
alter table fpinput.ecatchment add column lakename2 varchar;
alter table fpinput.ecatchment add column rivernameid1 varchar(32);
alter table fpinput.ecatchment add column rivername1 varchar;
alter table fpinput.ecatchment add column rivernameid2 varchar(32);
alter table fpinput.ecatchment add column rivername2 varchar;

alter table fpinput.terminal_node add column rivernameid1 varchar(32);
alter table fpinput.terminal_node add column rivername1 varchar;
alter table fpinput.terminal_node add column rivernameid2 varchar(32);
alter table fpinput.terminal_node add column rivername2 varchar;
alter table fpinput.terminal_node add column geodbname varchar;


SELECT UpdateGeometrySRID('fpoutput','construction_points','the_geom',4617);
alter table fpoutput.errors alter column geometry type geometry(geometry, 4617);

alter table fpoutput.errors add column process varchar;
update fpoutput.errors set process = 'FLOWPATHFULL';


