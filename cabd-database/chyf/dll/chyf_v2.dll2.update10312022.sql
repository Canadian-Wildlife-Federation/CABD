alter table fpinput.aoi add column processing_start_datetime timestamp;
alter table fpinput.aoi add column processing_end_datetime timestamp;
alter table fpoutput.aoi add column processing_start_datetime timestamp;
alter table fpoutput.aoi add column processing_end_datetime timestamp;

alter table fpinput.aoi add column processing_parameters varchar;
alter table fpoutput.aoi add column processing_parameters varchar;
