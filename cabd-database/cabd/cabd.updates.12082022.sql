--set of queries to create staging tables to hold feature updates
--feature updates may come from CWF nonspatial work or user submitted updates from web tool
--cabd.user_feature_updates contains raw user submitted updates


--dams
CREATE TABLE cabd.dam_updates (LIKE dams.dams);
ALTER TABLE cabd.dam_updates ADD COLUMN latitude decimal(8,6);
ALTER TABLE cabd.dam_updates ADD COLUMN longitude decimal(9,6);
ALTER TABLE cabd.dam_updates ADD COLUMN entry_classification varchar;
ALTER TABLE cabd.dam_updates ADD COLUMN data_source_short_name varchar;
ALTER TABLE cabd.dam_updates ADD COLUMN update_status varchar;
ALTER TABLE cabd.dam_updates ADD COLUMN update_type varchar;

ALTER TABLE cabd.dam_updates ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'done'));
ALTER TABLE cabd.dam_updates ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE cabd.dam_updates ADD CONSTRAINT dam_record_unique UNIQUE (cabd_id, data_source_short_name);

ALTER TABLE cabd.dam_updates OWNER to cabd;


--fishways
CREATE TABLE cabd.fishway_updates (LIKE fishways.fishways);
ALTER TABLE cabd.fishway_updates ADD COLUMN latitude decimal(8,6);
ALTER TABLE cabd.fishway_updates ADD COLUMN longitude decimal(9,6);
ALTER TABLE cabd.fishway_updates ADD COLUMN entry_classification varchar;
ALTER TABLE cabd.fishway_updates ADD COLUMN data_source_short_name varchar;
ALTER TABLE cabd.fishway_updates ADD COLUMN update_status varchar;
ALTER TABLE cabd.fishway_updates ADD COLUMN update_type varchar;

ALTER TABLE cabd.fishway_updates ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'done'));
ALTER TABLE cabd.fishway_updates ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE cabd.fishway_updates ADD CONSTRAINT fish_record_unique UNIQUE (cabd_id, data_source_short_name);

ALTER TABLE cabd.fishway_updates OWNER to cabd;


--waterfalls
CREATE TABLE cabd.waterfall_updates (LIKE waterfalls.waterfalls);
ALTER TABLE cabd.waterfall_updates ADD COLUMN latitude decimal(8,6);
ALTER TABLE cabd.waterfall_updates ADD COLUMN longitude decimal(9,6);
ALTER TABLE cabd.waterfall_updates ADD COLUMN entry_classification varchar;
ALTER TABLE cabd.waterfall_updates ADD COLUMN data_source_short_name varchar;
ALTER TABLE cabd.waterfall_updates ADD COLUMN update_status varchar;
ALTER TABLE cabd.waterfall_updates ADD COLUMN update_type varchar;

ALTER TABLE cabd.waterfall_updates ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'done'));
ALTER TABLE cabd.waterfall_updates ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE cabd.waterfall_updates ADD CONSTRAINT fall_record_unique UNIQUE (cabd_id, data_source_short_name);

ALTER TABLE cabd.waterfall_updates OWNER to cabd;
