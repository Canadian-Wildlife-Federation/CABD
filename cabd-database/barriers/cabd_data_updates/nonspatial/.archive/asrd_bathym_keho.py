import nonspatial as main

script = main.MappingScript("keho_bathymetry")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Keho Lake Bathymetry', now(), null, 'Alberta Environment and Parks', 'Data update - ' || now());

--add data source to the table
ALTER TABLE {script.workingTable} ADD COLUMN data_source uuid;
UPDATE {script.workingTable} SET data_source = '{script.dsUuid}';

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.waterbody_name_en_ds END,
    reservoir_area_skm_ds = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN {script.datasetname}.reservoir_depth_m IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    lake_control_code_ds = CASE WHEN (cabd.lake_control_code IS NULL AND {script.datasetname}.lake_control_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.lake_control_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND {script.datasetname}.waterbody_name_en IS NOT NULL) THEN {script.datasetname}.waterbody_name_en ELSE cabd.waterbody_name_en END,
    reservoir_area_skm = CASE WHEN {script.datasetname}.reservoir_area_skm IS NOT NULL THEN {script.datasetname}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN {script.datasetname}.reservoir_depth_m IS NOT NULL THEN {script.datasetname}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    lake_control_code = CASE WHEN (cabd.lake_control_code IS NULL AND {script.datasetname}.lake_control_code IS NOT NULL) THEN {script.datasetname}.lake_control_code ELSE cabd.lake_control_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
