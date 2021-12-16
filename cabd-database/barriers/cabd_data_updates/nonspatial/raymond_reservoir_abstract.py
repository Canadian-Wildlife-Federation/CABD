import nonspatial as main

script = main.MappingScript("raymond_reservoir_abstract")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'Reservoir and canal system regulation for operation of the Raymond Reservoir Hydro Project',
now(), null, 'Miller, H.D; Davidson, B', 'Data update - ' || now());

--add data source to the table
UPDATE TABLE {script.workingTable} ADD COLUMN data_source varchar(512);
UPDATE TABLE {script.workingTable} SET data_source = {script.dsUuid};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.data_source ELSE cabdsource.dam_name_en_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.construction_year_ds END,
    generating_capacity_mwh_ds = CASE WHEN (cabd.generating_capacity_mwh IS NULL AND {script.datasetname}.generating_capacity_mwh IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    reservoir_name_en_ds = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.reservoir_name_en_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.use_electricity_code_ds END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN {script.datasetname}.dam_name_en IS NOT NULL THEN {script.datasetname}.dam_name_en ELSE cabd.dam_name_en END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND {script.datasetname}.construction_year IS NOT NULL) THEN {script.datasetname}.construction_year ELSE cabd.construction_year END,
    generating_capacity_mwh = CASE WHEN (cabd.generating_capacity_mwh IS NULL AND {script.datasetname}.generating_capacity_mwh IS NOT NULL) THEN {script.datasetname}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    reservoir_name_en = CASE WHEN (cabd.reservoir_name_en IS NULL AND {script.datasetname}.reservoir_name_en IS NOT NULL) THEN {script.datasetname}.reservoir_name_en ELSE cabd.reservoir_name_en END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND {script.datasetname}.use_electricity_code IS NOT NULL) THEN {script.datasetname}.use_electricity_code ELSE cabd.use_electricity_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)