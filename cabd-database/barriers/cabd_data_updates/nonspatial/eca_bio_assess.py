import nonspatial as main

script = main.MappingScript("eca_bio_assess")

mappingquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, source, comments, source_type)
VALUES(
    '{script.dsUuid}',
    '{script.datasetName}',
    '2014-04-13',
    'East Coast Aquatics Inc., 2014. Biological Assessment for Proposed Liverpool Wind Farm. pp. 17; Table 3. Accessed from https://novascotia.ca/nse/ea/liverpool-wind-farm/Appendix_H-K.pdf',
    'Accessed February 22, 2022',
    'non-spatial');

--add data source to the table
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} SET data_source = '{script.dsUuid}';

--update new features
UPDATE
   {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    reservoir_present_ds = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
    reservoir_depth_m_ds = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    reservoir_area_skm_ds = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_area_skm_ds END,

    dam_name_en_dsfid = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN NULL ELSE cabdsource.dam_name_en_dsfid END,
    reservoir_present_dsfid = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN NULL ELSE cabdsource.reservoir_present_dsfid END,
    reservoir_depth_m_dsfid = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN NULL ELSE cabdsource.reservoir_depth_m_dsfid END,
    reservoir_area_skm_dsfid = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN NULL ELSE cabdsource.reservoir_area_skm_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id AND cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    reservoir_present = CASE WHEN ({script.datasetName}.reservoir_present IS NOT NULL AND {script.datasetName}.reservoir_present IS DISTINCT FROM cabd.reservoir_present) THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END,
    reservoir_depth_m = CASE WHEN ({script.datasetName}.reservoir_depth_m IS NOT NULL AND {script.datasetName}.reservoir_depth_m IS DISTINCT FROM cabd.reservoir_depth_m) THEN {script.datasetName}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    reservoir_area_skm = CASE WHEN ({script.datasetName}.reservoir_area_skm IS NOT NULL AND {script.datasetName}.reservoir_area_skm IS DISTINCT FROM cabd.reservoir_area_skm) THEN {script.datasetName}.reservoir_area_skm ELSE cabd.reservoir_area_skm END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)