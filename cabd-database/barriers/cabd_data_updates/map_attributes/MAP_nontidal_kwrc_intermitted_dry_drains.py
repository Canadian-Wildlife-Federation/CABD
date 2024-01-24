import MAP_crossing_attributes_main as main

script = main.MappingScript("kwrc_intermitted_dry_drains")

mappingquery = f"""

UPDATE
    {script.nonTidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    original_assessment_id = {script.datasetname}.original_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    flow_condition_code = CASE WHEN ({script.datasetname}.flow_condition_code IS NOT NULL AND {script.datasetname}.flow_condition_code IS DISTINCT FROM cabd.flow_condition_code) THEN {script.datasetname}.flow_condition_code ELSE cabd.flow_condition_code END,
    crossing_comments = CASE WHEN ({script.datasetname}.crossing_comments IS NOT NULL AND {script.datasetname}.crossing_comments IS DISTINCT FROM cabd.crossing_comments) THEN {script.datasetname}.crossing_comments ELSE cabd.crossing_comments END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)