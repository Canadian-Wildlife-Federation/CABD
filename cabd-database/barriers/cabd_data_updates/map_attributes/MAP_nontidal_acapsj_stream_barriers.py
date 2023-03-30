import MAP_crossing_attributes_main as main

script = main.MappingScript("acapsj_stream_barriers")

mappingquery = f"""

UPDATE
    {script.nonTidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    crossing_type_code = CASE WHEN ({script.datasetname}.crossing_type_code IS NOT NULL AND {script.datasetname}.crossing_type_code IS DISTINCT FROM cabd.crossing_type_code) THEN {script.datasetname}.crossing_type_code ELSE cabd.crossing_type_code END,
    stream_name = CASE WHEN ({script.datasetname}.stream_name IS NOT NULL AND {script.datasetname}.stream_name IS DISTINCT FROM cabd.stream_name) THEN {script.datasetname}.stream_name ELSE cabd.stream_name END,
    crossing_comments = CASE WHEN ({script.datasetname}.crossing_comments IS NOT NULL AND {script.datasetname}.crossing_comments IS DISTINCT FROM cabd.crossing_comments) THEN {script.datasetname}.crossing_comments ELSE cabd.crossing_comments END,
    site_type = CASE WHEN ({script.datasetname}.site_type_code IS NOT NULL AND {script.datasetname}.site_type_code IS DISTINCT FROM cabd.site_type) THEN {script.datasetname}.site_type_code ELSE cabd.site_type END,
    crossing_condition_code = CASE WHEN ({script.datasetname}.crossing_condition_code IS NOT NULL AND {script.datasetname}.crossing_condition_code IS DISTINCT FROM cabd.crossing_condition_code) THEN {script.datasetname}.crossing_condition_code ELSE cabd.crossing_condition_code END,
    flow_condition_code = CASE WHEN ({script.datasetname}.flow_condition_code IS NOT NULL AND {script.datasetname}.flow_condition_code IS DISTINCT FROM cabd.flow_condition_code) THEN {script.datasetname}.flow_condition_code ELSE cabd.flow_condition_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)
