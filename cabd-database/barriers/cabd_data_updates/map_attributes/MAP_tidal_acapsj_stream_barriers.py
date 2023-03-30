import MAP_crossing_attributes_main as main

script = main.MappingScript("acapsj_stream_barriers")

mappingquery = f"""

UPDATE
    {script.tidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    stream_name = CASE WHEN ({script.datasetname}.stream_name IS NOT NULL AND {script.datasetname}.stream_name IS DISTINCT FROM cabd.stream_name) THEN {script.datasetname}.stream_name ELSE cabd.stream_name END,
    site_type = CASE WHEN ({script.datasetname}.site_type_code IS NOT NULL AND {script.datasetname}.site_type_code IS DISTINCT FROM cabd.site_type) THEN {script.datasetname}.site_type_code ELSE cabd.site_type END, 
    crossing_type_code = CASE WHEN ({script.datasetname}.crossing_type_code IS NOT NULL AND {script.datasetname}.crossing_type_code IS DISTINCT FROM cabd.crossing_type_code) THEN {script.datasetname}.crossing_type_code ELSE cabd.crossing_type_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)
