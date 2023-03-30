import MAP_crossing_attributes_main as main

script = main.MappingScript("acapsj_master_sheet")

mappingquery = f"""

UPDATE
    {script.nonTidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    original_assessment_id = {script.datasetname}.original_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    date_observed = CASE WHEN ({script.datasetname}.date_observed IS NOT NULL AND {script.datasetname}.date_observed IS DISTINCT FROM cabd.date_observed) THEN {script.datasetname}.date_observed ELSE cabd.date_observed END,
    lead_observer = CASE WHEN ({script.datasetname}.lead_observer IS NOT NULL AND {script.datasetname}.lead_observer IS DISTINCT FROM cabd.lead_observer) THEN {script.datasetname}.lead_observer ELSE cabd.lead_observer END,
    stream_name = CASE WHEN ({script.datasetname}.stream_name IS NOT NULL AND {script.datasetname}.stream_name IS DISTINCT FROM cabd.stream_name) THEN {script.datasetname}.stream_name ELSE cabd.stream_name END, 
    crossing_type_code = CASE WHEN ({script.datasetname}.crossing_type_code IS NOT NULL AND {script.datasetname}.crossing_type_code IS DISTINCT FROM cabd.crossing_type_code) THEN {script.datasetname}.crossing_type_code ELSE cabd.crossing_type_code END,
    road_type_code = CASE WHEN ({script.datasetname}.road_type_code IS NOT NULL AND {script.datasetname}.road_type_code IS DISTINCT FROM cabd.road_type_code) THEN {script.datasetname}.road_type_code ELSE cabd.road_type_code END, 
    road_surface = CASE WHEN ({script.datasetname}.road_surface IS NOT NULL AND {script.datasetname}.road_surface IS DISTINCT FROM cabd.road_surface) THEN {script.datasetname}.road_surface ELSE cabd.road_surface END, 
    road_class = CASE WHEN ({script.datasetname}.road_class IS NOT NULL AND {script.datasetname}.road_class IS DISTINCT FROM cabd.road_class) THEN {script.datasetname}.road_class ELSE cabd.road_class END, 
    crossing_condition_code = CASE WHEN ({script.datasetname}.crossing_condition_code IS NOT NULL AND {script.datasetname}.crossing_condition_code IS DISTINCT FROM cabd.crossing_condition_code) THEN {script.datasetname}.crossing_condition_code ELSE cabd.crossing_condition_code END,
    flow_condition_code = CASE WHEN ({script.datasetname}.flow_condition_code IS NOT NULL AND {script.datasetname}.flow_condition_code IS DISTINCT FROM cabd.flow_condition_code) THEN {script.datasetname}.flow_condition_code ELSE cabd.flow_condition_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)
