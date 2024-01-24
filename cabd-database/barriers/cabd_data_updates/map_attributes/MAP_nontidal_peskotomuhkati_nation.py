import MAP_crossing_attributes_main as main

script = main.MappingScript("peskotomuhkati_nation_01192023")

mappingquery = f"""

UPDATE
    {script.nonTidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    original_assessment_id = {script.datasetname}.original_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    date_observed = CASE WHEN ({script.datasetname}.date_observed IS NOT NULL AND {script.datasetname}.date_observed IS DISTINCT FROM cabd.date_observed) THEN {script.datasetname}.date_observed ELSE cabd.date_observed END,
    stream_name = CASE WHEN ({script.datasetname}.stream_name IS NOT NULL AND {script.datasetname}.stream_name IS DISTINCT FROM cabd.stream_name) THEN {script.datasetname}.stream_name ELSE cabd.stream_name END,
    road_name = CASE WHEN ({script.datasetname}.road_name IS NOT NULL AND {script.datasetname}.road_name IS DISTINCT FROM cabd.road_name) THEN {script.datasetname}.road_name ELSE cabd.road_name END,
    road_type_code = CASE WHEN ({script.datasetname}.road_type_code IS NOT NULL AND {script.datasetname}.road_type_code IS DISTINCT FROM cabd.road_type_code) THEN {script.datasetname}.road_type_code ELSE cabd.road_type_code END,
    crossing_type_code = CASE WHEN ({script.datasetname}.crossing_type_code IS NOT NULL AND {script.datasetname}.crossing_type_code IS DISTINCT FROM cabd.crossing_type_code) THEN {script.datasetname}.crossing_type_code ELSE cabd.crossing_type_code END,
    structure_count = CASE WHEN ({script.datasetname}.structure_count IS NOT NULL AND {script.datasetname}.structure_count IS DISTINCT FROM cabd.structure_count) THEN {script.datasetname}.structure_count ELSE cabd.structure_count END,
    flow_condition_code = CASE WHEN ({script.datasetname}.flow_condition_code IS NOT NULL AND {script.datasetname}.flow_condition_code IS DISTINCT FROM cabd.flow_condition_code) THEN {script.datasetname}.flow_condition_code ELSE cabd.flow_condition_code END,
    crossing_condition_code = CASE WHEN ({script.datasetname}.crossing_condition_code IS NOT NULL AND {script.datasetname}.crossing_condition_code IS DISTINCT FROM cabd.crossing_condition_code) THEN {script.datasetname}.crossing_condition_code ELSE cabd.crossing_condition_code END,
    site_type = CASE WHEN ({script.datasetname}.site_type IS NOT NULL AND {script.datasetname}.site_type IS DISTINCT FROM cabd.site_type) THEN {script.datasetname}.site_type ELSE cabd.site_type END,
    alignment_code = CASE WHEN ({script.datasetname}.alignment_code IS NOT NULL AND {script.datasetname}.alignment_code IS DISTINCT FROM cabd.alignment_code) THEN {script.datasetname}.alignment_code ELSE cabd.alignment_code END,
    road_fill_height_m = CASE WHEN ({script.datasetname}.road_fill_height_m IS NOT NULL AND {script.datasetname}.road_fill_height_m IS DISTINCT FROM cabd.road_fill_height_m) THEN {script.datasetname}.road_fill_height_m ELSE cabd.road_fill_height_m END,
    upstream_channel_depth_m = CASE WHEN ({script.datasetname}.upstream_channel_depth_m IS NOT NULL AND {script.datasetname}.upstream_channel_depth_m IS DISTINCT FROM cabd.upstream_channel_depth_m) THEN {script.datasetname}.upstream_channel_depth_m ELSE cabd.upstream_channel_depth_m END,
    downstream_channel_depth_m = CASE WHEN ({script.datasetname}.downstream_channel_depth_m IS NOT NULL AND {script.datasetname}.downstream_channel_depth_m IS DISTINCT FROM cabd.downstream_channel_depth_m) THEN {script.datasetname}.downstream_channel_depth_m ELSE cabd.downstream_channel_depth_m END,
    upstream_bankfull_width_m = CASE WHEN ({script.datasetname}.upstream_bankfull_width_m IS NOT NULL AND {script.datasetname}.upstream_bankfull_width_m IS DISTINCT FROM cabd.upstream_bankfull_width_m) THEN {script.datasetname}.upstream_bankfull_width_m ELSE cabd.upstream_bankfull_width_m END,
    downstream_bankfull_width_m = CASE WHEN ({script.datasetname}.downstream_bankfull_width_m IS NOT NULL AND {script.datasetname}.downstream_bankfull_width_m IS DISTINCT FROM cabd.downstream_bankfull_width_m) THEN {script.datasetname}.downstream_bankfull_width_m ELSE cabd.downstream_bankfull_width_m END,
    upstream_bankfull_width_confidence_code = CASE WHEN ({script.datasetname}.upstream_bankfull_width_confidence_code IS NOT NULL AND {script.datasetname}.upstream_bankfull_width_confidence_code IS DISTINCT FROM cabd.upstream_bankfull_width_confidence_code) THEN {script.datasetname}.upstream_bankfull_width_confidence_code ELSE cabd.upstream_bankfull_width_confidence_code END,
    downstream_bankfull_width_confidence_code = CASE WHEN ({script.datasetname}.downstream_bankfull_width_confidence_code IS NOT NULL AND {script.datasetname}.downstream_bankfull_width_confidence_code IS DISTINCT FROM cabd.downstream_bankfull_width_confidence_code) THEN {script.datasetname}.downstream_bankfull_width_confidence_code ELSE cabd.downstream_bankfull_width_confidence_code END,
    constriction_code = CASE WHEN ({script.datasetname}.constriction_code IS NOT NULL AND {script.datasetname}.constriction_code IS DISTINCT FROM cabd.constriction_code) THEN {script.datasetname}.constriction_code ELSE cabd.constriction_code END,
    tailwater_scour_pool_code = CASE WHEN ({script.datasetname}.tailwater_scour_pool_code IS NOT NULL AND {script.datasetname}.tailwater_scour_pool_code IS DISTINCT FROM cabd.tailwater_scour_pool_code) THEN {script.datasetname}.tailwater_scour_pool_code ELSE cabd.tailwater_scour_pool_code END,
    crossing_comments = CASE WHEN ({script.datasetname}.crossing_comments IS NOT NULL AND {script.datasetname}.crossing_comments IS DISTINCT FROM cabd.crossing_comments) THEN {script.datasetname}.crossing_comments ELSE cabd.crossing_comments END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)
