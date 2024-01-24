import MAP_crossing_attributes_main as main

script = main.MappingScript("kwrc_master_2")

mappingquery = f"""

UPDATE
    {script.nonTidalSitesTable} AS cabd
SET
    cabd_assessment_id = {script.datasetname}.cabd_assessment_id,
    original_assessment_id = {script.datasetname}.original_assessment_id,
    data_source_id = {script.datasetname}.data_source_id,
    date_observed = CASE WHEN ({script.datasetname}.date_observed IS NOT NULL AND {script.datasetname}.date_observed IS DISTINCT FROM cabd.date_observed) THEN {script.datasetname}.date_observed ELSE cabd.date_observed END,
    road_type_code = CASE WHEN ({script.datasetname}.road_type_code IS NOT NULL AND {script.datasetname}.road_type_code IS DISTINCT FROM cabd.road_type_code) THEN {script.datasetname}.road_type_code ELSE cabd.road_type_code END,
    road_class = CASE WHEN ({script.datasetname}.road_class IS NOT NULL AND {script.datasetname}.road_class IS DISTINCT FROM cabd.road_class) THEN {script.datasetname}.road_class ELSE cabd.road_class END,
    road_surface = CASE WHEN ({script.datasetname}.road_surface IS NOT NULL AND {script.datasetname}.road_surface IS DISTINCT FROM cabd.road_surface) THEN {script.datasetname}.road_surface ELSE cabd.road_surface END,
    site_type = CASE WHEN ({script.datasetname}.site_type IS NOT NULL AND {script.datasetname}.site_type IS DISTINCT FROM cabd.site_type) THEN {script.datasetname}.site_type ELSE cabd.site_type END,
    alignment_code = CASE WHEN ({script.datasetname}.alignment_code IS NOT NULL AND {script.datasetname}.alignment_code IS DISTINCT FROM cabd.alignment_code) THEN {script.datasetname}.alignment_code ELSE cabd.alignment_code END,
    upstream_channel_depth_m = CASE WHEN ({script.datasetname}.upstream_channel_depth_m IS NOT NULL AND {script.datasetname}.upstream_channel_depth_m IS DISTINCT FROM cabd.upstream_channel_depth_m) THEN {script.datasetname}.upstream_channel_depth_m ELSE cabd.upstream_channel_depth_m END,
    downstream_channel_depth_m = CASE WHEN ({script.datasetname}.downstream_channel_depth_m IS NOT NULL AND {script.datasetname}.downstream_channel_depth_m IS DISTINCT FROM cabd.downstream_channel_depth_m) THEN {script.datasetname}.downstream_channel_depth_m ELSE cabd.downstream_channel_depth_m END,
    upstream_bankfull_width_m = CASE WHEN ({script.datasetname}.upstream_bankfull_width_m IS NOT NULL AND {script.datasetname}.upstream_bankfull_width_m IS DISTINCT FROM cabd.upstream_bankfull_width_m) THEN {script.datasetname}.upstream_bankfull_width_m ELSE cabd.upstream_bankfull_width_m END,
    downstream_bankfull_width_m = CASE WHEN ({script.datasetname}.downstream_bankfull_width_m IS NOT NULL AND {script.datasetname}.downstream_bankfull_width_m IS DISTINCT FROM cabd.downstream_bankfull_width_m) THEN {script.datasetname}.downstream_bankfull_width_m ELSE cabd.downstream_bankfull_width_m END,
    constriction_code = CASE WHEN ({script.datasetname}.constriction_code IS NOT NULL AND {script.datasetname}.constriction_code IS DISTINCT FROM cabd.constriction_code) THEN {script.datasetname}.constriction_code ELSE cabd.constriction_code END,
    tailwater_scour_pool_code = CASE WHEN ({script.datasetname}.tailwater_scour_pool_code IS NOT NULL AND {script.datasetname}.tailwater_scour_pool_code IS DISTINCT FROM cabd.tailwater_scour_pool_code) THEN {script.datasetname}.tailwater_scour_pool_code ELSE cabd.tailwater_scour_pool_code END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id
    AND {script.datasetname}.entry_classification = 'update feature';

"""

script.do_work(mappingquery)