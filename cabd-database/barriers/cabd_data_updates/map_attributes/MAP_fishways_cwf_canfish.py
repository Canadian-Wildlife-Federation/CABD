import MAP_attributes_main as main

script = main.MappingScript("cwf_canfish")

mappingquery = f"""

--find CABD IDs
UPDATE {script.workingTable} SET cabd_id = NULL;

UPDATE
    {script.workingTable} AS {script.datasetname}
SET
    cabd_id = duplicates.cabd_id
FROM
    {script.fishwayTable} AS duplicates
WHERE
    ({script.datasetname}.data_source_id = duplicates.data_source_id AND duplicates.data_source_text = '{script.datasetname}');

--update existing features
UPDATE
    {script.fishwayAttributeTable} AS cabdsource
SET    
    structure_name_en_ds = CASE WHEN (cabd.structure_name_en IS NULL AND {script.datasetname}.structure_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.structure_name_en_ds END,   
    structure_name_fr_ds = CASE WHEN (cabd.structure_name_fr IS NULL AND {script.datasetname}.structure_name_fr IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.structure_name_fr_ds END,   
    river_name_en_ds = CASE WHEN (cabd.river_name_en IS NULL AND {script.datasetname}.river_name_en IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.river_name_en_ds END,   
    fishpass_type_code_ds = CASE WHEN (cabd.fishpass_type_code IS NULL AND {script.datasetname}.fishpass_type_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.fishpass_type_code_ds END,
    monitoring_equipment_ds = CASE WHEN (cabd.monitoring_equipment IS NULL AND {script.datasetname}.monitoring_equipment IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.monitoring_equipment_ds END,
    contracted_by_ds = CASE WHEN (cabd.contracted_by IS NULL AND {script.datasetname}.contracted_by IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.contracted_by_ds END,
    constructed_by_ds = CASE WHEN (cabd.constructed_by IS NULL AND {script.datasetname}.constructed_by IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.constructed_by_ds END,
    plans_held_by_ds = CASE WHEN (cabd.plans_held_by IS NULL AND {script.datasetname}.plans_held_by IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.plans_held_by_ds END,
    purpose_ds = CASE WHEN (cabd.purpose IS NULL AND {script.datasetname}.purpose IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.purpose_ds END,
    designed_on_biology_ds = CASE WHEN (cabd.designed_on_biology IS NULL AND {script.datasetname}.designed_on_biology IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.designed_on_biology_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.length_m_ds END,
    elevation_m_ds = CASE WHEN (cabd.elevation_m IS NULL AND {script.datasetname}.elevation_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.elevation_m_ds END,
    gradient_ds = CASE WHEN (cabd.gradient IS NULL AND {script.datasetname}.gradient IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.gradient_ds END,
    depth_m_ds = CASE WHEN (cabd.depth_m IS NULL AND {script.datasetname}.depth_m IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.depth_m_ds END,
    entrance_location_code_ds = CASE WHEN (cabd.entrance_location_code IS NULL AND {script.datasetname}.entrance_location_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.entrance_location_code_ds END,
    entrance_position_code_ds = CASE WHEN (cabd.entrance_position_code IS NULL AND {script.datasetname}.entrance_position_code IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.entrance_position_code_ds END,
    modified_ds = CASE WHEN (cabd.modified IS NULL AND {script.datasetname}.modified IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.modified_ds END,
    modification_year_ds = CASE WHEN (cabd.modification_year IS NULL AND {script.datasetname}.modification_year IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.modification_year_ds END,
    modification_purpose_ds = CASE WHEN (cabd.modification_purpose IS NULL AND {script.datasetname}.modification_purpose IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.modification_purpose_ds END,
    year_constructed_ds = CASE WHEN (cabd.year_constructed IS NULL AND {script.datasetname}.year_constructed IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.year_constructed_ds END,
    operated_by_ds = CASE WHEN (cabd.operated_by IS NULL AND {script.datasetname}.operated_by IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operated_by_ds END,
    operation_period_ds = CASE WHEN (cabd.operation_period IS NULL AND {script.datasetname}.operation_period IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operation_period_ds END,
    has_evaluating_studies_ds = CASE WHEN (cabd.has_evaluating_studies IS NULL AND {script.datasetname}.has_evaluating_studies IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.has_evaluating_studies_ds END,
    nature_of_evaluation_studies_ds = CASE WHEN (cabd.nature_of_evaluation_studies IS NULL AND {script.datasetname}.nature_of_evaluation_studies IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.nature_of_evaluation_studies_ds END,
    engineering_notes_ds = CASE WHEN (cabd.engineering_notes IS NULL AND {script.datasetname}.engineering_notes IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.engineering_notes_ds END,
    operating_notes_ds = CASE WHEN (cabd.operating_notes IS NULL AND {script.datasetname}.operating_notes IS NOT NULL) THEN {script.datasetname}.data_source ELSE cabdsource.operating_notes_ds END
FROM
    {script.fishwayTable} AS cabd,
    {script.workingTable} AS {script.datasetname}
WHERE
    cabdsource.cabd_id = {script.datasetname}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.fishwayTable} AS cabd
SET
    structure_name_en = CASE WHEN (cabd.structure_name_en IS NULL AND {script.datasetname}.structure_name_en IS NOT NULL) THEN {script.datasetname}.structure_name_en ELSE cabd.structure_name_en END,
    structure_name_fr = CASE WHEN (cabd.structure_name_fr IS NULL AND {script.datasetname}.structure_name_fr IS NOT NULL) THEN {script.datasetname}.structure_name_fr ELSE cabd.structure_name_fr END,
    river_name_en = CASE WHEN (cabd.river_name_en IS NULL AND {script.datasetname}.river_name_en IS NOT NULL) THEN {script.datasetname}.river_name_en ELSE cabd.river_name_en END,
    fishpass_type_code = CASE WHEN (cabd.fishpass_type_code IS NULL AND {script.datasetname}.fishpass_type_code IS NOT NULL) THEN {script.datasetname}.fishpass_type_code ELSE cabd.fishpass_type_code END,
    monitoring_equipment = CASE WHEN (cabd.monitoring_equipment IS NULL AND {script.datasetname}.monitoring_equipment IS NOT NULL) THEN {script.datasetname}.monitoring_equipment ELSE cabd.monitoring_equipment END,
    contracted_by = CASE WHEN (cabd.contracted_by IS NULL AND {script.datasetname}.contracted_by IS NOT NULL) THEN {script.datasetname}.contracted_by ELSE cabd.contracted_by END,
    constructed_by = CASE WHEN (cabd.constructed_by IS NULL AND {script.datasetname}.constructed_by IS NOT NULL) THEN {script.datasetname}.constructed_by ELSE cabd.constructed_by END,
    plans_held_by = CASE WHEN (cabd.plans_held_by IS NULL AND {script.datasetname}.plans_held_by IS NOT NULL) THEN {script.datasetname}.plans_held_by ELSE cabd.plans_held_by END,
    purpose = CASE WHEN (cabd.purpose IS NULL AND {script.datasetname}.purpose IS NOT NULL) THEN {script.datasetname}.purpose ELSE cabd.purpose END,
    designed_on_biology = CASE WHEN (cabd.designed_on_biology IS NULL AND {script.datasetname}.designed_on_biology IS NOT NULL) THEN {script.datasetname}.designed_on_biology ELSE cabd.designed_on_biology END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND {script.datasetname}.length_m IS NOT NULL) THEN {script.datasetname}.length_m ELSE cabd.length_m END,
    elevation_m = CASE WHEN (cabd.elevation_m IS NULL AND {script.datasetname}.elevation_m IS NOT NULL) THEN {script.datasetname}.elevation_m ELSE cabd.elevation_m END,
    gradient = CASE WHEN (cabd.gradient IS NULL AND {script.datasetname}.gradient IS NOT NULL) THEN {script.datasetname}.gradient ELSE cabd.gradient END,
    depth_m = CASE WHEN (cabd.depth_m IS NULL AND {script.datasetname}.depth_m IS NOT NULL) THEN {script.datasetname}.depth_m ELSE cabd.depth_m END,
    entrance_location_code = CASE WHEN (cabd.entrance_location_code IS NULL AND {script.datasetname}.entrance_location_code IS NOT NULL) THEN {script.datasetname}.entrance_location_code ELSE cabd.entrance_location_code END,
    entrance_position_code = CASE WHEN (cabd.entrance_position_code IS NULL AND {script.datasetname}.entrance_position_code IS NOT NULL) THEN {script.datasetname}.entrance_position_code ELSE cabd.entrance_position_code END,
    modified = CASE WHEN (cabd.modified IS NULL AND {script.datasetname}.modified IS NOT NULL) THEN {script.datasetname}.modified ELSE cabd.modified END,
    modification_year = CASE WHEN (cabd.modification_year IS NULL AND {script.datasetname}.modification_year IS NOT NULL) THEN {script.datasetname}.modification_year ELSE cabd.modification_year END,
    modification_purpose = CASE WHEN (cabd.modification_purpose IS NULL AND {script.datasetname}.modification_purpose IS NOT NULL) THEN {script.datasetname}.modification_purpose ELSE cabd.modification_purpose END,
    year_constructed = CASE WHEN (cabd.year_constructed IS NULL AND {script.datasetname}.year_constructed IS NOT NULL) THEN {script.datasetname}.year_constructed ELSE cabd.year_constructed END,
    operated_by = CASE WHEN (cabd.operated_by IS NULL AND {script.datasetname}.operated_by IS NOT NULL) THEN {script.datasetname}.operated_by ELSE cabd.operated_by END,
    operation_period = CASE WHEN (cabd.operation_period IS NULL AND {script.datasetname}.operation_period IS NOT NULL) THEN {script.datasetname}.operation_period ELSE cabd.operation_period END,
    has_evaluating_studies = CASE WHEN (cabd.has_evaluating_studies IS NULL AND {script.datasetname}.has_evaluating_studies IS NOT NULL) THEN {script.datasetname}.has_evaluating_studies ELSE cabd.has_evaluating_studies END,
    nature_of_evaluation_studies = CASE WHEN (cabd.nature_of_evaluation_studies IS NULL AND {script.datasetname}.nature_of_evaluation_studies IS NOT NULL) THEN {script.datasetname}.nature_of_evaluation_studies ELSE cabd.nature_of_evaluation_studies END,
    engineering_notes = CASE WHEN (cabd.engineering_notes IS NULL AND {script.datasetname}.engineering_notes IS NOT NULL) THEN {script.datasetname}.engineering_notes ELSE cabd.engineering_notes END,
    operating_notes = CASE WHEN (cabd.operating_notes IS NULL AND {script.datasetname}.operating_notes IS NOT NULL) THEN {script.datasetname}.operating_notes ELSE cabd.operating_notes END,
    species_known_to_use_fishway = CASE WHEN (cabd.species_known_to_use_fishway IS NULL AND {script.datasetname}.species_known_to_use_fishway IS NOT NULL) THEN {script.datasetname}.species_known_to_use_fishway ELSE cabd.species_known_to_use_fishway END,
    species_known_not_to_use_fishway = CASE WHEN (cabd.species_known_not_to_use_fishway IS NULL AND {script.datasetname}.species_known_not_to_use_fishway IS NOT NULL) THEN {script.datasetname}.species_known_not_to_use_fishway ELSE cabd.species_known_not_to_use_fishway END
FROM
    {script.workingTable} AS {script.datasetname}
WHERE
    cabd.cabd_id = {script.datasetname}.cabd_id;

"""

script.do_work(mappingquery)
