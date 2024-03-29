import nonspatial as main

script = main.MappingScript("ab_facility_names")

mappingquery = f"""

--create new data source records
INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'awps_brid',
    'https://albertawater.com/latest/8786-bow-river-irrigation-district/',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'alrs_spray_lakes',
    'https://www.canlii.org/en/commentary/doc/2012CanLIIDocs150#!',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'amec_ssrba_wss',
    'https://www1.agric.gov.ab.ca/$Department/deptdocs.nsf/all/irr13053/$FILE/ssrb_main_report.pdf',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'awa_obed_spill',
    'https://albertawilderness.ca/news-release-4-years-obed-spill-questions-remain/',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'golder_genesee_2013',
    'https://www.capitalpower.com/wp-content/uploads/2019/06/Appendix_F_Genesee_Cooling_Pond_Modelling_Study.pdf',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'ecc_ceaa_090152237',
    'https://www.ceaa-acee.gc.ca/052/details-eng.cfm?pid=52237',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'aep_cldps',
    'https://legacy.merx.com/English/SUPPLIER_Menu.asp?WCE=Show&TAB=1&PORTAL=MERX&State=7&id=AB-2015-02510&src=osr&FED_ONLY=0&ACTION=NEXT&rowcount=2000&lastpage=200&MoreResults=&PUBSORT=2&CLOSESORT=0&IS_SME=N&langswitch=1&hcode=sTvMTncPfnbG4oSXtP%2fDhw%3d%3d',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'nrcb_earp_app9401',
    'https://publications.gc.ca/collections/collection_2018/acee-ceaa/En106-167-1995-eng.pdf',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'awps_smrid',
    'https://albertawater.com/latest/8795-st-mary-river-irrigation-district-projects-overview/',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'transalta_interlakes',
    'https://transalta.com/plants-operation/interlakes/',
    'Accessed May 6, 2022',
    'non-spatial');

INSERT INTO cabd.data_source (id, name, source, comments, source_type)
VALUES(
    uuid_generate_v4(),
    'wiki_dickson_dam',
    'https://en.wikipedia.org/wiki/Dickson_Dam#cite_note-3',
    'Accessed May 6, 2022',
    'non-spatial');

--add data source ids to the table
ALTER TABLE {script.sourceTable} RENAME COLUMN data_source to data_source_text;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
UPDATE {script.sourceTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_text;

--SELECT DISTINCT data_source, data_source_text FROM {script.sourceTable};

--update existing features 
UPDATE
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    facility_name_en_ds = CASE WHEN {script.datasetName}.facility_name_en IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.facility_name_en_ds END,
    owner_ds = CASE WHEN {script.datasetName}.owner IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN {script.datasetName}.ownership_type_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.ownership_type_code_ds END,
    use_code_ds = CASE WHEN {script.datasetName}.dam_name_en IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.dam_name_en_ds END,
    use_irrigation_code_ds = CASE WHEN {script.datasetName}.use_irrigation_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN {script.datasetName}.use_electricity_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.use_electricity_code_ds END,
    construction_year_ds = CASE WHEN {script.datasetName}.construction_year IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.construction_year_ds END,    
    function_code_ds = CASE WHEN {script.datasetName}.function_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.function_code_ds END,
    construction_type_code_ds = CASE WHEN {script.datasetName}.construction_type_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.construction_type_code_ds END,    
    spillway_capacity_ds = CASE WHEN {script.datasetName}.spillway_capacity IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.spillway_capacity_ds END,
    spillway_type_code_ds = CASE WHEN {script.datasetName}.spillway_type_code IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.spillway_type_code_ds END,
    reservoir_present_ds = CASE WHEN {script.datasetName}.reservoir_present IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_present_ds END,
    reservoir_area_skm_ds = CASE WHEN {script.datasetName}.reservoir_area_skm IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN {script.datasetName}.reservoir_depth_m IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.reservoir_depth_m_ds END,
    avg_rate_of_discharge_ls_ds = CASE WHEN {script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.avg_rate_of_discharge_ls_ds END,
    federal_flow_req_ds = CASE WHEN {script.datasetName}.federal_flow_req IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.federal_flow_req_ds END,
    generating_capacity_mwh_ds = CASE WHEN {script.datasetName}.generating_capacity_mwh IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.generating_capacity_mwh_ds END,
    turbine_number_ds = CASE WHEN {script.datasetName}.turbine_number IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.turbine_number_ds END,
    comments_ds = CASE WHEN {script.datasetName}.comments IS NOT NULL THEN {script.datasetName}.data_source ELSE cabdsource.comments_ds END
FROM
    {script.damTable} AS cabd,
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabdsource.cabd_id = {script.datasetName}.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN ({script.datasetName}.dam_name_en IS NOT NULL AND {script.datasetName}.dam_name_en IS DISTINCT FROM cabd.dam_name_en) THEN {script.datasetName}.dam_name_en ELSE cabd.dam_name_en END,
    facility_name_en = CASE WHEN {script.datasetName}.facility_name_en IS NOT NULL THEN {script.datasetName}.facility_name_en ELSE cabd.facility_name_en END,
    "owner" = CASE WHEN {script.datasetName}.owner IS NOT NULL THEN {script.datasetName}.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN {script.datasetName}.ownership_type_code IS NOT NULL THEN {script.datasetName}.ownership_type_code ELSE cabd.ownership_type_code END,
    use_code = CASE WHEN {script.datasetName}.use_code IS NOT NULL THEN {script.datasetName}.use_code ELSE cabd.use_code END,
    use_irrigation_code = CASE WHEN {script.datasetName}.use_irrigation_code IS NOT NULL THEN {script.datasetName}.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN {script.datasetName}.use_electricity_code IS NOT NULL THEN {script.datasetName}.use_electricity_code ELSE cabd.use_electricity_code END,
    construction_year = CASE WHEN {script.datasetName}.construction_year IS NOT NULL THEN {script.datasetName}.construction_year ELSE cabd.construction_year END,
    function_code = CASE WHEN {script.datasetName}.function_code IS NOT NULL THEN {script.datasetName}.function_code ELSE cabd.function_code END,
    construction_type_code = CASE WHEN {script.datasetName}.construction_type_code IS NOT NULL THEN {script.datasetName}.construction_type_code ELSE cabd.construction_type_code END,
    spillway_capacity = CASE WHEN {script.datasetName}.spillway_capacity IS NOT NULL THEN {script.datasetName}.spillway_capacity ELSE cabd.spillway_capacity END,
    spillway_type_code = CASE WHEN {script.datasetName}.spillway_type_code IS NOT NULL THEN {script.datasetName}.spillway_type_code ELSE cabd.spillway_type_code END,
    reservoir_present = CASE WHEN {script.datasetName}.reservoir_present IS NOT NULL THEN {script.datasetName}.reservoir_present ELSE cabd.reservoir_present END,
    reservoir_area_skm = CASE WHEN {script.datasetName}.reservoir_area_skm IS NOT NULL THEN {script.datasetName}.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN {script.datasetName}.reservoir_depth_m IS NOT NULL THEN {script.datasetName}.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    avg_rate_of_discharge_ls = CASE WHEN {script.datasetName}.avg_rate_of_discharge_ls IS NOT NULL THEN {script.datasetName}.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
    federal_flow_req = CASE WHEN {script.datasetName}.federal_flow_req IS NOT NULL THEN {script.datasetName}.federal_flow_req ELSE cabd.federal_flow_req END,
    generating_capacity_mwh = CASE WHEN {script.datasetName}.generating_capacity_mwh IS NOT NULL THEN {script.datasetName}.generating_capacity_mwh ELSE cabd.generating_capacity_mwh END,
    turbine_number = CASE WHEN {script.datasetName}.turbine_number IS NOT NULL THEN {script.datasetName}.turbine_number ELSE cabd.turbine_number END,
    "comments" = CASE WHEN {script.datasetName}.comments IS NOT NULL THEN {script.datasetName}.comments ELSE cabd.comments END
FROM
    {script.sourceTable} AS {script.datasetName}
WHERE
    cabd.cabd_id = {script.datasetName}.cabd_id;

"""

script.do_work(mappingquery)
