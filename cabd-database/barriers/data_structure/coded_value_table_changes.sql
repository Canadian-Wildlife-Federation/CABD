---------------------------------------------------------------------
-- This set of queries requires the new structure for each changed
-- coded value table to be imported into the database.
-- THIS STILL NEEDS TESTING BUT METHOD IS STRAIGHTFORWARD
---------------------------------------------------------------------

-- replace structure_type and function_code tables entirely
DELETE * FROM dams.structure_type_codes;
INSERT INTO dams.structure_type_codes
    (code, name_en, description_en, name_fr, description_fr)
SELECT
    code, name, description, name, description
FROM dams.temp_structure_type_codes;

DELETE * FROM dams.function_codes;
INSERT INTO dams.function_codes
    (code, name_en, description_en, name_fr, description_fr)
SELECT
    code, name, description, name, description
FROM dams.temp_function_codes;


-- change a number of codes for 'Unknown' to code 99 and
-- reshuffle values where needed

--------------------------------------------
-- RECORD OLD VALUES - JUST FOR TESTING
--------------------------------------------

ALTER TABLE dams.dams ADD COLUMN structure_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN function_code_old int2;
ALTER TABLE dams.dams ADD COLUMN size_class_code_old int2;
ALTER TABLE dams.dams ADD COLUMN use_code_old int2;
ALTER TABLE dams.dams ADD COLUMN operating_status_code_old int2;
ALTER TABLE dams.dams ADD COLUMN ownership_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN spillway_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN turbine_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN up_passage_type_code_old int2;

UPDATE dams.dams SET structure_type_code_old = structure_type_code;
UPDATE dams.dams SET function_code_old = function_code;
UPDATE dams.dams SET size_class_code_old = size_class_code;
UPDATE dams.dams SET use_code_old = use_code;
UPDATE dams.dams SET operating_status_code_old = operating_status_code;
UPDATE dams.dams SET ownership_type_code_old = ownership_type_code;
UPDATE dams.dams SET spillway_type_code_old = spillway_type_code;
UPDATE dams.dams SET turbine_type_code_old = turbine_type_code;
UPDATE dams.dams SET up_passage_type_code_old = up_passage_type_code;

ALTER TABLE fishways.fishways ADD COLUMN fishpass_type_code_old int2;
UPDATE fishways.fishways SET fishpass_type_code_old = fishpass_type_code;

--------------------------------------------
-- size class
--------------------------------------------

UPDATE dams.size_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET size_class_code = (SELECT code FROM dams.size_codes WHERE name_en = 'Unknown')
WHERE size_class_code = 4;

--------------------------------------------
-- dam use
--------------------------------------------

UPDATE dams.dam_use_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown') 
WHERE use_code = 11;

UPDATE dams.dam_use_codes
SET code = 10 WHERE name_en = 'Wildlife Conservation';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Wildlife Conservation')
WHERE use_code = 12;

UPDATE dams.dam_use_codes
SET code = 11 WHERE name_en = 'Other';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Other')
WHERE use_code = 10;

--------------------------------------------
-- operating status
--------------------------------------------

UPDATE dams.operating_status_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.operating_status_codes
SET name_en = 
    CASE 
    WHEN name_en = 'Abandoned/Orphaned' THEN 'Abandoned/ Orphaned'
    WHEN name_en = 'Decommissioned/Removed' THEN 'Decommissioned/ Removed'
    WHEN name_en = 'Retired/Closed' THEN 'Retired/ Closed'
    ELSE name_en END;

UPDATE dams.dams
SET operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown') 
WHERE operating_status_code = 5;

UPDATE dams.operating_status_codes
SET code = 5 WHERE name_en = 'Remediated';

UPDATE dams.dams
SET operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Remediated') 
WHERE operating_status_code = 6;

--------------------------------------------
-- ownership type
--------------------------------------------

UPDATE cabd.barrier_ownership_type_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE cabd.barrier_ownership_type_codes
SET name_en = 
    CASE 
    WHEN name_en = 'Charity/Non-profit' THEN 'Charity/ Non-profit'
    WHEN name_en = 'Provincial/Territorial' THEN 'Provincial/ Territorial'
    ELSE name_en END;

UPDATE dams.dams
SET ownership_type_code = (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown') 
WHERE ownership_type_code = 7;

UPDATE cabd.barrier_ownership_type_codes
SET code = 7 WHERE name_en = 'Indigenous';

UPDATE dams.dams
SET ownership_type_code = (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Indigenous') 
WHERE ownership_type_code = 8;

--------------------------------------------
-- spillway type
--------------------------------------------

UPDATE dams.spillway_type_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET spillway_type_code = (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Unknown')
WHERE spillway_type_code = 6;

--------------------------------------------
-- turbine type
--------------------------------------------

UPDATE dams.turbine_type_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET turbine_type_code = (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Unknown') 
WHERE turbine_type_code = 5;

UPDATE dams.turbine_type_codes
SET code = 5 WHERE name_en = 'Other';

UPDATE dams.dams
SET turbine_type_code = (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Other') 
WHERE turbine_type_code = 6;

--------------------------------------------
-- upstream passage type
--------------------------------------------

UPDATE cabd.upstream_passage_type_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET up_passage_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') 
WHERE up_passage_type_code = 9;

UPDATE fishways.fishways
SET fishpass_type_code = (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown') 
WHERE fishpass_type_code = 9;

--------------------------------------------
-- structure type - just for ones where
-- original data source no longer accessible
-- or this info is no longer included
--------------------------------------------

UPDATE dams.dams
SET structure_type_code = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Other')
WHERE structure_type_code = 10;