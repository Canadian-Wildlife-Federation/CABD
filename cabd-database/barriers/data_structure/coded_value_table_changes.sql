---------------------------------------------------------------------
-- This set of queries requires the new structure for each changed
-- coded value table to be imported into the database.
-- THIS STILL NEEDS TESTING BUT METHOD IS STRAIGHTFORWARD
---------------------------------------------------------------------

--------------------------------------------
-- disable triggers while we make changes
--------------------------------------------

ALTER TABLE dams.dams DISABLE TRIGGER ALL;
ALTER TABLE dams.structure_type_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.function_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.size_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.dam_use_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.operating_status_codes DISABLE TRIGGER ALL;
ALTER TABLE cabd.barrier_ownership_type_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.spillway_type_codes DISABLE TRIGGER ALL;
ALTER TABLE dams.turbine_type_codes DISABLE TRIGGER ALL;
ALTER TABLE cabd.upstream_passage_type_codes DISABLE TRIGGER ALL;

-- replace structure_type and function_code tables entirely
DELETE FROM dams.structure_type_codes;
INSERT INTO dams.structure_type_codes
    (code, name_en, description_en, name_fr, description_fr)
SELECT
    code, name_en, description_en, name_fr, description_fr
FROM dams.temp_structure_type_codes;

DELETE FROM dams.function_codes;
INSERT INTO dams.function_codes
    (code, name_en, description_en, name_fr, description_fr)
SELECT
    code, name_en, description_en, name_fr, description_fr
FROM dams.temp_function_codes;

--------------------------------------------
-- RECORD OLD VALUES - JUST FOR TESTING
--------------------------------------------

ALTER TABLE dams.dams ADD COLUMN structure_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN function_code_old int2;
ALTER TABLE dams.dams ADD COLUMN size_class_code_old int2;
ALTER TABLE dams.dams ADD COLUMN dam_use_code_old int2;
ALTER TABLE dams.dams ADD COLUMN operating_status_code_old int2;
ALTER TABLE dams.dams ADD COLUMN ownership_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN spillway_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN turbine_type_code_old int2;
ALTER TABLE dams.dams ADD COLUMN up_passage_type_code_old int2;

UPDATE dams.dams SET structure_type_code_old = structure_type_code;
UPDATE dams.dams SET function_code_old = function_code;
UPDATE dams.dams SET size_class_code_old = size_class_code;
UPDATE dams.dams SET dam_use_code_old = use_code;
UPDATE dams.dams SET operating_status_code_old = operating_status_code;
UPDATE dams.dams SET ownership_type_code_old = ownership_type_code;
UPDATE dams.dams SET spillway_type_code_old = spillway_type_code;
UPDATE dams.dams SET turbine_type_code_old = turbine_type_code;
UPDATE dams.dams SET up_passage_type_code_old = up_passage_type_code;

ALTER TABLE fishways.fishways ADD COLUMN fishpass_type_code_old int2;
UPDATE fishways.fishways SET fishpass_type_code_old = fishpass_type_code;

-- change a number of codes for 'Unknown' to code 99 and
-- reshuffle values where needed

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

UPDATE dams.dams SET use_code = NULL WHERE use_code IN (10, 11, 12);

UPDATE dams.dam_use_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown') 
WHERE dam_use_code_old = 11;

UPDATE dams.dam_use_codes
SET code = 11 WHERE name_en = 'Other';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Other')
WHERE dam_use_code_old = 10;

UPDATE dams.dam_use_codes
SET code = 10 WHERE name_en = 'Wildlife Conservation';

UPDATE dams.dams
SET use_code = (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Wildlife Conservation')
WHERE dam_use_code_old = 12;

--------------------------------------------
-- operating status
--------------------------------------------

UPDATE dams.dams SET operating_status_code = NULL WHERE operating_status_code IN (5,6);

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
WHERE operating_status_code_old = 5;

UPDATE dams.operating_status_codes
SET code = 5 WHERE name_en = 'Remediated';

UPDATE dams.dams
SET operating_status_code = (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Remediated') 
WHERE operating_status_code_old = 6;

--------------------------------------------
-- ownership type
--------------------------------------------

UPDATE dams.dams SET ownership_type_code = NULL WHERE ownership_type_code IN (7,8);

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
WHERE ownership_type_code_old = 7;

UPDATE cabd.barrier_ownership_type_codes
SET code = 7 WHERE name_en = 'Indigenous';

UPDATE dams.dams
SET ownership_type_code = (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Indigenous') 
WHERE ownership_type_code_old = 8;

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

UPDATE dams.dams SET turbine_type_code = NULL WHERE turbine_type_code IN (5,6);

UPDATE dams.turbine_type_codes
SET code = 99 WHERE name_en = 'Unknown';

UPDATE dams.dams
SET turbine_type_code = (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Unknown') 
WHERE turbine_type_code_old = 5;

UPDATE dams.turbine_type_codes
SET code = 5 WHERE name_en = 'Other';

UPDATE dams.dams
SET turbine_type_code = (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Other') 
WHERE turbine_type_code_old = 6;

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
-- function
--------------------------------------------

UPDATE dams.dams SET function_code = NULL WHERE function_code > 3;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
WHERE function_code_old IN (4,5);

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Saddle')
WHERE function_code_old = 6;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Closed-cycle pumped storage')
WHERE function_code_old = 7;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Conventional storage')
WHERE function_code_old = 8;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Open-cycle pumped storage')
WHERE function_code_old = 9;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Run-of-river')
WHERE function_code_old = 10;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Tidal')
WHERE function_code_old = 11;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Other')
WHERE function_code_old = 12;

UPDATE dams.dams
SET function_code = (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
WHERE function_code_old = 13;

--------------------------------------------
-- structure type
--------------------------------------------

UPDATE dams.dams SET structure_type_code = NULL WHERE structure_type_code > 5;

UPDATE dams.dams
SET structure_type_code = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown')
WHERE structure_type_code_old = 9;

UPDATE dams.dams
SET structure_type_code = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Other')
WHERE structure_type_code_old = 10;

--------------------------------------------
-- re-enable triggers
--------------------------------------------

ALTER TABLE dams.dams ENABLE TRIGGER ALL;
ALTER TABLE dams.structure_type_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.function_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.size_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.dam_use_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.operating_status_codes ENABLE TRIGGER ALL;
ALTER TABLE cabd.barrier_ownership_type_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.spillway_type_codes ENABLE TRIGGER ALL;
ALTER TABLE dams.turbine_type_codes ENABLE TRIGGER ALL;
ALTER TABLE cabd.upstream_passage_type_codes ENABLE TRIGGER ALL;