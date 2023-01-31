################################################################################################

# this script updates structure type for dams
# where the existing values come from the data source specified on line 12
# the new values will match the updated codes and values
# as defined on the CABD documentation site as part of version 1.1 of the dams dataset

# this script is different from the others as all structures represent powerhouses

################################################################################################

import change_values_main as main

script = main.MappingScript("nlp_depreciation_2019")

updatequery = f"""

-------------------------------------------
--structure type
-------------------------------------------

CREATE TABLE dams.temp AS (
    with sources AS (
        SELECT cabd_id, datasource_id, datasource_feature_id FROM {script.damFeatureTable}
        WHERE datasource_id = (SELECT id FROM {script.dataSourceTable} WHERE name = '{script.datasetname}')
    )

    SELECT d.cabd_id, d.dam_name_en, d.structure_type_code, a.structure_type_code_ds
    FROM {script.damTable} d
        JOIN {script.damAttributeTable} a ON a.cabd_id = d.cabd_id
    WHERE a.structure_type_code_ds = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}')
    AND d.cabd_id IN (SELECT cabd_id FROM sources)
);

ALTER TABLE dams.temp ADD COLUMN structure_type_new int2;

UPDATE dams.temp SET structure_type_new = (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Powerhouse');

UPDATE
    {script.damTable} d
SET
    structure_type_code = t.structure_type_new
FROM
    {script.damSchema}.temp t
WHERE
    d.cabd_id = t.cabd_id
    AND t.structure_type_new IS NOT NULL
    AND d.cabd_id IN (
    SELECT cabd_id FROM {script.damSchema}.temp);

DROP TABLE {script.damSchema}.temp;

"""

script.do_work(updatequery)