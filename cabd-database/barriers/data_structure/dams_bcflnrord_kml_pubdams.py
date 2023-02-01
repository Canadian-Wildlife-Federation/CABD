################################################################################################

# this script updates structure type and construction material values for dams
# where the existing values come from the data source specified on line 12
# the new values will match the updated codes and values
# as defined on the CABD documentation site as part of version 1.1 of the dams dataset

################################################################################################

import change_values_main as main

script = main.MappingScript("bcflnrord_kml_pubdams")

updatequery = f"""

-------------------------------------------
--structure type and construction material
-------------------------------------------

CREATE TABLE dams.temp AS (
    with features AS (
        SELECT objectid, dam_name, dam_type FROM source_data.{script.datasetname}
    ),

    sources AS (
        SELECT cabd_id, datasource_id, datasource_feature_id FROM {script.damFeatureTable}
        WHERE datasource_id = (SELECT id FROM {script.dataSourceTable} WHERE name = '{script.datasetname}')
        AND datasource_feature_id IN (SELECT objectid::varchar FROM features)
    ),

    original AS (
        SELECT s.cabd_id, f.objectid, s.datasource_feature_id, f.dam_type
        FROM sources s
            JOIN features f ON f.objectid::varchar = s.datasource_feature_id
    )

    SELECT d.cabd_id, d.dam_name_en, o.dam_type, d.structure_type_code, a.structure_type_code_ds, d.construction_material_code
    FROM {script.damTable} d
        JOIN {script.damAttributeTable} a ON a.cabd_id = d.cabd_id
        JOIN original o ON o.cabd_id = d.cabd_id
    WHERE a.structure_type_code_ds = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}')
    AND d.cabd_id IN (SELECT cabd_id FROM sources)
);

ALTER TABLE dams.temp ADD COLUMN structure_type_new int2;
ALTER TABLE dams.temp ADD COLUMN construction_material_new int2;

UPDATE dams.temp SET structure_type_new = 
    CASE
    WHEN dam_type = 'Concrete Arch' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Arch')
    WHEN dam_type = 'Buttress' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN dam_type = 'Earthfill' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN dam_type = 'Concrete Gravity' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN dam_type = 'Rockfill' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN dam_type = 'Steel' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN dam_type = 'Log Crib' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN dam_type IN ('Concrete', 'Dugout/Pond', 'Other') THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    ELSE NULL END;

UPDATE dams.temp SET construction_material_new =
    CASE
    WHEN dam_type = 'Concrete Arch' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN dam_type = 'Earthfill' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Earth')
    WHEN dam_type = 'Concrete Gravity' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN dam_type = 'Rockfill' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN dam_type = 'Steel' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Steel')
    WHEN dam_type = 'Log Crib' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Timber')
    WHEN dam_type = 'Concrete' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    ELSE NULL END;

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

UPDATE
    {script.damAttributeTable}
SET
    construction_material_code_ds = (SELECT id FROM {script.dataSourceTable} WHERE name = '{script.datasetname}')
WHERE
    cabd_id IN (
        SELECT cabd_id FROM {script.damSchema}.temp
        WHERE construction_material_new IS NOT NULL
    );

UPDATE
    {script.damTable} d
SET
    construction_material_code = t.construction_material_new
FROM
    {script.damSchema}.temp t
WHERE
    d.cabd_id = t.cabd_id
    AND t.construction_material_new IS NOT NULL
    AND d.cabd_id IN (
    SELECT cabd_id FROM {script.damSchema}.temp);

DROP TABLE {script.damSchema}.temp;

"""

script.do_work(updatequery)