################################################################################################

# this script updates structure type, and construction material values for dams
# where the existing values come from the data source specified on line 12
# the new values will match the updated codes and values
# as defined on the CABD documentation site as part of version 1.1 of the dams dataset

################################################################################################

import change_values_main as main

script = main.MappingScript("qmelcc_repbarrages")

updatequery = f"""
-------------------------------------------
--structure type and construction material
-------------------------------------------

CREATE TABLE dams.temp AS (
    with features AS (
        SELECT "numéro_barrage", nom_du_barrage, type_de_barrage FROM source_data.{script.datasetname}
    ),

    sources AS (
        SELECT cabd_id, datasource_id, datasource_feature_id FROM {script.damFeatureTable}
        WHERE datasource_id = (SELECT id FROM {script.dataSourceTable} WHERE name = '{script.datasetname}')
        AND datasource_feature_id IN (SELECT "numéro_barrage"::varchar FROM features)
    ),

    original AS (
        SELECT s.cabd_id, f."numéro_barrage", s.datasource_feature_id, f.type_de_barrage
        FROM sources s
            JOIN features f ON f."numéro_barrage"::varchar = s.datasource_feature_id
    )

    SELECT d.cabd_id, d.dam_name_fr, o.type_de_barrage, d.structure_type_code, a.structure_type_code_ds, d.construction_material_code
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
    WHEN "type_de_barrage" = 'Béton-gravité' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN "type_de_barrage" = 'Béton-gravité remblayé' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN "type_de_barrage" = 'Béton-voûte' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Arch')
    WHEN "type_de_barrage" = 'Caissons de bois remplis de pierres' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Caissons de bois remplis de terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de pierres' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Contreforts de bois (caissons)' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN "type_de_barrage" = 'Contreforts de bois (chandelles)' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN "type_de_barrage" = 'Contreforts de béton' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN "type_de_barrage" = 'Déversoir libre - carapace de béton' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Weir')
    WHEN "type_de_barrage" = 'Déversoir libre en enrochement' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Weir')
    WHEN "type_de_barrage" = 'Écran de béton à l''amont d''une digue de terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Écran de palplanches en acier à l''amont d''une digue de terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Enrochement' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN "type_de_barrage" = 'Enrochement - masque amont de béton' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN "type_de_barrage" = 'Enrochement - masque amont de terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN "type_de_barrage" = 'Enrochement - zoné (noyau)' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN "type_de_barrage" = 'Enrochement - zoné (écran d''étanchéité)' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN "type_de_barrage" = 'Palplanches en acier' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN "type_de_barrage" = 'Terre' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    ELSE NULL END;

UPDATE dams.temp SET construction_material_new =
    CASE
    WHEN "type_de_barrage" = 'Béton-gravité' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Béton-gravité remblayé' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Béton-voûte' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Caissons de bois remplis de pierres' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Caissons de bois remplis de terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Earth')
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de pierres' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Earth')
    WHEN "type_de_barrage" = 'Contreforts de bois (caissons)' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Timber')
    WHEN "type_de_barrage" = 'Contreforts de bois (chandelles)' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Timber')
    WHEN "type_de_barrage" = 'Contreforts de béton' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Déversoir libre - carapace de béton' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Déversoir libre en enrochement' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Écran de béton à l''amont d''une digue de terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Concrete')
    WHEN "type_de_barrage" = 'Écran de palplanches en acier à l''amont d''une digue de terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Steel')
    WHEN "type_de_barrage" = 'Enrochement' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Enrochement - masque amont de béton' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Enrochement - masque amont de terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Enrochement - zoné (noyau)' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Enrochement - zoné (écran d''étanchéité)' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Rock')
    WHEN "type_de_barrage" = 'Palplanches en acier' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Steel')
    WHEN "type_de_barrage" = 'Terre' THEN (SELECT code FROM dams.construction_material_codes WHERE name_en = 'Earth')
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