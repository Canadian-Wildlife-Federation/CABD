import LOAD_dams_main as main

script = main.DamLoadingScript("cehq")
    
query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = "numéro_barrage";
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_name_fr varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN maintenance_next date;
ALTER TABLE {script.tempTable} ADD COLUMN maintenance_last date;

UPDATE {script.tempTable} SET dam_name_fr = "numéro_barrage";
UPDATE {script.tempTable} SET nearest_municipality = "municipalité";
UPDATE {script.tempTable} SET reservoir_name_fr = "nom_du_réservoir";
UPDATE {script.tempTable} SET reservoir_present =
    CASE 
    WHEN reservoir_name_fr IS NOT NULL THEN TRUE 
    ELSE FALSE END;
UPDATE {script.tempTable} SET waterbody_name_fr = "lac";
UPDATE {script.tempTable} SET use_code = 
    CASE
    WHEN "utilisation" = 'Agriculture' THEN 1
    WHEN "utilisation" = 'Anciennement flottage' THEN 10
    WHEN "utilisation" = 'Autre ou inconnu' THEN 10
    WHEN "utilisation" = 'Bassin de rétention' THEN 10
    WHEN "utilisation" = 'Cannebergière' THEN 10
    WHEN "utilisation" = 'Contrôle des inondations' THEN 4
    WHEN "utilisation" = 'Faune' THEN 7
    WHEN "utilisation" = 'Fins environnementales' THEN 10
    WHEN "utilisation" = 'Hydroélectricité' THEN 2
    WHEN "utilisation" = 'Ouvrage minier - site en exploitation' THEN 10
    WHEN "utilisation" = 'Ouvrage minier- ancien site minier' THEN 10
    WHEN "utilisation" = 'Pisciculture' THEN 7
    WHEN "utilisation" = 'Prise d''eau' THEN 3
    WHEN "utilisation" = 'Prise d''eau (autres)' THEN 3
    WHEN "utilisation" = 'Prise d''eau (municipale)' THEN 3
    WHEN "utilisation" = 'Récréatif et villégiature' THEN 5
    WHEN "utilisation" = 'Régularisation' THEN 10
    WHEN "utilisation" = 'Réserve incendie' THEN 3
    WHEN "utilisation" = 'Site historique' THEN 5
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = "hauteur_du_barrage_m";
UPDATE {script.tempTable} SET construction_type_code =
    CASE
    WHEN "type_de_barrage" = 'Béton-gravité' THEN 4
    WHEN "type_de_barrage" = 'Béton-gravité remblayé' THEN 4
    WHEN "type_de_barrage" = 'Béton-voûte' THEN 1
    WHEN "type_de_barrage" = 'Caissons de bois remplis de pierres' THEN 8
    WHEN "type_de_barrage" = 'Caissons de bois remplis de terre' THEN 8
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de pierres' THEN 7
    WHEN "type_de_barrage" = 'Caissons de palplanches en acier remplis de terre' THEN 7
    WHEN "type_de_barrage" = 'Contreforts de bois (caissons)' THEN 8
    WHEN "type_de_barrage" = 'Contreforts de bois (chandelles)' THEN 8
    WHEN "type_de_barrage" = 'Contreforts de béton' THEN 2
    WHEN "type_de_barrage" = 'Déversoir libre - carapace de béton' THEN 10
    WHEN "type_de_barrage" = 'Déversoir libre en enrochement' THEN 6
    WHEN "type_de_barrage" = 'Enrochement' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - masque amont de béton' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - masque amont de terre' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - zoné (noyau)' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - zoné (écran d''étanchéité)' THEN 3 
    WHEN "type_de_barrage" = 'Palplanches en acier' THEN 7
    WHEN "type_de_barrage" = 'Terre' THEN 3
    WHEN "type_de_barrage" = 'Écran de béton à l''amont d''une digue de terre' THEN 10
    WHEN "type_de_barrage" = 'Écran de palplanches en acier à l''amont d''une digue de terre' THEN 7
    ELSE NULL END;
UPDATE {script.tempTable} SET construction_year = "année_construction";
UPDATE {script.tempTable} SET storage_capacity_mcm = ("capacité_de_retenue_m3"/1000000)::float8;
UPDATE {script.tempTable} SET length_m = "longueur_m";
UPDATE {script.tempTable} SET reservoir_area_skm = ("sup_réservoir_ha"/100)::double precision;
UPDATE {script.tempTable} SET "owner" = "propriétaire_mandataire";
UPDATE {script.tempTable} SET maintenance_next = ('01-01-' || "année_prévue_étude")::date;
UPDATE {script.tempTable} SET maintenance_last = ('01-01-' || "année_dernière_étude")::date;


ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_fr varchar(512),
    nearest_municipality varchar(512),
    reservoir_name_fr varchar(512),
    reservoir_present bool,
    waterbody_name_fr varchar(512),
    use_code int2,
    height_m float4,
    construction_type_code int2,
    construction_year numeric,
    storage_capacity_mcm float8,
    length_m float4,
    reservoir_area_skm float4,
    "owner" varchar(512),
    maintenance_next date,
    maintenance_last date,
    duplicate_id varchar,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_fr,
    nearest_municipality,
    reservoir_name_fr,
    reservoir_present,
    waterbody_name_fr,
    use_code,
    height_m,
    construction_type_code,
    construction_year,
    storage_capacity_mcm,
    length_m,
    reservoir_area_skm,
    "owner",
    maintenance_next,
    maintenance_last,
    duplicate_id,
    data_source,
    data_source_id
)
SELECT
    dam_name_fr,
    nearest_municipality,
    reservoir_name_fr,
    reservoir_present,
    waterbody_name_fr,
    use_code,
    height_m,
    construction_type_code,
    construction_year,
    storage_capacity_mcm,
    length_m,
    reservoir_area_skm,
    "owner",
    maintenance_next,
    maintenance_last,
    'cehq_' || data_source_id,
    data_source
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_fr,
    DROP COLUMN nearest_municipality,
    DROP COLUMN reservoir_name_fr,
    DROP COLUMN reservoir_present,
    DROP COLUMN waterbody_name_fr,
    DROP COLUMN use_code,
    DROP COLUMN height_m,
    DROP COLUMN construction_type_code,
    DROP COLUMN construction_year,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN length_m,
    DROP COLUMN reservoir_area_skm,
    DROP COLUMN "owner",
    DROP COLUMN maintenance_next,
    DROP COLUMN maintenance_last;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS cehq
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	cehq.duplicate_id = duplicates.data_source
	OR cehq.duplicate_id = duplicates.dups_cehq;
    """


#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'cehq', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_fr = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.dam_name_fr ELSE cabd.dam_name_fr END,
    nearest_municipality = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.nearest_municipality ELSE cabd.nearest_municipality END,
    reservoir_name_fr = CASE WHEN (cabd.reservoir_name_fr IS NULL AND origin.reservoir_name_fr IS NOT NULL) THEN origin.reservoir_name_fr ELSE cabd.reservoir_name_fr END,
    reservoir_present = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.reservoir_present ELSE cabd.reservoir_present END,
    waterbody_name_fr = CASE WHEN (cabd.waterbody_name_fr IS NULL AND origin.waterbody_name_fr IS NOT NULL) THEN origin.waterbody_name_fr ELSE cabd.waterbody_name_fr END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.use_code ELSE cabd.use_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END,         
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.construction_type_code ELSE cabd.construction_type_code END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.length_m ELSE cabd.length_m END,         
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    owner = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.owner ELSE cabd.owner END,
    maintenance_next = CASE WHEN (cabd.maintenance_next IS NULL AND origin.maintenance_next IS NOT NULL) THEN origin.maintenance_next ELSE cabd.maintenance_next END,
    maintenance_last = CASE WHEN (cabd.maintenance_last IS NULL AND origin.maintenance_next IS NOT NULL) THEN origin.maintenance_last ELSE cabd.maintenance_last END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    dam_name_fr_ds = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_fr_ds END,
    nearest_municipality_ds = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.data_source ELSE cabd.nearest_municipality_ds END,
    reservoir_name_fr_ds = CASE WHEN (cabd.reservoir_name_fr IS NULL AND origin.reservoir_name_fr IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_name_fr_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_present_ds END,
    waterbody_name_fr_ds = CASE WHEN (cabd.waterbody_name_fr IS NULL AND origin.waterbody_name_fr IS NOT NULL) THEN origin.data_source ELSE cabd.waterbody_name_fr_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_code_ds END,         
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabd.height_m_ds END,         
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source ELSE cabd.construction_type_code_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabd.construction_year_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source ELSE cabd.storage_capacity_mcm_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source ELSE cabd.length_m_ds END,         
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_area_skm_ds END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source ELSE cabd.owner_ds END,
    maintenance_next_ds = CASE WHEN (cabd.maintenance_next IS NULL AND origin.maintenance_next IS NOT NULL) THEN origin.data_source ELSE cabd.maintenance_next_ds END,
    maintenance_last_ds = CASE WHEN (cabd.maintenance_last IS NULL AND origin.maintenance_last IS NOT NULL) THEN origin.data_source ELSE cabd.maintenance_last_ds END,
    
    dam_name_fr_dsfid = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_fr_dsfid END,
    nearest_municipality_dsfid = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.data_source_id ELSE cabd.nearest_municipality_dsfid END,
    reservoir_name_fr_dsfid = CASE WHEN (cabd.reservoir_name_fr IS NULL AND origin.reservoir_name_fr IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_name_fr_dsfid END,
    reservoir_present_dsfid = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_present_dsfid END,
    waterbody_name_fr_dsfid = CASE WHEN (cabd.waterbody_name_fr IS NULL AND origin.waterbody_name_fr IS NOT NULL) THEN origin.data_source_id ELSE cabd.waterbody_name_fr_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_code_dsfid END,         
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.height_m_dsfid END,         
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_year_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source_id ELSE cabd.storage_capacity_mcm_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.length_m_dsfid END,         
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_area_skm_dsfid END,
    owner_dsfid = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source_id ELSE cabd.owner_dsfid END,
    maintenance_next_dsfid = CASE WHEN (cabd.maintenance_next IS NULL AND origin.maintenance_next IS NOT NULL) THEN origin.data_source_id ELSE cabd.maintenance_next_dsfid END,
    maintenance_last_dsfid = CASE WHEN (cabd.maintenance_last IS NULL AND origin.maintenance_last IS NOT NULL) THEN origin.data_source_id ELSE cabd.maintenance_last_dsfid END
        
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)