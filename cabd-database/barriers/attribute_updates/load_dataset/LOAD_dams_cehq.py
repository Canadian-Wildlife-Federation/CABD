import LOAD_dams_main as main

script = main.DamLoadingScript("cehq")
    
query = f"""
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
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

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
UPDATE {script.tempTable} SET data_source = 'cehq_' || "numéro_barrage";

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
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
    data_source text PRIMARY KEY
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
    data_source
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
    data_source
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
	cehq.data_source = duplicates.data_source
	OR cehq.data_source = duplicates.dups_cehq;
    """

script.do_work(query)