import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "cabd_dev"
dbUser = "xxxx@cabd_dev"
dbPassword = "xxxx"

#create temporary table and table to be inserted into CABD dataset
tempSchema = "source_data"
tempTableRaw = "dams_cehq_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_cehq"
workingTable = workingSchema + "." + workingTableRaw

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_cehq.py <datafile>")
    sys.exit()


print("Loading data from file " +  dataFile)


conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

query = f"""
CREATE SCHEMA IF NOT EXISTS {tempSchema};
CREATE SCHEMA IF NOT EXISTS {workingSchema};
DROP TABLE IF EXISTS {tempTable};
DROP TABLE IF EXISTS {workingTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + tempTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + '"' + dataFile + '"'
print(pycmd)
subprocess.run(pycmd)

#run scripts to convert the data
print("Running mapping queries...")
query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {tempTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {tempTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {tempTable} ADD COLUMN reservoir_name_fr varchar(512);
ALTER TABLE {tempTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {tempTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {tempTable} ADD COLUMN use_code int2;
ALTER TABLE {tempTable} ADD COLUMN height_m float4;
ALTER TABLE {tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {tempTable} ADD COLUMN length_m float4;
ALTER TABLE {tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {tempTable} ADD COLUMN maintenance_next date;
ALTER TABLE {tempTable} ADD COLUMN maintenance_last date;
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET dam_name_fr = "numéro_barrage";
UPDATE {tempTable} SET nearest_municipality = "municipalité";
UPDATE {tempTable} SET reservoir_name_fr = "nom_du_réservoir";
UPDATE {tempTable} SET reservoir_present =
    CASE 
    WHEN reservoir_name_fr IS NOT NULL THEN TRUE 
    ELSE FALSE END;
UPDATE {tempTable} SET waterbody_name_fr = "lac";
UPDATE {tempTable} SET use_code = 
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
UPDATE {tempTable} SET height_m = "hauteur_du_barrage_m";
UPDATE {tempTable} SET construction_type_code =
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
UPDATE {tempTable} SET construction_year = "année_construction";
UPDATE {tempTable} SET storage_capacity_mcm = ("capacité_de_retenue_m3"/1000000)::float8;
UPDATE {tempTable} SET length_m = "longueur_m";
UPDATE {tempTable} SET reservoir_area_skm = ("sup_réservoir_ha"/100)::double precision;
UPDATE {tempTable} SET "owner" = "propriétaire_mandataire";
UPDATE {tempTable} SET maintenance_next = ('01-01-' || "année_prévue_étude")::date;
UPDATE {tempTable} SET maintenance_last = ('01-01-' || "année_dernière_étude")::date;
UPDATE {tempTable} SET data_source = 'cehq_' || "numéro_barrage";

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
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
INSERT INTO {workingTable}(
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
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
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
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_cehq AS cehq
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	cehq.data_source = duplicates.data_source
	OR cehq.data_source = duplicates.dups_cehq;
    """

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)