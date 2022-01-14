import LOAD_main as main

script = main.LoadingScript("cehq")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = "numéro_barrage";
UPDATE {script.sourceTable} SET data_source = '217bf7db-be4d-4f86-9e53-a1a6499da46a';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        "nom_du_barrage",
        "municipalité",
        "nom_du_réservoir",
        "lac",
        "utilisation",
        "hauteur_du_barrage_m",
        "type_de_barrage",
        "année_construction",
        "capacité_de_retenue_m3",
        "longueur_m",
        "sup_réservoir_ha",
        "propriétaire_mandataire",
        "année_prévue_étude",
        "année_dernière_étude",
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN municipality varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_name_fr varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN maintenance_next date;
ALTER TABLE {script.damWorkingTable} ADD COLUMN maintenance_last date;

UPDATE {script.damWorkingTable} SET dam_name_fr = "nom_du_barrage";
UPDATE {script.damWorkingTable} SET municipality = "municipalité";
UPDATE {script.damWorkingTable} SET reservoir_name_fr = "nom_du_réservoir";
UPDATE {script.damWorkingTable} SET reservoir_present =
    CASE 
    WHEN reservoir_name_fr IS NOT NULL THEN TRUE
    ELSE FALSE END;
UPDATE {script.damWorkingTable} SET waterbody_name_fr = "lac";
UPDATE {script.damWorkingTable} SET use_code = 
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
UPDATE {script.damWorkingTable} SET height_m = "hauteur_du_barrage_m";
UPDATE {script.damWorkingTable} SET length_m = "longueur_m";
UPDATE {script.damWorkingTable} SET construction_type_code =
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
    WHEN "type_de_barrage" = 'Écran de béton à l''amont d''une digue de terre' THEN 10
    WHEN "type_de_barrage" = 'Écran de palplanches en acier à l''amont d''une digue de terre' THEN 7
    WHEN "type_de_barrage" = 'Enrochement' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - masque amont de béton' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - masque amont de terre' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - zoné (noyau)' THEN 3
    WHEN "type_de_barrage" = 'Enrochement - zoné (écran d''étanchéité)' THEN 3 
    WHEN "type_de_barrage" = 'Palplanches en acier' THEN 7
    WHEN "type_de_barrage" = 'Terre' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET construction_year = "année_construction";
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = ("capacité_de_retenue_m3"/1000000)::float8;
UPDATE {script.damWorkingTable} SET reservoir_area_skm = ("sup_réservoir_ha"/100)::double precision;
UPDATE {script.damWorkingTable} SET "owner" = "propriétaire_mandataire";
UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN 
        ((regexp_match("owner", '(?i)(association)|(club)|(fondation)|(conservation)|(comité)|(congrégation)|(église)|(école)|(Frères de)') IS NOT NULL)
        AND (regexp_match("owner", '(?i)(coop club)|(club de golf)|(club link)') IS NULL))
        OR "owner" IN (
            'Base plein air de Bellefeuille',
            'Camps Odyssée inc.',
            'Canards Illimités Canada',
            'Centre écologique de Port-au-Saumon',
            'Centre Intercommunautaire Quatre Saisons inc.',
            'Centre Robert Piché / Elphège Roussel',
            'Chambre de commerce régionale de Saint-Raymond',
            'Communauté monastique orthodoxe de la protection de la mère de Dieu',
            'Corporation des résidants du Lac Noir inc.',
            'Corporation des Trois Lacs Sainte-Sophie inc.',
            'Corporation d''exploitation des ressources fauniques Vallée de La Matapédia (C.E.R.F.)',
            'Corporation du Vieux Moulin d''Ulverton inc.',
            'Corporation honorifique agréée propriétaire des établissements ancestraux d''Upton (C.H.A.P.E.A.U.)',
            'Corporation Ours Noir inc.',
            'Domaine Batchelder inc.',
            'Domaine des Lacs Bury inc.',
            'Domaine du lac Martin inc.',
            'Domaine Lemay Inc.',
            'Domaine Sportif Rémi-Marco inc.',
            'Espaces Jeunesse inc.',
            'Fabrique de la Paroisse de St-Joseph de Soulanges',
            'Familia St-Jérôme inc.',
            'Fédération canadienne polonaise du bien-être inc.',
            'Fonds Jules-Ledoux',
            'Foyer Wales',
            'Héritage Charlevoix inc.',
            'La Communauté des Soeurs de Charité de la Providence',
            'La Corporation Cité-Joie inc.',
            'La Corporation des Pères Maristes',
            'La Corporation des Propriétaires du Lac Lemieux (Jonquière) inc.',
            'La Corporation des Résidents des Lacs Larivée inc.',
            'La Corporation Maurice Hudon-Beaulieu',
            'La Plage du Vieux Moulin inc.',
            'La Société polonaise-canadienne d''aide mutuelle',
            'Les Filles de la Charité du Sacré-Coeur de Jésus',
            'L''héritage canadien du Québec',
            'Oeuvres Josaphat-Vanier',
            'Regroupement des Propriétaires du Domaine Richer',
            'Société Provancher d''histoire naturelle du Canada',
            'Société de gestion des ressources du Bas-St-Laurent',
            'Société d''Exploitation de la Centrale de Traitement d''Eau Chambly-Marieville-Richelieu',
            'Union canadienne des moniales de l''ordre de Sainte-Ursule'
        )
        THEN 1
    WHEN 
        "owner" IN ('Ministère de l''Environnement du Canada', 'Parcs Canada')
        THEN 2
    WHEN 
        ((regexp_match("owner", '(?i)(canton)|(municip)|(ville d)|(village)|(régie)') IS NOT NULL)
        AND "owner" <> 'Le Village Canadien de Earle Moore inc.')
        OR "owner" IN ('Corporation d''Aménagement et de Développement de la Doré inc.', 'Ville Saint-Sauveur')
        THEN 3
    WHEN 
        ((regexp_match("owner", '(?i)(Canada Inc)|(Québec inc)|(camp)|(club de golf)|(coop)|(Domaine)|(Développement)|(hydro)|(Société)') IS NOT NULL)
        AND "owner" NOT IN (
            'Hydro-Québec',
            'Société des établissements de plein air du Québec'
            ))
        OR (regexp_match("owner", '(?i)(Algonquin Power)|(Bromont)|(Compagnie)|(Ferme)|(Domtar)|(Entreprise)|(Rio Tinto)') IS NOT NULL)
        OR (regexp_match("owner", '(?i)(Malarctic)|(Mining)|(Club Link)|(Bois-Francs)|(gestion)|(Glencore)|(Iamgold)|(Inc.)') IS NOT NULL)
        THEN 4
    WHEN 
        "owner" IN (
            'Agence du revenu du Québec',
            'Centre intégré de Santé et de Services sociaux des Laurentides',
            'Direction générale des barrages (Ministère de l''Environnement et de la Lutte contre les changements climatiques)',
            'Hydro-Québec',
            'Ministère de l''Énergie et des Ressources naturelles',
            'Ministère de l''Environnement et de la Lutte contre les changements climatiques',
            'Ministère des Forêts, de la Faune et des Parcs',
            'Ministère des Transports',
            'Ontario Power Generation',
            'Société des établissements de plein air du Québec'
        )
        THEN 5
    WHEN 
        (regexp_match("owner", '(?i)(Innu)|(Fiducie)|(Univers)') IS NOT NULL)
        OR "owner" IN (
            'Coop Club Alcaniens',
            'Cric à David 1985 inc.',
            'Les Apôtres de l''Amour infini',
            'Les Apôtres de l''Amour Infini Canada',
            'Les Brebis de Jésus',
            'Les Filles de Jésus (Trois-Rivières)',
            'Les Marianistes de Saint-Anselme',
            'Les Prêtres de Saint-Sulpice de Montréal',
            'Les Religieux de Saint-Vincent de Paul (Canada)',
            'Les Soeurs de l''Assomption de la Sainte Vierge',
            'Les Soeurs du Bon-Pasteur de Québec',
            'Oeuvres Rivat',
            'Ordre de Saint-Antoine Le Grand',
            'Paroisse de Saint-Marc-du-Lac-Long',
            'Séminaire de Québec',
            'Syndicat Aqueduc Grand-Rang',
            'The St-Raymond Trust',
            'The Trustee Board of the Presbyterian Church in Canada',
            'Thomas Miles in Trust Kee Myong Hong'
        )
        THEN 6
    WHEN 
        "owner" = 'Personne physique'
        THEN 7
    ELSE NULL END;

UPDATE {script.damWorkingTable} SET maintenance_next = ('01-01-' || "année_prévue_étude")::date;
UPDATE {script.damWorkingTable} SET maintenance_last = ('01-01-' || "année_dernière_étude")::date;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN "nom_du_barrage",
    DROP COLUMN "municipalité",
    DROP COLUMN "nom_du_réservoir",
    DROP COLUMN "lac",
    DROP COLUMN "utilisation",
    DROP COLUMN "hauteur_du_barrage_m",
    DROP COLUMN "type_de_barrage",
    DROP COLUMN "année_construction",
    DROP COLUMN "capacité_de_retenue_m3",
    DROP COLUMN "longueur_m",
    DROP COLUMN "sup_réservoir_ha",
    DROP COLUMN "propriétaire_mandataire",
    DROP COLUMN "année_prévue_étude",
    DROP COLUMN "année_dernière_étude";

"""

script.do_work(query)