-- Script contains shared tables across all barrier types

CREATE TABLE cabd.barrier_ownership_type_codes (
	code int2 NOT NULL,
	name varchar(32) NOT NULL,
	description text NULL,
	CONSTRAINT barrier_ownership_types_pk PRIMARY KEY (code)
);
COMMENT ON TABLE cabd.barrier_ownership_type_codes IS 'Reference table for the general classification of the type of barrier owner.';



CREATE TABLE cabd.fish_species (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	name varchar(128) NULL,
	common_name varchar(128) NULL,
	species_name varchar(128) NULL,
	"family" varchar(128) NULL,
	CONSTRAINT fish_species_pk PRIMARY KEY (id),
	CONSTRAINT fish_species_un UNIQUE (name)
);


CREATE TABLE cabd.province_territory_codes (
	code varchar(2) NOT NULL, -- Character code for province or Territory.
	name varchar(32) NOT NULL, -- Province or Territory name.
	geometry geometry(MULTIPOLYGON, 4326),
	CONSTRAINT province_territory_pk PRIMARY KEY (code)
);
COMMENT ON TABLE cabd.province_territory_codes IS 'Reference table for Province or Territory codes.';
COMMENT ON COLUMN cabd.province_territory_codes.code IS 'Character code for province or Territory.';
COMMENT ON COLUMN cabd.province_territory_codes.name IS 'Province or Territory name.';



CREATE TABLE cabd.upstream_passage_type_codes (

	code int2 NOT NULL, -- Code referencing the upstream fish passage measure type.
	name varchar(32) NOT NULL, -- Type of upstream fish passage measure associated with barrier structures.
	description text NULL,
	CONSTRAINT upstream_passage_types_pk PRIMARY KEY (code)
);
COMMENT ON TABLE cabd.upstream_passage_type_codes IS 'Reference table for the types of upstream fish passage measures associated with barrier structures.';
COMMENT ON COLUMN cabd.upstream_passage_type_codes.code IS 'Code referencing the upstream fish passage measure type.';
COMMENT ON COLUMN cabd.upstream_passage_type_codes.name IS 'Type of upstream fish passage measure associated with barrier structures.';


CREATE TABLE cabd.watershed_groups (
	code varchar(32) NOT NULL,
	name varchar(512) NOT NULL,
	polygon geometry(MULTIPOLYGON, 3857) NULL,
	CONSTRAINT watershed_groups_pkey PRIMARY KEY (code)
);
CREATE INDEX watershed_groups_polygon_idx ON cabd.watershed_groups USING gist (polygon);


-- Barrier type configurations
CREATE TABLE cabd.feature_type_metadata (
	view_name varchar NOT NULL, -- view name
	field_name varchar NOT NULL, -- field name
	name varchar NULL, -- human friendly name of field
	description varchar NULL, -- description of field
	is_link bool NOT NULL DEFAULT false,
	CONSTRAINT barrier_type_metadata_pk PRIMARY KEY (view_name, field_name)
);
COMMENT ON TABLE cabd.feature_type_metadata IS 'A table that maps view field names to human friendly field names.';

COMMENT ON COLUMN cabd.feature_type_metadata.view_name IS 'view name';
COMMENT ON COLUMN cabd.feature_type_metadata.field_name IS 'field name';
COMMENT ON COLUMN cabd.feature_type_metadata.name IS 'human friendly name of field';
COMMENT ON COLUMN cabd.feature_type_metadata.description IS 'description of field';

CREATE TABLE cabd.feature_types (
	"type" varchar(32) NOT NULL, -- Unique key representing feature type.
	data_view varchar NOT NULL, -- Database view for querying feature dataset
	CONSTRAINT barrier_types_pk PRIMARY KEY (type)
);
COMMENT ON TABLE cabd.feature_types IS 'Table that contains all supported feature types and links to the view describing the feature.';
COMMENT ON COLUMN cabd.feature_types."type" IS 'Unique key representing feature type.';
COMMENT ON COLUMN cabd.feature_types.data_view IS 'Database view for querying feature dataset';



INSERT INTO cabd.province_territory_codes (code,"name") VALUES
	 ('ab','Alberta'),
	 ('bc','British Columbia'),
	 ('mb','Manitoba'),
	 ('nb','New Brunswick'),
	 ('nl','Newfoundland and Labrador'),
	 ('ns','Nova Scotia'),
	 ('nt','Northwest Territories'),
	 ('nu','Nunavut'),
	 ('on','Ontario'),
	 ('pe','Prince Edward Island'),
	 ('sk','Saskatchewan'),
	 ('yt','Yukon'),
	 ('qc','Quebec');
 
INSERT INTO cabd.upstream_passage_type_codes (code,"name",description) VALUES
	 (1,'Denil','A series of close-spaced baffles placed on the bottom and/or sides of an inclined (up to 20%) channel, to redirect the flow of water and reduce its velocity, allowing fish to ascend and migrate upstream.'),
	 (2,'Nature-like fishway','A diversion channel excavated along the bank of the river, allowing fish to bypass the barrier structure.'),
	 (3,'Pool and weir','A series of small overflow pools and weirs forming steps that fish can jump between, migrating upstream.'),
	 (4,'Pool and weir with hole','A series of small overflow pools and weirs provided with submerged holes in which fish can jump between, migrating upstream.'),
	 (5,'Trap and truck','Fish are trapped and transported upstream of the barrier where they are released.'),
	 (6,'Vertical slot','A variation of pool and weir fishways where weirs are replaced by walls with vertical slots in which fish can pass through, allowing fish to swim at their desired depth.'),
	 (7,'Other',NULL),
	 (8,'No structure','No fish passage structure is present.'),
	 (9,'Unknown', NULL);
	 
INSERT INTO cabd.barrier_ownership_type_codes (code,"name",description) VALUES
	 (1,'Charity/Non-profit','Privately held entities that do not provide financial benefits to their members or organizations; independent from any government.'),
	 (2,'Federal','National government organization (i.e., department, agency, crown corporation) that performs national level regulatory and administrative functions.'),
	 (3,'Municipal','Local governing body that provides services, facilities, safety, and infrastructure for communities.'),
	 (4,'Private','Organization, corporation, or partnership operated for profit that is not public or controlled by one or more public corporations. '),
	 (5,'Provincial/Territorial','Regional government organization with jurisdiction over their specific regional boundaries.'),
	 (6,'Other',NULL);	 
	 
	 
INSERT INTO cabd.fish_species (id,"name",common_name,species_name,"family") VALUES
	 (uuid_generate_v4(),'Sea lamprey (Petromyzon marinus)','Sea lamprey','Petromyzon marinus','Petromyzontidae'),
	 (uuid_generate_v4(),'Pacific lamprey (Lampetra tridentata)','Pacific lamprey','Lampetra tridentata','Petromyzontidae'),
	 (uuid_generate_v4(),'Atlantic sturgeon (Acipenser oxyrhinchus)','Atlantic sturgeon','Acipenser oxyrhinchus','Acipenseridae'),
	 (uuid_generate_v4(),'Green sturgeon (Acipenser medirostris)','Green sturgeon','Acipenser medirostris','Acipenseridae'),
	 (uuid_generate_v4(),'Lake sturgeon (Acipenser fulvescens)','Lake sturgeon','Acipenser fulvescens','Acipenseridae'),
	 (uuid_generate_v4(),'Shortnose sturgeon (Acipenser brevirostrum)','Shortnose sturgeon','Acipenser brevirostrum','Acipenseridae'),
	 (uuid_generate_v4(),'Spotted gar (Lepisosteus oculatus)','Spotted gar','Lepisosteus oculatus','Acipenseridae'),
	 (uuid_generate_v4(),'Longnose gar (Lepisosteus osseus)','Longnose gar','Lepisosteus osseus','Acipenseridae'),
	 (uuid_generate_v4(),'American Shad (Alosa sapidissima)','American Shad','Alosa sapidissima','Clupeidae'),
	 (uuid_generate_v4(),'Alewife (Alosa pseudoharengus)','Alewife','Alosa pseudoharengus','Clupeidae'),
	 (uuid_generate_v4(),'Cutthroat trout (Oncorhyncus clarkii)','Cutthroat trout','Oncorhyncus clarkii','Salmonidae'),
	 (uuid_generate_v4(),'Rainbow/steelhead trout (Oncorhynchus mykiss)','Rainbow/steelhead trout','Oncorhynchus mykiss','Salmonidae'),
	 (uuid_generate_v4(),'Atlantic salmon (Salmo salar)','Atlantic salmon','Salmo salar','Salmonidae'),
	 (uuid_generate_v4(),'Brook trout (Salvelinus fontinalis)','Brook trout','Salvelinus fontinalis','Salmonidae'),
	 (uuid_generate_v4(),'Arctic char (Salvelinus alpinus)','Arctic char','Salvelinus alpinus','Salmonidae'),
	 (uuid_generate_v4(),'Pink salmon (Oncorhynchus gorbuscha)','Pink salmon','Oncorhynchus gorbuscha','Salmonidae'),
	 (uuid_generate_v4(),'Chinook salmon (Oncorhynchus tshawytscha)','Chinook salmon','Oncorhynchus tshawytscha','Salmonidae'),
	 (uuid_generate_v4(),'Coho salmon (Oncorhynchus kisutch)','Coho salmon','Oncorhynchus kisutch','Salmonidae'),
	 (uuid_generate_v4(),'Sockeye/Kokanee salmon (Oncoryhnchus nerka)','Sockeye/Kokanee salmon','Oncoryhnchus nerka','Salmonidae'),
	 (uuid_generate_v4(),'Chum salmon (Oncorhynchus keta)','Chum salmon','Oncorhynchus keta','Salmonidae'),
	 (uuid_generate_v4(),'Central mudminnow (Umbra limi)','Central mudminnow','Umbra limi','Umbridae'),
	 (uuid_generate_v4(),'Muskellunge (Esox masquinongy)','Muskellunge','Esox masquinongy','Esocidae'),
	 (uuid_generate_v4(),'Northern pike (Esox lucius)','Northern pike','Esox lucius','Esocidae'),
	 (uuid_generate_v4(),'Chain pickerel (Esox niger)','Chain pickerel','Esox niger','Esocidae'),
	 (uuid_generate_v4(),'Common carp (Cyprinus carpio)','Common carp','Cyprinus carpio','Cyprinidae'),
	 (uuid_generate_v4(),'Longnose dace (Rhinichthys cataractae)','Longnose dace','Rhinichthys cataractae','Cyprinidae'),
	 (uuid_generate_v4(),'Blacknose dace (Rhinichthys atratulus)','Blacknose dace','Rhinichthys atratulus','Cyprinidae'),
	 (uuid_generate_v4(),'Flathead chub (Platygobio gracilis)','Flathead chub','Platygobio gracilis','Cyprinidae'),
	 (uuid_generate_v4(),'Leopard dace (Rhinichthys falcatus)','Leopard dace','Rhinichthys falcatus','Cyprinidae'),
	 (uuid_generate_v4(),'Hornyhead chub (Nocomis biguttatus)','Hornyhead chub','Nocomis biguttatus','Cyprinidae'),
	 (uuid_generate_v4(),'River chub (Nocomis micropogon)','River chub','Nocomis micropogon','Cyprinidae'),
	 (uuid_generate_v4(),'Northern redbelly dace (Phoxinus eos)','Northern redbelly dace','Phoxinus eos','Cyprinidae'),
	 (uuid_generate_v4(),'Finescale dace (Phoxinus neogaeus)','Finescale dace','Phoxinus neogaeus','Cyprinidae'),
	 (uuid_generate_v4(),'Redside shiner (Richardsonius balteatus)','Redside shiner','Richardsonius balteatus','Cyprinidae'),
	 (uuid_generate_v4(),'Redside dace (Clinostomus elongatus)','Redside dace','Clinostomus elongatus','Cyprinidae'),
	 (uuid_generate_v4(),'Creek chub (Semotilus atromaculatus)','Creek chub','Semotilus atromaculatus','Cyprinidae'),
	 (uuid_generate_v4(),'Pearl dace (Margariscus margarita)','Pearl dace','Margariscus margarita','Cyprinidae'),
	 (uuid_generate_v4(),'Golden shiner (Notemigonus crysoleucas)','Golden shiner','Notemigonus crysoleucas','Cyprinidae'),
	 (uuid_generate_v4(),'Fathead minnow (Pimephales promelas)','Fathead minnow','Pimephales promelas','Cyprinidae'),
	 (uuid_generate_v4(),'Bluntnose minnow (Pimephales notatus)','Bluntnose minnow','Pimephales notatus','Cyprinidae'),
	 (uuid_generate_v4(),'Pugnose minnow (Opsopoeodus emiliae)','Pugnose minnow','Opsopoeodus emiliae','Cyprinidae'),
	 (uuid_generate_v4(),'Common shiner (Luxilus cornutus)','Common shiner','Luxilus cornutus','Cyprinidae'),
	 (uuid_generate_v4(),'Redfin shiner (Lythrurus umbratilis)','Redfin shiner','Lythrurus umbratilis','Cyprinidae'),
	 (uuid_generate_v4(),'Ninespine stickleback (Pungitius pungitius)','Ninespine stickleback','Pungitius pungitius','Gasterosteidae'),
	 (uuid_generate_v4(),'Brook stickleback (Culaea inconstans)','Brook stickleback','Culaea inconstans','Gasterosteidae'),
	 (uuid_generate_v4(),'Threespine stickleback (Gasterosteus aculeatus)','Threespine stickleback','Gasterosteus aculeatus','Gasterosteidae'),
	 (uuid_generate_v4(),'White perch (Morone americana)','White perch','Morone americana','Moronidae'),
	 (uuid_generate_v4(),'White bass (Morone chrysops)','White bass','Morone chrysops','Moronidae'),
	 (uuid_generate_v4(),'Striped bass (Morone saxatilis)','Striped bass','Morone saxatilis','Moronidae'),
	 (uuid_generate_v4(),'Rock bass (Ambloplites rupestris)','Rock bass','Ambloplites rupestris','Centrarchidae'),
	 (uuid_generate_v4(),'Smallmouth bass (Micropterus dolomieu)','Smallmouth bass','Micropterus dolomieu','Centrarchidae'),
	 (uuid_generate_v4(),'Largemouth bass (Micropterus salmoides)','Largemouth bass','Micropterus salmoides','Centrarchidae'),
	 (uuid_generate_v4(),'Pumpkinseed sunfish (Lepomis gibbosus)','Pumpkinseed sunfish','Lepomis gibbosus','Centrarchidae'),
	 (uuid_generate_v4(),'Bluegill sunfish (Lepomis macrochirus)','Bluegill sunfish','Lepomis macrochirus','Centrarchidae'),
	 (uuid_generate_v4(),'Redbreast sunfish (Lepomis auritus)','Redbreast sunfish','Lepomis auritus','Centrarchidae'),
	 (uuid_generate_v4(),'Green sunfish (Lepomis cyanellus)','Green sunfish','Lepomis cyanellus','Centrarchidae'),
	 (uuid_generate_v4(),'Longear sunfish (Lepomis megalotis)','Longear sunfish','Lepomis megalotis','Centrarchidae'),
	 (uuid_generate_v4(),'White crappie (Pomoxis annularis)','White crappie','Pomoxis annularis','Centrarchidae'),
	 (uuid_generate_v4(),'Black crappie (Pomoxis nigromaculatus)','Black crappie','Pomoxis nigromaculatus','Centrarchidae'),
	 (uuid_generate_v4(),'Yellow perch (Perca flavescens)','Yellow perch','Perca flavescens','Percidae'),
	 (uuid_generate_v4(),'Walleye (Sander vitreus)','Walleye','Sander vitreus','Percidae'),
	 (uuid_generate_v4(),'Sauger (Sander canadensis)','Sauger','Sander canadensis','Percidae'),
	 (uuid_generate_v4(),'Eastern sand darter (Ammocrypta pellucida)','Eastern sand darter','Ammocrypta pellucida','Percidae'),
	 (uuid_generate_v4(),'Channel darter (Percina copelandi)','Channel darter','Percina copelandi','Percidae'),
	 (uuid_generate_v4(),'Logperch (Percina caprodes)','Logperch','Percina caprodes','Percidae'),
	 (uuid_generate_v4(),'River darter (Percina shumardi)','River darter','Percina shumardi','Percidae'),
	 (uuid_generate_v4(),'Blackside darter (Percina maculata)','Blackside darter','Percina maculata','Percidae'),
	 (uuid_generate_v4(),'Johnny darter (Etheostoma nigrum)','Johnny darter','Etheostoma nigrum','Percidae'),
	 (uuid_generate_v4(),'Iowa darter (Etheostoma exile)','Iowa darter','Etheostoma exile','Percidae'),
	 (uuid_generate_v4(),'Rainbow darter (Etheostoma caeruleum)','Rainbow darter','Etheostoma caeruleum','Percidae'),
	 (uuid_generate_v4(),'Least darter (Etheostoma microperca)','Least darter','Etheostoma microperca','Percidae'),
	 (uuid_generate_v4(),'Fantail darter (Etheostoma flabellare)','Fantail darter','Etheostoma flabellare','Percidae'),
	 (uuid_generate_v4(),'Greenside darter (Etheostoma blennioides)','Greenside darter','Etheostoma blennioides','Percidae'),
	 (uuid_generate_v4(),'Freshwater drum (Aplodinotus grunniens)','Freshwater drum','Aplodinotus grunniens','Sciaenidae'),
	 (uuid_generate_v4(),'Deepwater sculpin (Myoxocephalus thompsonii)','Deepwater sculpin','Myoxocephalus thompsonii','Cottidae'),
	 (uuid_generate_v4(),'Spoonhead sculpin (Cottus ricei)','Spoonhead sculpin','Cottus ricei','Cottidae'),
	 (uuid_generate_v4(),'Torrent sculpin (Cottus rhotheus)','Torrent sculpin','Cottus rhotheus','Cottidae'),
	 (uuid_generate_v4(),'Slimy sculpin (Cottus cognatus)','Slimy sculpin','Cottus cognatus','Cottidae'),
	 (uuid_generate_v4(),'Mottled sculpin (Cottus bairdii)','Mottled sculpin','Cottus bairdii','Cottidae'),
	 (uuid_generate_v4(),'Shorthead sculpin (Cottus confusus)','Shorthead sculpin','Cottus confusus','Cottidae'),
	 (uuid_generate_v4(),'Prickly sculpin (Cottus asper)','Prickly sculpin','Cottus asper','Cottidae'),
	 (uuid_generate_v4(),'Coastrange sculpin (Cottus aleuticus)','Coastrange sculpin','Cottus aleuticus','Cottidae'),
	 (uuid_generate_v4(),'Rosyface shiner (Notropis rubellus)','Rosyface shiner','Notropis rubellus','Cyprinidae'),
	 (uuid_generate_v4(),'Emerald shiner (Notropis atherinoides)','Emerald shiner','Notropis atherinoides','Cyprinidae'),
	 (uuid_generate_v4(),'Spotfin shiner (Cyprinella spiloptera)','Spotfin shiner','Cyprinella spiloptera','Cyprinidae'),
	 (uuid_generate_v4(),'Spottail shiner (Notropis hudsonius)','Spottail shiner','Notropis hudsonius','Cyprinidae'),
	 (uuid_generate_v4(),'River shiner (Notropis blennius)','River shiner','Notropis blennius','Cyprinidae'),
	 (uuid_generate_v4(),'Pugnose shiner (Notropis anogenus)','Pugnose shiner','Notropis anogenus','Cyprinidae'),
	 (uuid_generate_v4(),'Blackchin shiner (Notropis heterodon)','Blackchin shiner','Notropis heterodon','Cyprinidae'),
	 (uuid_generate_v4(),'Blacknose shiner (Notropis heterolepis)','Blacknose shiner','Notropis heterolepis','Cyprinidae'),
	 (uuid_generate_v4(),'Mimic shiner (Notropis volucellus)','Mimic shiner','Notropis volucellus','Cyprinidae'),
	 (uuid_generate_v4(),'Sand shiner (Notropis stramineus)','Sand shiner','Notropis stramineus','Cyprinidae'),
	 (uuid_generate_v4(),'Brassy minnow (Hybognathus hankinsoni)','Brassy minnow','Hybognathus hankinsoni','Cyprinidae'),
	 (uuid_generate_v4(),'Silvery minnow (Hybognathus regius)','Silvery minnow','Hybognathus regius','Cyprinidae'),
	 (uuid_generate_v4(),'Bigmouth buffalo (Ictiobus cyprinellus)','Bigmouth buffalo','Ictiobus cyprinellus','Catostomidae'),
	 (uuid_generate_v4(),'Quillback (Carpiodes cyprinus)','Quillback','Carpiodes cyprinus','Catostomidae'),
	 (uuid_generate_v4(),'Lake chubsucker (Erimyzon sucetta)','Lake chubsucker','Erimyzon sucetta','Catostomidae'),
	 (uuid_generate_v4(),'Spotted sucker (Minytrema melanops)','Spotted sucker','Minytrema melanops','Catostomidae'),
	 (uuid_generate_v4(),'Northern hog sucker (Hypentelium nigricans)','Northern hog sucker','Hypentelium nigricans','Catostomidae'),
	 (uuid_generate_v4(),'Bridgelip sucker (Catostomus columbianus)','Bridgelip sucker','Catostomus columbianus','Catostomidae'),
	 (uuid_generate_v4(),'Mountain sucker (Catostomus platyrhynchus)','Mountain sucker','Catostomus platyrhynchus','Catostomidae'),
	 (uuid_generate_v4(),'White sucker (Catostomus commersonii)','White sucker','Catostomus commersonii','Catostomidae'),
	 (uuid_generate_v4(),'Largescale sucker (Catostumus macrocheilus)','Largescale sucker','Catostumus macrocheilus','Catostomidae'),
	 (uuid_generate_v4(),'Copper redhorse (Moxostoma hubbsi)','Copper redhorse','Moxostoma hubbsi','Catostomidae'),
	 (uuid_generate_v4(),'Greater redhorse (Moxostoma valenciennesi)','Greater redhorse','Moxostoma valenciennesi','Catostomidae'),
	 (uuid_generate_v4(),'Silver redhorse (Moxostoma anisurum)','Silver redhorse','Moxostoma anisurum','Catostomidae'),
	 (uuid_generate_v4(),'River redhorse (Moxostoma carinatum)','River redhorse','Moxostoma carinatum','Catostomidae'),
	 (uuid_generate_v4(),'Black redhorse (Moxostoma duquesnii)','Black redhorse','Moxostoma duquesnii','Catostomidae'),
	 (uuid_generate_v4(),'Shorthead redhorse (Moxostoma macrolepidotum)','Shorthead redhorse','Moxostoma macrolepidotum','Catostomidae'),
	 (uuid_generate_v4(),'Golden redhorse (Moxostoma erythrurum)','Golden redhorse','Moxostoma erythrurum','Catostomidae'),
	 (uuid_generate_v4(),'Channel catfish (Ictalurus punctatus)','Channel catfish','Ictalurus punctatus','Ictaluridae'),
	 (uuid_generate_v4(),'Yellow bullhead (Ameiurus natalis)','Yellow bullhead','Ameiurus natalis','Ictaluridae'),
	 (uuid_generate_v4(),'Brown bullhead (Ameiurus nebulosus)','Brown bullhead','Ameiurus nebulosus','Ictaluridae'),
	 (uuid_generate_v4(),'Black bullhead (Ameiurus melas)','Black bullhead','Ameiurus melas','Ictaluridae'),
	 (uuid_generate_v4(),'Stonecat (Noturus flavus)','Stonecat','Noturus flavus','Ictaluridae'),
	 (uuid_generate_v4(),'American eel (Anguilla rostrata)','American eel','Anguilla rostrata','Anguillidae'),
	 (uuid_generate_v4(),'Banded killifish (Fundulus diaphanus)','Banded killifish','Fundulus diaphanus','Cyprinodontidae'),
	 (uuid_generate_v4(),'Mummichog (Fundulus heteroclitus)','Mummichog','Fundulus heteroclitus','Cyprinodontidae'),
	 (uuid_generate_v4(),'Burbot (Lota lota)','Burbot','Lota lota','Gadidiae'),
	 (uuid_generate_v4(),'European river lamprey (Lampeta fluviatilis)','European river lamprey','Lampeta fluviatilis','Petromyzontidae'),
	 (uuid_generate_v4(),'Striped shiner (Luxilus chrysocephalus)','Striped shiner','Luxilus chrysocephalus','Cyprinidae'),
	 (uuid_generate_v4(),'Bowfin (Amia calva)','Bowfin','Amia calva','Amiidae'),
	 (uuid_generate_v4(),'Sea trout (Salmo trutta)','Sea trout','Salmo trutta','Salmonidae'),
	 (uuid_generate_v4(),'American gizzard shad (Dorosoma cepedianum)','American gizzard shad','Dorosoma cepedianum','Clupeidae'),
	 (uuid_generate_v4(),'Goldfish (Carassius auratus)','Goldfish','Carassius auratus','Cyprinidae'),
	 (uuid_generate_v4(),'Lake trout (Salvelinus namaycush)','Lake trout','Salvelinus namaycush','Salmonidae'),
	 (uuid_generate_v4(),'Longnose sucker (Catostomus catostomus)','Longnose sucker','Catostomus catostomus','Catostomidae'),
	 (uuid_generate_v4(),'Lake whitefish (Coregonus clupeaformis)','Lake whitefish','Coregonus clupeaformis','Salmonidae'),
	 (uuid_generate_v4(),'Cisco (Coregonus artedi)','Cisco','Coregonus artedi','Salmonidae'),
	 (uuid_generate_v4(),'Arctic grayling (Thymallus arcticus)','Arctic grayling','Thymallus arcticus','Salmonidae'),
	 (uuid_generate_v4(),'Mooneye (Hiodon tergisus)','Mooneye','Hiodon tergisus','Actinopterygii'),
	 (uuid_generate_v4(),'Bull trout (Salvelinus confluentus)','Bull trout','Salvelinus confluentus','Salmonidae'),
	 (uuid_generate_v4(),'Rainbow smelt (Osmerus mordax)','Rainbow smelt','Osmerus mordax','Osmeridae'),
	 (uuid_generate_v4(),'Mountain whitefish (Prosopium williamsoni)','Mountain whitefish','Prosopium williamsoni','Salmonidae'),
	 (uuid_generate_v4(),'Dolly Varden trout (Salvelinus malma)','Dolly Varden trout','Salvelinus malma','Salmonidae'),
	 (uuid_generate_v4(), 'Zander (Sander lucioperca)', 'Zander', 'Sander lucioperca', 'Percidae');
	 
	 
	 