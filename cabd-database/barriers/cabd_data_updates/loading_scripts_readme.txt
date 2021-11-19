------------ CABD - Source Data Loading Scripts ----------------

The scripts in the load_dataset folder load your source datasets into the DB.

**** !!!
Before executing these scripts, check the date fields for records in the cabd.data_source table.
Many (if not all) of your source datasets may already be loaded into the DB.
If so, you can skip this step.
**** !!!

These Python scripts load feature data from various sources into the CABD database.

The scripts load data into two schemas:

(1) An original, source data table, which contains all feature types,
fields, and field names from the original dataset.
--example format: source_data.fiss

(2) Load data tables separated by feature type, which only contain the 
fields that map to the CABD data structure. Fields in these tables will
have the same name and data type as their target field in the CABD.
--example format: featurecopy.dams_fiss, featurecopy.waterfalls_fiss

Requirements:
This script requires Python, the ogr tools, and psycopg2.

Note: For Windows users: QGIS comes with a version of the ogr tools that 
can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

To run these scripts:
1) Configure the database connection and ogr2ogr tools in the script file. 
These fields are defined at the top of the script file. Update these as 
required to reflect the settings you require. The variables are ogr, dbHost,
dbPort, and dbName. To protect your credentials, the dbUser and dbPassword
arguments will be specified when you run the script.

2) Run the script providing the source dataset file and your database
username and password as your arguments:

py LOAD_<datasetid>.py "<datafile>" <dbUser> <dbPassword>

Once the script is complete, you can view the data and make any additional updates
needed.

------------ CABD - Feature Review Loading Scripts ----------------

Before you can map attributes from source datasets into your feature tables,
you'll need to load your feature review geopackage into the DB. This geopackage
should contain de-duplicated data points for the feature type (e.g., dams).

It should contain the following fields at a minimum:

(1) Data source (as a dataset id, e.g., fiss or ab_basefeatures, not 
'Fisheries Information Summary System').

(2) Data source id (as a stable id from the data source you specified)

(3) Fields for all your input datasets with a name like 'dups_<datasetid>', where 
field values representthe stable id from that source dataset for a duplicate point. 

(4) A "use for analysis" field which indicates whether the feature should be snapped
to the hydro network.

(5) Geometry as a point or multipoint (note that multipoint features are currently
not supported in the DB, but planned for the future).

The script will load your feature geopackage into the DB, assign a cabd_id to each
feature, add additional fields for that feature type as required for the CABD,
populate constraints, snap features to the CHyF network, and populate the
<featuretype>_attribute_source_tracking table with records for each feature loaded.

Requirements:
This script requires Python, the ogr tools, and psycopg2.

Note: For Windows users: QGIS comes with a version of the ogr tools that 
can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

To run these scripts:
1) Configure the database connection and ogr2ogr tools in the script file. 
These fields are defined at the top of the script file. Update these as 
required to reflect the settings you require. The variables are ogr, dbHost,
dbPort, and dbName. To protect your credentials, the dbUser and dbPassword
arguments will be specified when you run the script.

2) Run the script providing the source dataset file, province or territory
code (e.g., 'ab', 'bc') and your database username and password as your arguments:

py LOAD_<featuretype>_review.py <datafile> <provinceCode> <dbUser> <dbPassword>

Once the script is complete, you can view the data and make any additional updates
needed.
