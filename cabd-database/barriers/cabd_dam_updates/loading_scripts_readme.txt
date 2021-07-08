------------ CABD Feature Types - Data Loading Scripts ----------------

These Python scripts load feature data from various sources into the CABD database.
All scripts load data into two tables:
(1) an original, source data table which contains all the fields from the original dataset.
(2) a load data table which only contains the fields that map to the CABD data structure.
The load table fields have the same name and data type as their target field in the CABD.

**** !!! 
These scripts DO NOT load data into the main feature database table.
To do this, you will need to run the corresponding update script for that dataset.
**** !!!

Requirements:
This script requires Python, the ogr tools, and psycopg2.

Note: For Windows users: QGIS comes with a version of the ogr tools that 
can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

To run this script:
1) Configure the database connection and ogr2ogr tools in the script file. 
These fields are defined at the top of the script file. Update these as 
required to reflect the settings you require. The variables are ogr, dbHost,
dbPort, dbName, dbUser, and dbPassword.

2) Run the script providing the source dataset file as your argument:

LOAD_[dams]_[datasetid].py "<dataFile>"

Once this script is complete, you can view the data and make any additional updates
needed before running the corresponding update script to load the attribute data
into the main CABD table.
