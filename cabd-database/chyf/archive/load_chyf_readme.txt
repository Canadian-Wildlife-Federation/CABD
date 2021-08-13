------------ CHyF Hydro Data Load Script ----------------

The python script chyf_hydro.py loads hydro data outputs from the CHyF processing tools into the CHyF postgreSQL database.


Requirements:
This script requires python and the ogr tools.

Note: For windows users: QGIS comes with a version of the ogr tools that can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)


To run this script:
1) Configure the database connection and ogr2ogr tools.  These fields are defined at the top of the script file.  Update these as required to reflect the settings you require.  The variables are ogr, dbHost, dbPort, dbName, dbUser, dbPassword.

2) Run the script providing the output file from the CHyF processing tools and region name as arguments:

chyf_hydro.py <dataFile> <region>

Region is a unique identifier for the region being loaded.  We link all data from the dataFile file to the region so we can easily delete and replace data in the future.

