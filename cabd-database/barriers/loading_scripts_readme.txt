------------ CABD Feature Types - Data Loading Scripts ----------------

These python scripts load feature data from various sources into the CABD database.
All scripts load data into a temporary table identified at the top of the file.  Once finished the
script write out the sql statement to run to load the data into the main feature database table.


**** !!! 
The script DOES NOT load data into the main feature database table - the user must
run the sql that is printed out after the data is loaded in order to move the data
from the temporary working table into the production feature table.
**** !!!
 


Requirements:
This script requires python and the ogr tools.

Note: For windows users: QGIS comes with a version of the ogr tools that 
can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

To run this script:
1) Configure the database connection and ogr2ogr tools.  These fields are defined at 
the top of the script file.  Update these as required to reflect the settings you require.  
The variables are ogr, dbHost, dbPort, dbName, dbUser, dbPassword.

2) Run the script providing the output file from the CHyF processing tools and region name as arguments:

cabd_XXX_XXXXX.py <dataFile>

Once the data has been loaded into the temporary table you can view the data and make any addition updates you want
to make before loading the data into the main feature table. (The script writes out the sql statement required to load the
data).

