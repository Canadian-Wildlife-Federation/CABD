import psycopg2 as pg2
import subprocess
import sys
from uuid import uuid4

ogr = "C:\\Program Files\\QGIS 3.12\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "chyf"
dbUser = "postgres"
dbPassword = "sql"

duplicatestable = "load.duplicates"

damTable = "dams.dams_medium_large"
damAttributeTable = "dams.dams_medium_large_attribute_source"

class DamLoadingScript:

    #create temporary table and table to be inserted into CABD dataset
    tempSchema = "source_data"
    workingSchema = "load"
    
    tempTable = ""
    workingTable = ""
    
    datasetname = ""
    datafile = ""
    
    dsUuid = uuid4();
    
    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.tempTable = self.tempSchema + "." + datasetname
        self.workingTable = self.workingSchema + "." + datasetname
        
        if len(sys.argv) == 2:
            self.datafile = sys.argv[1]
            
            
        if self.datafile == '':
            print("Data file required.  Usage: LOAD_dams_grand.py <datafile>")
            sys.exit()


    def do_work(self, mappingquery, prodquery):       
        print("Loading data from file " +  self.datafile)
         
        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        self.initdb();
        self.load_data(self.datafile)
        print("running mapping query")
        self.run_mapping_query(mappingquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Data loaded into table: " + self.workingTable)
        print("To Update Production Data:")
        print(prodquery);

    #load data from geopackage file into the database
    #into a table calls tempSchema.datasetname
    def load_data(self, filename):
        #load data using ogr
        orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
        pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + self.tempTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + '"' + filename + '"'
        print(pycmd)
        subprocess.run(pycmd)

    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)

    def initdb(self):
        query = f"""
CREATE SCHEMA IF NOT EXISTS {self.tempSchema};
CREATE SCHEMA IF NOT EXISTS {self.workingSchema};
DROP TABLE IF EXISTS {self.tempTable};
DROP TABLE IF EXISTS {self.workingTable};
"""
        with self.conn.cursor() as cursor:
            cursor.execute(query);
        self.conn.commit();
