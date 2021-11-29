import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class LoadingScript:

    #create source data table and tables to be mapped into CABD dataset
    sourceSchema = "source_data"
    workingSchema = "featurecopy"
    
    sourceTable = ""

    damWorkingTable = ""

    fallWorkingTable = ""

    fishWorkingTable = ""

    speciesMapping = "fishmapping"
    
    datasetname = ""
    datafile = ""
    
    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.sourceTable = self.sourceSchema + "." + datasetname
        self.damWorkingTable = self.workingSchema + ".dams_" + datasetname
        self.fallWorkingTable = self.workingSchema + ".waterfalls_" + datasetname
        self.fishWorkingTable = self.workingSchema + ".fishways_" + datasetname
        self.speciesMappingTable = self.workingSchema + "." + self.speciesMapping
        
        self.datafile = sys.argv[1]
        
        if len(sys.argv) != 4:
            print('Invalid usage: py LOAD_<datasetid>.py "<datafile>" <dbUser> <dbPassword>')
            sys.exit()

    def do_work(self, loadquery):
        print("Loading data from file " +  self.datafile)

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        self.initdb()
        self.load_data(self.datafile)
        print("Splitting by feature type and mapping attributes to data model...")
        self.run_load_query(loadquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Data loaded into schemas: " + self.sourceSchema + " (original data), " + self.workingSchema + " (mapped data)")

    #load data from geopackage file into the database
    def load_data(self, filename):
        #load data using ogr
        orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
        pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + self.sourceTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + '"' + filename + '"'
        print(pycmd)
        subprocess.run(pycmd)

    def run_load_query(self, loadquery):
        with self.conn.cursor() as cursor:
            cursor.execute(loadquery)

    def initdb(self):
        query = f"""
        CREATE SCHEMA IF NOT EXISTS {self.sourceSchema};
        CREATE SCHEMA IF NOT EXISTS {self.workingSchema};
        DROP TABLE IF EXISTS {self.sourceTable};
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
        self.conn.commit()
