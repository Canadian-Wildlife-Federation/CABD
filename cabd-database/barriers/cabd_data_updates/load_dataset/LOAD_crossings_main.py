import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class LoadingScript:

    #create source data table and tables to be mapped into CABD dataset
    sourceSchema = "source_data"
    workingSchema = "featurecopy"
    reviewSchema = "nb_data"
    
    sourceTable = ""
    
    reviewTable = "nb_crossing_review"

    nonTidalSites = ""
    nonTidalStructures = ""

    tidalSites = ""
    tidalStructures = ""

    tidalMaterialMapping = "tidal_material_mapping"
    nonTidalMaterialMapping = "nontidal_material_mapping"    

    tidalPhysicalBarrierMapping = "tidal_physical_barrier_mapping"
    nonTidalPhysicalBarrierMapping = "nontidal_physical_barrier_mapping"
    
    datasetname = ""
    datafile = ""
    
    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.sourceTable = self.sourceSchema + "." + datasetname
        self.reviewTable = self.reviewSchema + "." + self.reviewTable

        self.nonTidalSites = self.workingSchema + ".nontidal_sites_" + datasetname
        self.nonTidalStructures = self.workingSchema + ".nontidal_structures_" + datasetname

        self.tidalSites = self.workingSchema + ".tidal_sites_" + datasetname
        self.tidalStructures = self.workingSchema + ".tidal_structures_" + datasetname        

        self.tidalMaterialMappingTable = self.workingSchema + "." + self.tidalMaterialMapping # these need to already be set up
        self.nonTidalMaterialMappingTable = self.workingSchema + "." + self.nonTidalMaterialMapping # these need to already be set up

        self.tidalPhysicalBarrierMappingTable = self.workingSchema + "." + self.tidalPhysicalBarrierMapping # these need to already be set up
        self.nonTidalPhysicalBarrierMappingTable = self.workingSchema + "." + self.nonTidalPhysicalBarrierMapping # these need to already be set up
        
        self.datafile = sys.argv[1]
        
        if len(sys.argv) != 4:
            print('Invalid usage: py LOAD_<datasetname>.py "<datafile>" <dbUser> <dbPassword>')
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
        print("Splitting by site type and mapping attributes to data model...")
        # print(loadquery)
        self.run_load_query(loadquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Data loaded into following schemas: " + self.sourceSchema + " (original data), " + self.workingSchema + " (mapped data)")

    #load data from geopackage file into the database
    def load_data(self, filename):
        #load data using ogr
        orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
        pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + self.sourceTable + '" -lco GEOMETRY_NAME=geometry -nlt POINT ' + '"' + filename + '"'
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