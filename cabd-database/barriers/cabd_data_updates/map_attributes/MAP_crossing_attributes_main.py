import psycopg2 as pg2
import subprocess
import sys

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class MappingScript:

    workingSchema = "featurecopy"
    
    workingTable = ""
    
    datasetname = ""

    nonTidalSites = "nontidal_sites"
    nonTidalStructures = "nontidal_structures"

    tidalSites = "tidal_sites"
    tidalStructures = "tidal_structures"

    siteType = sys.argv[1]
    
    def __init__(self, datasetname):

        self.datasetname = datasetname
        self.workingTable = self.workingSchema + "." + self.siteType + "_sites_" + datasetname

        self.nonTidalSitesTable = self.workingSchema + "." + self.nonTidalSites
        self.nonTidalStructuresTable = self.workingSchema + "." + self.nonTidalStructures

        self.tidalSitesTable = self.workingSchema + "." + self.tidalSites
        self.tidalStructuresTable = self.workingSchema + "." + self.tidalStructures

        if len(sys.argv) != 4:
            print("Invalid usage: MAP_<datasetid>.py <siteType> <dbUser> <dbPassword>")
            sys.exit()

    def do_work(self, mappingquery):
        print("Mapping attributes from " +  self.datasetname)
         
        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)
        self.run_mapping_query(mappingquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Attributes mapped to " + self.workingSchema + " from " + self.datasetname)

    def run_mapping_query(self, mappingquery):
        print(mappingquery)
        # with self.conn.cursor() as cursor:
        #     cursor.execute(mappingquery)
