import psycopg2 as pg2
import subprocess
import sys

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class MappingScript:

    workingSchema = "featurecopy"
    
    workingTable = ""
    
    datasetname = ""

    damTable = workingSchema + ".dams"
    damAttributeTable = workingSchema + ".dams_attribute_source"

    waterfallTable = workingSchema + ".waterfalls"
    waterfallAttributeTable = workingSchema + ".waterfalls_attribute_source"

    fishwayTable = workingSchema + ".fishways"
    fishwayAttributeTable = workingSchema + ".fishways_attribute_source"

    featureType = sys.argv[1]
    
    def __init__(self, datasetname):

        self.datasetname = datasetname
        self.workingTable = self.workingSchema + "." + self.featureType + "_" + datasetname

        if len(sys.argv) != 4:
            print("Invalid usage: MAP_<featureType>_<datasetid>.py <featureType> <dbUser> <dbPassword>")
            sys.exit()
        
        if sys.argv[1] not in ["dams", "waterfalls", "fishways"]:
            print("Invalid usage: featureType must be either dams, waterfalls, or fishways.")
            print("Correct usage is: MAP_<featureType>_<datasetid>.py <featureType> <dbUser> <dbPassword>")
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
        print("Attributes mapped to " + self.workingSchema + "." + self.featureType + " from " + self.datasetname)

    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)