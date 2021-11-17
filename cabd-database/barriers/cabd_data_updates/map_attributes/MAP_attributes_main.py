import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "cabd"

damTable = "featurecopy.dams"
damAttributeTable = "featurecopy.dams_attribute_source"

waterfallTable = "featurecopy.waterfalls"
waterfallAttributeTable = "featurecopy.waterfalls_attribute_source"

fishwayTable = "featurecopy.fishways"
fishwayAttributeTable = "featurecopy.fishways_attribute_source"

class MappingScript:

    workingSchema = "featurecopy"
    
    workingTable = ""
    
    datasetname = ""
    
    def __init__(self, datasetname):

        self.featureType = sys.argv[1]
        self.dbUser = sys.argv[2]
        self.dbPassword = sys.argv[3]

        self.datasetname = datasetname
        self.workingTable = self.workingSchema + "." + self.featureType + "_" + datasetname
            
        if len(sys.argv) != 3:
            print("Invalid usage: MAP_<featureType>_<datasetid>.py <featureType> <dbUser> <dbPassword>")
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
        print("Attributes mapped to " + self.featureType + " from: " + self.datasetname)

    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)
