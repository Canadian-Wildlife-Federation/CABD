import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "cabd"

damTable = "featurecopy.dams"
damAttributeTable = "featurecopy.dams_attribute_source"

class DamMappingScript:

    workingSchema = "featurecopy"
    
    workingTable = ""
    
    datasetname = ""
    
    def __init__(self, datasetname):
        self.datasetname = datasetname
        self.workingTable = self.workingSchema + ".dams_" + datasetname

        self.dbUser = sys.argv[1]
        self.dbPassword = sys.argv[2]
            
        if len(sys.argv) != 3:
            print("Invalid usage: MAP_dams_<datasetid>.py <dbUser> <dbPassword>")
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
        print("Attributes mapped from: " + self.datasetname)

    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)
