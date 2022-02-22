import psycopg2 as pg2
import subprocess
import sys
from uuid import uuid4

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[1]
dbPassword = sys.argv[2]

class MappingScript:

    workingSchema = "featurecopy"
    workingTable = ""

    damTable = workingSchema + ".dams"
    damAttributeTable = workingSchema + ".dams_attribute_source"

    fishTable = workingSchema + ".fishways"
    fishAttributeTable = workingSchema + ".fishways_attribute_source"

    liveDamSchema = "dams"

    liveDamTable = liveDamSchema + ".dams"
    liveDamAttributeTable = liveDamSchema + ".dams_attribute_source"

    datasetname = ""

    dataFile = ""

    dsUuid = uuid4()

    def __init__(self, datasetname):

        self.datasetname = datasetname
        self.workingTable = self.workingSchema + "." + datasetname

        if len(sys.argv) != 3:
            print("Invalid usage: PY <datasetname>.py <dbUser> <dbPassword>")
            sys.exit()

    def do_work(self, mappingquery):

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        print("Mapping attributes from " +  self.datasetname)
        self.run_mapping_query(mappingquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Attributes mapped to " + self.damTable + " from " + self.datasetname)
    
    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)