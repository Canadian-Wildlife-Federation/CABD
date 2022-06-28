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

    sourceSchema = "source_data"
    sourceTable = ""

    tempSchema = "featurecopy"

    damTable = tempSchema + ".dams"
    damAttributeTable = tempSchema + ".dams_attribute_source"

    fishTable = tempSchema + ".fishways"
    fishAttributeTable = tempSchema + ".fishways_attribute_source"

    liveSchema = "dams"

    liveDamTable = liveSchema + ".dams"
    liveDamAttributeTable = liveSchema + ".dams_attribute_source"

    liveFishTable = liveSchema + ".fishways"
    liveFishAttributeTable = liveSchema + ".fishways_attribute_source"

    datasetName = ""

    dsUuid = uuid4()

    def __init__(self, datasetName):

        self.datasetName = datasetName
        self.sourceTable = self.sourceSchema + "." + datasetName

        if len(sys.argv) != 3:
            print("Invalid usage: py <datasetName>.py <dbUser> <dbPassword>")
            sys.exit()

    def do_work(self, mappingquery):

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        print("Mapping attributes from " +  self.datasetName)
        self.run_mapping_query(mappingquery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Attributes mapped from " + self.datasetName)
    
    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)