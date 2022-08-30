import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class MappingScript:

    sourceSchema = "source_data"
    sourceTable = ""

    damTableNewModify = sourceSchema + '.dams_new_modify'
    damTableDelete = sourceSchema + '.dams_delete'

    fishTableNewModify = sourceSchema + '.fishways_new_modify'
    fishTableDelete = sourceSchema + '.fishways_delete'

    damSchema = "dams"

    damTable = damSchema + ".dams"
    damAttributeTable = damSchema + ".dams_attribute_source"
    damFeatureTable = damSchema + ".dams_feature_source"

    fishSchema = "fishways"

    fishTable = fishSchema + ".fishways"
    fishAttributeTable = fishSchema + ".fishways_attribute_source"
    fishFeatureTable = fishSchema + ".fishways_feature_source"

    datasetName = ""
    dataFile = ""

    def __init__(self, datasetName):

        self.datasetName = datasetName
        self.sourceTable = self.sourceSchema + "." + datasetName
        self.dataFile = sys.argv[1]

        print("datasetName: " + self.datasetName)
        print("sourceTable: " + self.sourceTable)
        print("dataFile: " + self.dataFile)

        if len(sys.argv) != 4:
            print('Invalid usage: py dams_user_submitted_updates.py "<dataFile>" <dbUser> <dbPassword>')
            sys.exit()

    def do_work(self, mappingquery):
        print("Loading csv file " + self.dataFile)

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        self.load_data(self.dataFile)
    #     #print("Updating live data")
    #     #self.run_mapping_query(mappingquery)

        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
    #     #print("Live data updated")

    # def run_mapping_query(self, mappingquery):
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(mappingquery)

    #load data from csv file into the database
    def load_data(self, filename):
        #load data using ogr
        orgDb = "dbname='" + dbName + "' host='"+ dbHost +"' port='"+ dbPort + "' user='" + dbUser + "' password='" + dbPassword + "'"
        pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + ' "' + filename + '"' + ' -nln "' + self.sourceTable + '" -oo AUTODETECT_TYPE = YES'
        print(pycmd)
        subprocess.run(pycmd)