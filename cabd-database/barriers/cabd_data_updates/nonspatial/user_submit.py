import psycopg2 as pg2
import subprocess
import sys

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = sys.argv[1]
dbPassword = sys.argv[2]

class MappingScript:

    updateSchema = "cabd"
    damUpdateTable = updateSchema + '.dam_updates'
    fishUpdateTable = updateSchema + '.fishway_updates'
    fallUpdateTable = updateSchema + '.waterfall_updates'

    damSchema = "dams"
    damTable = damSchema + ".dams"
    damAttributeTable = damSchema + ".dams_attribute_source"
    damFeatureTable = damSchema + ".dams_feature_source"

    fishSchema = "fishways"
    fishTable = fishSchema + ".fishways"
    fishAttributeTable = fishSchema + ".fishways_attribute_source"
    fishFeatureTable = fishSchema + ".fishways_feature_source"

    fallSchema = "waterfalls"
    fallTable = fallSchema + ".waterfalls"
    fallAttributeTable = fallSchema + ".waterfalls_attribute_source"
    fallFeatureTable = fallSchema + ".waterfalls_feature_source"

    datasetName = ""

    def __init__(self, datasetName):

        self.datasetName = datasetName

        print("datasetName: " + self.datasetName)
        print("damUpdateTable: " + self.damUpdateTable)

        if len(sys.argv) != 3:
            print("Invalid usage: py map_<featureType>_updates.py <dbUser> <dbPassword>")
            sys.exit()

    def do_work(self, mappingquery):
        # print("Updating live data")

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        print(mappingquery)
        print("Run queries above to update live data")
        print("Please make sure to run checks for accuracy as well")
        # self.run_mapping_query(mappingquery)
        # self.conn.commit()
        # self.conn.close()
        
        # print("Script complete - live data updated")

    # def run_mapping_query(self, mappingquery):
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(mappingquery)