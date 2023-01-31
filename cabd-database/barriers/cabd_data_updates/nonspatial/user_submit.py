import psycopg2 as pg2
import subprocess
import sys

dbName = "cabd_dev" # change back to production details when ready
dbHost = "localhost" # change back to production details when ready
dbPort = "5432"
featureType = sys.argv[1]
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

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
    updateTable = ""

    def __init__(self, datasetName):

        self.datasetName = datasetName

        if len(sys.argv) != 4:
            print("Invalid usage: py map_<featureType>_updates.py <featureType> <dbUser> <dbPassword>")
            sys.exit()
        
        if sys.argv[1] not in ["dams", "fishways", "waterfalls"]:
            print("Error: featureType must be either dams, fishways, or waterfalls")
            print("Correct usage is: py map_<featureType>_updates.py <featureType> <dbUser> <dbPassword>")
            sys.exit()
        
        if featureType == "dams":
            self.updateTable = self.damUpdateTable
        elif featureType == "fishways":
            self.updateTable = self.fishUpdateTable
        elif featureType == "waterfalls":
            self.updateTable = self.fallUpdateTable
        else:
            print("Error: featureType must be either dams, fishways, or waterfalls")

    def do_work(self, initializequery, mappingquery):
        print("Checking list of updates for", featureType)

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

        # where multiple updates exist for a feature, only update one at a time
        waitcount = 0
        waitquery = f"""SELECT COUNT(*) FROM {self.updateTable} WHERE update_status = 'wait'"""

        while(True):
            self.run_init_query(initializequery)
            with self.conn.cursor() as cursor:
                cursor.execute(waitquery)
                waitcount = int(cursor.fetchone()[0])
            print(waitcount, "updates are waiting to be made...")

            self.run_mapping_query(mappingquery)
            self.conn.commit()

            if waitcount == 0:
                break
        
        self.conn.commit()
        self.conn.close
        print("Script complete - all updates have been processed.")

    def run_init_query(self, initializequery):
        with self.conn.cursor() as cursor:
            cursor.execute(initializequery)

    def run_mapping_query(self, mappingquery):
        with self.conn.cursor() as cursor:
            cursor.execute(mappingquery)