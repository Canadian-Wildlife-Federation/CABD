import psycopg2 as pg2
import subprocess
import sys
import getpass

# dbHost = "localhost"
# dbPort = "5432"
# dbName = "cabd_dev_2024"

dbHost = "cabd-postgres-prod.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

featureType = sys.argv[1]
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

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
        
        if sys.argv[1] not in ["dams", "fishways", "waterfalls"]:
            print("Error: featureType must be either dams, fishways, or waterfalls")
            print("Correct usage is: py map_<featureType>_updates.py <featureType>")
            sys.exit()
        
        if featureType == "dams":
            self.updateTable = self.damUpdateTable
        elif featureType == "fishways":
            self.updateTable = self.fishUpdateTable
        elif featureType == "waterfalls":
            self.updateTable = self.fallUpdateTable
        else:
            print("Error: featureType must be either dams, fishways, or waterfalls")

    def do_work(self, query, initializequery, mappingquery):
        print("Checking list of updates for", featureType)

        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)
        
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            self.conn.commit()

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