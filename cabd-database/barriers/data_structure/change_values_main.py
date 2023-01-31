import psycopg2 as pg2
import subprocess
import sys

dbHost = "localhost" # change this when ready to use on live data
dbPort = "5432"
dbName = "cabd_dev" # change this when ready to use on live data
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

class MappingScript:
    
    datasetname = ""

    cabdSchema = "cabd"
    dataSourceTable = cabdSchema + ".data_source"

    damSchema = "dams"
    damTable = damSchema + ".dams"
    damAttributeTable = damSchema + ".dams_attribute_source"
    damFeatureTable = damSchema + ".dams_feature_source"

    waterfallSchema = "waterfalls"
    waterfallTable = waterfallSchema + ".waterfalls"
    waterfallAttributeTable = waterfallSchema + ".waterfalls_attribute_source"
    waterfallFeatureTable = waterfallSchema + ".waterfalls_feature_source"

    fishwaySchema = "fishways"
    fishwayTable = fishwaySchema + ".fishways"
    fishwayAttributeTable = fishwaySchema + ".fishways_attribute_source"
    fishwayFeatureTable = fishwaySchema + ".fishways_feature_source"

    featureType = sys.argv[1]
    
    def __init__(self, datasetname):

        self.datasetname = datasetname

        if len(sys.argv) != 4:
            print("Invalid usage: <featureType>_<datasetid>.py <featureType> <dbUser> <dbPassword>")
            sys.exit()
        
        if sys.argv[1] not in ["dams", "waterfalls", "fishways"]:
            print("Invalid usage: featureType must be either dams, waterfalls, or fishways")
            print("Correct usage is: <featureType>_<datasetid>.py <featureType> <dbUser> <dbPassword>")
            sys.exit()

    def do_work(self, updatequery):
        print("Mapping attributes from " +  self.datasetname)
         
        self.conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)
        
        # print(updatequery)
        self.run_mapping_query(updatequery)
        
        self.conn.commit()
        self.conn.close()
        
        print("Script complete")
        print("Values updated in " + self.damTable + " from " + self.datasetname)

    def run_mapping_query(self, updatequery):
        with self.conn.cursor() as cursor:
            cursor.execute(updatequery)