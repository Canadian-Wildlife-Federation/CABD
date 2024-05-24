# script to find list of columns to revert from updates table
# returns an excel spreadsheet with the columns: cabd_id, data_source_short_name, revert_col
# revert_col refers to an array of columns that need to be reverted
#
# @author: Andrew Pozzuoli
# @date: May 2024
#
# usage: py create_revert_table.py <filtered update table excel file>
# table should first be filtered by updates that need to be reverted
# and that have comments indicating what needs to be reverted

import pandas as pd
import sys
import getpass
import psycopg2 as pg2

# ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
ogr = "C:\\Program Files\\QGIS 3.22.1\\bin\\ogr2ogr.exe"


# log in and connection information
dbHost = "localhost"
dbPort = "5432"
dbName = "cabd_dev_2024"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

damSchema = "dams"
damTable = damSchema + ".dams"

dataFile = ""
dataFile = sys.argv[1]

data = pd.read_excel(dataFile)

revert_df = pd.DataFrame(columns=['cabd_id', 'data_source_short_name', 'revert_col', 'reviewer_comments']) # result dataframe

# get list of column names for dams
with conn.cursor() as cursor:
    cursor.execute(f"SELECT * FROM {damTable} LIMIT 0")
    colnames = [col[0] for col in cursor.description]
conn.commit()

# go through data updates and get columns that need to be reverted from reviewer comments
for i in data.index:
    feature = data.loc[i]
    revert_col = feature['reviewer_comments'].replace(',', '').replace('.', '').split()
    revert_col = [x for x in revert_col if x in colnames]
    # this is for comments like 'please remove entire row' or 'please clear all columns'
    if not revert_col:
        revert_col = ['all']
    revert_df.loc[i] = [feature['cabd_id'], feature['data_source_short_name'], revert_col, feature['reviewer_comments']]

revert_df.to_excel('revert_columns.xlsx')
print('Done!')
    
                                                                             



