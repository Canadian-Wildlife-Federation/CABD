#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# Removes isolated flowpaths from the stream network
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import psycopg2 as pg2
import psycopg2.extras as pg2e
import configparser
import argparse
import getpass
import networkx as nx
import shapely.wkb
from tqdm import tqdm
import ast

import sys

#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Removing isolated flowpaths from stream network.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
args = parser.parse_args()
if (args.c):
    configfile = args.c

appconfig = configparser.ConfigParser()
appconfig.read(configfile)



# Database settings
dbHost = appconfig['DATABASE']['host']
dbPort = appconfig['DATABASE']['port']
dbName = appconfig['DATABASE']['name']
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")


dbTargetSchema = appconfig['DATABASE']['data_schema']
dbTargetStreamTable = appconfig['CHYF']['streamTable']
mSRID = appconfig['DATABASE']['cabdSRID']
watershed_id = ast.literal_eval(appconfig['DATASETS']['watershed_id'])



def createTable(connection):
    """
    Creates a table to store the copy of the streams table
    """

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable}_copy;

        CREATE TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy (LIKE {dbTargetSchema}.{dbTargetStreamTable} INCLUDING ALL);

        --ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy OWNER TO cwf_analyst;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def copyStreams(conn, watershed_id):
    """
    copy streams from watershed into table
    """
    
    query = f"""
        INSERT INTO {dbTargetSchema}.{dbTargetStreamTable}_copy
        SELECT * FROM {dbTargetSchema}.{dbTargetStreamTable}
        WHERE {dbTargetSchema}.{dbTargetStreamTable}.short_name in {watershed_id};
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def disconnectedIslands(conn, watershed_id):
    """
    Creates an undirected graph of stream network and 
    groups by connected portions.
    Result is an additional column in the streams_copy table indicating
    which network group the stream portion belongs to

    Based on algorithm in disconnected islands plugin
    :see: https://github.com/AfriGIS-South-Africa/disconnected-islands/blob/master/disconnected_islands.py
    """
    G = nx.Graph()
    tolerance = 0.000001

    global numGroups

    # Get num rows for progress bar
    query = f"""
        SELECT COUNT(*) 
        FROM {dbTargetSchema}.{dbTargetStreamTable}_copy
        WHERE {dbTargetSchema}.{dbTargetStreamTable}_copy.short_name in {watershed_id};
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        numRows = result[0]

    # Get the stream network
    query = f"""
        SELECT * 
        FROM {dbTargetSchema}.{dbTargetStreamTable}_copy
        WHERE {dbTargetSchema}.{dbTargetStreamTable}_copy.short_name in {watershed_id};
    """

    with conn.cursor(name='server_cursor') as cursor:
        cursor.execute(query)
        chunkSize = 100000

        # Calculate progress
        iteration = 1
        remaining = int(numRows/chunkSize)+1

        while True:
            features = cursor.fetchmany(chunkSize)

            if not features:
                break

            for feat in tqdm(features, desc=f'  [{iteration}/{remaining}]', miniters=5000):

                # TO DO: need programmatic way to determine which index for feat is linestring
                geom = shapely.wkb.loads(feat[12], hex=True) # linestring from feature of stream network
                edges = []

                for i in range(len(geom.coords)-1):
                    G.add_edges_from(
                        [
                            ((int(geom.coords[i][0]/tolerance), int(geom.coords[i][1]/tolerance)),
                            (int(geom.coords[i+1][0]/tolerance), int(geom.coords[i+1][1]/tolerance)),
                            {'fid': feat[0]})
                        ]
                    )
            iteration += 1

    # query = f"""
    #     ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy
    #         ADD COLUMN IF NOT EXISTS networkGrp int DEFAULT 0
    # """
    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     conn.commit()

    query = f"""
        DROP TABLE IF EXISTS network_groups;
        CREATE TABLE network_groups
        (
            id uuid,
            ng int
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

    print("    finding connected subgraphs")
    connected_components = list(G.subgraph(c) for c in sorted(nx.connected_components(G), key=len, reverse=True))
    numGroups = len(connected_components)

    print("    writing results")
    # updateQuery = f"""
    #     UPDATE {dbTargetSchema}.{dbTargetStreamTable}_copy
    #     SET networkGrp = %s
    #     WHERE id = %s;
    # """

    insertQuery =f"""
        INSERT INTO network_groups (id, ng)
        VALUES %s;
    """

    # updateQuery = f"""
    #     UPDATE {dbTargetSchema}.{dbTargetStreamTable}_copy
    #     SET networkGrp = data.ng
    #     FROM (VALUES %s) AS data (id, ng)
    #     WHERE {dbTargetSchema}.{dbTargetStreamTable}_copy.id = data.id ;
    # """

    with conn.cursor() as cursor:
        for i, graph in enumerate(tqdm(connected_components, miniters=1000)):
            # ignore streams on largest network group (networkGrp = 0)
            # since most will be on this group
            # and default value is already 0
            # this speeds up the loop to only find streams off main network
            if i == 0:
                continue
            data = []
            for edge in graph.edges(data=True):
                    data.append(tuple([edge[2].get('fid'), i]))
                    # cursor.execute(updateQuery, (i, edge[2].get('fid')))
                    # pg2e.execute_values(cursor, insertQuery, data, template='(%s, %s::uuid)')
                    conn.commit()
            pg2e.execute_values(cursor, insertQuery, data, template='(%s::uuid, %s)')
            # conn.commit()

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy
            ADD COLUMN IF NOT EXISTS networkGrp int DEFAULT 0;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable}_copy AS s
        SET networkGrp = ng.ng
        FROM (SELECT id, ng FROM network_groups) AS ng
        WHERE s.id = ng.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
            

def dissolveFeatures(conn):
    """ Dissolve streams by network group """

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable}_dissolved;

        CREATE TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved 
        AS SELECT networkGrp, ST_UNION(geometry)::GEOMETRY(Geometry, {mSRID}) as geometry
            FROM {dbTargetSchema}.{dbTargetStreamTable}_copy
            WHERE networkGrp != 0
            GROUP BY networkGrp;
        
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved ADD COLUMN id SERIAL PRIMARY KEY;
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved ALTER COLUMN geometry 
            SET DATA TYPE geometry(MultiLineString, {mSRID}) USING ST_Multi(geometry);

        CREATE INDEX ON {dbTargetSchema}.{dbTargetStreamTable}_dissolved using gist (geometry);

        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved OWNER TO cwf_analyst;

        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable}_dissolved d
        USING public.chyf_shoreline as sh
        WHERE st_intersects(st_buffer(st_transform(sh.geometry, {appconfig.dataSrid}), 0.01), d.geometry);

    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    conn.commit()

def deleteIsolated(conn):
    """ Delete isolated streams from streams table and truncate copy of stream table"""

    # query = f"""
    #     DELETE 
    #     FROM {dbTargetSchema}.{dbTargetStreamTable}_copy s
    #     USING {dbTargetSchema}.{dbTargetStreamTable}_dissolved d
    #     WHERE st_intersects(s.geometry, d.geometry);

    #     DROP TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy;
    #     DROP TABLE {dbTargetSchema}.{dbTargetStreamTable}_dissolved;
    #     DROP TABLE network_groups;
    # """
    longestNetworks = int(0.005 * numGroups)
    query = f"""
        DELETE 
        FROM {dbTargetSchema}.{dbTargetStreamTable} s
        USING network_groups n
        WHERE n.ng >= {longestNetworks} AND s.id = n.id;

        DROP TABLE network_groups;
        TRUNCATE TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


def main():

    print("Connecting to database...")

    conn = pg2.connect(database=dbName, 
                    user=dbUser, 
                    host=dbHost, 
                    password=dbPassword, 
                    port=dbPort)

    conn.autocommit = False

    print("Removing Isolated Flowpaths")

    # Creates a copy of the stream table
    # Comment out if adding streams to existing table
    createTable(conn)

    for w in watershed_id:
        print(f"  WATERSHED: {w}")
        w = f"('{w}')"

        print("  copying stream table")
        copyStreams(conn, w)

        print("  grouping networks")
        disconnectedIslands(conn, w)

        # may be unnecessary
        # print("  dissolving features") 
        # dissolveFeatures(conn)

        print("  deleting isolated flowpaths")
        deleteIsolated(conn)

    # Drop copy of stream table when done processing watershed batch
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE {dbTargetSchema}.{dbTargetStreamTable}_copy;")
        conn.commit()

    conn.close()

    print("done")


        

if __name__ == "__main__":
    main()     