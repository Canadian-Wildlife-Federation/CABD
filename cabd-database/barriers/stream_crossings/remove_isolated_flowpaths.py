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


###########
# WATERSHED ID's for Saskatchewan
# watershed_id = ['05AK000', '05CK000', '05ED000', '05EF000', '05EG000', '05FE000', '05FF000', '05GA000', '05GB001', '05GB002', '05GC000', '05GD000', '05GE000', '05GF000', '05GG000']
# watershed_id = ['05JA000', '05JB000', '05JC000', '05JD001', '05JD002', '05JE000', '05JF000', '05JG000', '05JH000', '05JJ000', '05JK000', '05JL000', '05JM000']
# watershed_id = ['05KA000', '05KB000', '05KC000', '05KD000', '05KE000', '05KF000', '05KG000', '05KH000', '05KJ001']
# watershed_id = ['05LA000', '05LB000', '05LC000', '05LD000', '05LE000', '05MA000', '05MB000', '05MC000', '05MD000', '05ME000', '05MG000']
# watershed_id = ['05NA000', '05NB000', '05NC000', '05NDA00', '05NDB00', '05NE000', '05NFA00', '05NFB00', '05NG000']
# watershed_id = ['06AD000', '06AE000', '06AF000', '06AG000', '06BA000', '06BB000', '06BC000', '06BD000', '06CA000', '06CB000', '06CC000', '06CD000', '06CE000']
# watershed_id = ['06DA000', '06DB000', '06DC000', '06DD000', '06EA000', '06EB000', '06HA002', '06LA000']
# watershed_id = ['11AAD00', '11ABA00', '11ABC00', '11ABD00', '11AC000', '11ADA00', '11AEA00', '11AEB00', '11AEC00', '11AFA00', '11AFB00']




# if len(watershed_id) == 1:
#     watershed_id = f"('{watershed_id[0]}')"
# else:
#     watershed_id = tuple(watershed_id)


def createTable(connection):
    """
    Creates a table to store the new version of the streams table
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
        # features = cursor.fetchall()

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
    """ Delete isolated streams from streams table """

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
        FROM {dbTargetSchema}.{dbTargetStreamTable}_copy s
        USING network_groups n
        WHERE n.ng >= {longestNetworks} AND s.id = n.id;

        DROP TABLE network_groups;
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
    # createTable(conn)

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

    conn.close()

    print("done")


        

if __name__ == "__main__":
    main()     