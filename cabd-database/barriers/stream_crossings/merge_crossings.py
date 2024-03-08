import argparse
import getpass
import psycopg2 as pg2

#-- PARSE COMMAND LINE ARGUMENTS --
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-table', type=str, help='the table to hold final data')
args = parser.parse_args()

#database settings
dbHost = 'cabd-postgres.postgres.database.azure.com'
dbPort = '5432'
dbName = 'cabd'
dbSchema = 'stream_crossings'
cabdSRID = 4617
mSRID = 3347
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

targetTable = args.table

sourceTables = [
    'ab_crossings.modelled_crossings',
    'bc_crossings.modelled_crossings',
    'nt_crossings.modelled_crossings',
    'on_crossings.modelled_crossings',
    'qc_crossings.modelled_crossings',
    'sk_crossings.modelled_crossings',
    'yt_crossings.modelled_crossings'
]

nbTables = []

def createTable(conn, tableName):

    sql = f"""
    DROP TABLE IF EXISTS {dbSchema}.{tableName};
    CREATE TABLE {dbSchema}.{tableName} (
        id uuid NOT NULL PRIMARY KEY,
        nhn_watershed_id varchar,
        chyf_stream_id uuid,
        stream_name_1 varchar,
        stream_name_2 varchar,
        strahler_order integer,
        geometry geometry(Point,{cabdSRID}),
        geometry_m geometry(Point,{mSRID}),
        transport_feature_id varchar,
        transport_feature_source varchar(64),
        transport_feature_type varchar,
        transport_feature_name varchar,
        crossing_subtype varchar,
        passability_status varchar,
        roadway_type varchar,
        roadway_surface varchar,
        transport_feature_owner varchar,
        railway_operator varchar,
        num_railway_tracks varchar,
        transport_feature_condition varchar,
        new_crossing_subtype varchar,
        reviewer_status varchar,
        reviewer_comments varchar,
        comments varchar(256)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def loadData(conn, table):

    schemaName = table.split('.')[0]
    tableName = table.split('.')[1]

    print(f"""Checking {schemaName}.{tableName}""")

    sql = f"""
    with cols1 AS (
        SELECT column_name FROM information_schema.columns WHERE table_schema = '{dbSchema}' AND table_name = '{targetTable}'
        ),
    cols2 AS (
        SELECT column_name FROM information_schema.columns WHERE table_schema = '{schemaName}' AND table_name = '{tableName}'
        )
    SELECT * FROM cols1 WHERE column_name IN (SELECT column_name FROM cols2);
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()

    if not result:
        pass
    else:
        colList = ""
        for row in result:
            columnName = row[0]
            if row == result[-1]:
                colList = colList + f"""{columnName}"""
            else:
                colList = colList + f"""{columnName},\n"""

        sql = f"""INSERT INTO {dbSchema}.{targetTable} ({colList})\nSELECT {colList} FROM {schemaName}.{tableName};"""

        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()

def getStructureSite(conn, tables):
    pass

def finalizeAttributes(conn, table):

    print("Finalizing attributes")
    
    sql = f"""
    UPDATE {dbSchema}.{targetTable} SET crossing_subtype =
        CASE
        WHEN new_crossing_subtype IS NOT NULL AND new_crossing_subtype != 'unknown' AND (reviewer_comments IS NULL OR reviewer_comments ILIKE '%misrepresented%') AND new_crossing_subtype != crossing_subtype
            THEN new_crossing_subtype
        WHEN new_crossing_subtype = 'unknown' AND crossing_subtype IS NOT NULL AND (reviewer_comments IS NULL OR reviewer_comments ILIKE '%misrepresented%')
            THEN NULL
        ELSE crossing_subtype END;

    UPDATE {dbSchema}.{targetTable} SET passability_status = 'passable' WHERE crossing_subtype = 'bridge' AND passability_status IS NULL;

    UPDATE {dbSchema}.{targetTable} SET passability_status = 'unknown' WHERE passability_status IS NULL;

    UPDATE {dbSchema}.{targetTable} SET crossing_subtype = 'unknown' WHERE crossing_subtype IS NULL;

    CREATE INDEX IF NOT EXISTS {targetTable}_geometry_idx ON {dbSchema}.{targetTable} USING gist(geometry);

    UPDATE {dbSchema}.{targetTable} AS a SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, a.geometry) AND nhn_watershed_id IS NULL;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def main():

    # -- MAIN SCRIPT --

    print("Connecting to database...")

    conn = pg2.connect(database=dbName,
                    user=dbUser,
                    host=dbHost,
                    password=dbPassword,
                    port=dbPort)

    createTable(conn, targetTable)

    for table in sourceTables:
        loadData(conn, table)

    getStructureSite(conn, nbTables)

    finalizeAttributes(conn, targetTable)

    print("Done!")

if __name__ == "__main__":
    main()
