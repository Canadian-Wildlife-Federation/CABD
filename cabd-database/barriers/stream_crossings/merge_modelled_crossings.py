import argparse
import getpass
import psycopg2 as pg2

#-- PARSE COMMAND LINE ARGUMENTS --
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-table', type=str, help='the table to hold final data', required=True)
args = parser.parse_args()

#database settings
dbHost = 'cabd-postgres.postgres.database.azure.com'
dbPort = '5432'
dbName = 'cabd'
dbSchema = 'modelled_crossings'
cabdSRID = 4617
mSRID = 3347
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

targetTable = args.table

sourceTables = [
    'ab_crossings.modelled_crossings',
    'bc_crossings.modelled_crossings',
    'mb_crossings.modelled_crossings',
    'nb_crossings.modelled_crossings',
    'nl_crossings.modelled_crossings',
    'ns_crossings.modelled_crossings',
    'nt_crossings.modelled_crossings',
    'nu_crossings.modelled_crossings',
    'on_crossings.modelled_crossings',
    'pe_crossings.modelled_crossings',
    'qc_crossings.modelled_crossings',
    'sk_crossings.modelled_crossings',
    'yt_crossings.modelled_crossings'
]

def renameColumns(conn, table):

    print(f"""Renaming columns from {table}""")

    sql = f"""
    ALTER TABLE {table} RENAME COLUMN id TO cabd_id;
    ALTER TABLE {table} RENAME COLUMN strahler_order TO stream_order;
    ALTER TABLE {table} RENAME COLUMN transport_feature_condition TO railway_status;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def createTable(conn, tableName):

    print(f"""Creating {dbSchema}.{tableName}""")

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {dbSchema};

    DROP TABLE IF EXISTS {dbSchema}.{tableName};
    CREATE TABLE {dbSchema}.{tableName} (
        cabd_id uuid NOT NULL PRIMARY KEY, --renamed from id
        nhn_watershed_id varchar(7),
        municipality varchar(512),
        province_territory_code varchar(2),
        chyf_stream_id uuid,
        stream_name_1 varchar,
        stream_name_2 varchar,
        stream_order integer, --renamed from strahler_order
        geometry geometry(Point,{cabdSRID}),
        geometry_m geometry(Point,{mSRID}),
        transport_feature_id varchar,
        transport_feature_source varchar(64),
        transport_feature_type varchar,
        transport_feature_name varchar,
        crossing_type varchar,
        crossing_subtype varchar,
        crossing_subtype_source varchar,
        passability_status varchar,
        roadway_type varchar,
        roadway_surface varchar,
        transport_feature_owner varchar,
        railway_operator varchar,
        num_railway_tracks varchar,
        railway_status varchar, --renamed from transport_feature_condition
        new_crossing_subtype varchar,
        reviewer_status varchar,
        reviewer_comments varchar,
        comments varchar(256),
        use_analysis boolean
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def loadData(conn, table):

    schemaName = table.split('.')[0]
    tableName = table.split('.')[1]

    print(f"""Loading data from {schemaName}.{tableName}""")

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

def createArchive(conn, table):

    print("Archiving table with all original comments")

    sql = f"""
    DROP TABLE IF EXISTS {dbSchema}.modelled_crossings_archive;
    CREATE TABLE {dbSchema}.modelled_crossings_archive AS (SELECT * FROM {dbSchema}.{table});
    """

    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def finalizeAttributes(conn, table):

    print("Finalizing attributes")

    sql = f"""
    UPDATE {dbSchema}.{table} SET crossing_subtype_source =
        CASE
        WHEN new_crossing_subtype IS NOT NULL AND new_crossing_subtype != crossing_subtype THEN 'cwf'
        WHEN new_crossing_subtype IS NOT NULL AND crossing_subtype IS NULL THEN 'cwf'
        ELSE crossing_subtype_source END;

    UPDATE {dbSchema}.{table} SET crossing_subtype =
        CASE
        WHEN new_crossing_subtype IS NOT NULL AND new_crossing_subtype != crossing_subtype THEN new_crossing_subtype
        WHEN new_crossing_subtype IS NOT NULL AND crossing_subtype IS NULL THEN new_crossing_subtype
        ELSE crossing_subtype END;

    UPDATE {dbSchema}.{table} SET crossing_type =
        CASE
        WHEN crossing_subtype = 'bridge' THEN 'open-bottom structure'
        WHEN crossing_subtype IN ('culvert', 'multiple culvert') THEN 'closed-bottom structure'
        WHEN crossing_subtype = 'ford' THEN 'ford-like structure'
        ELSE NULL END;

    UPDATE {dbSchema}.{table} SET passability_status = 'passable' WHERE crossing_subtype IN ('bridge', 'no crossing', 'removed crossing');
    UPDATE {dbSchema}.{table} SET passability_status = 'unknown' WHERE passability_status IS NULL;

    UPDATE {dbSchema}.{table} SET crossing_subtype = 'unknown' WHERE crossing_subtype IS NULL;

    UPDATE {dbSchema}.{table} SET use_analysis = CASE WHEN crossing_subtype = 'no crossing' THEN false ELSE true END;

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = replace(crossing_subtype_source, 'crossing subtype set based on match from ', '')
    WHERE crossing_subtype_source ILIKE 'crossing subtype set based on match from%';
        
    UPDATE {dbSchema}.{table} SET reviewer_comments = crossing_subtype_source
    WHERE crossing_subtype_source ILIKE 'Automatically removed crossing%'
    AND reviewer_comments IS NULL;

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = regexp_replace(crossing_subtype_source, '^.* ', '')
    WHERE crossing_subtype_source ILIKE 'Automatically removed crossing%';

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = regexp_replace(crossing_subtype_source, '^.* ', '')
    WHERE crossing_subtype_source ILIKE 'crossing subtype set%'
    AND crossing_subtype_source NOT ILIKE '%strahler order%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Automatically removed crossing on dry channel based on match from bridges_2005_inventory'
    WHERE crossing_subtype_source = 'bridges_2005_inventory indicates there is no water flowing here';

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = 'bridges_2005_inventory'
    WHERE crossing_subtype_source = 'bridges_2005_inventory indicates there is no water flowing here';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Automatically removed crossing based on match from mnrf_segment'
    WHERE crossing_subtype_source = 'mnrf_segment indicated a crossing was removed';

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = 'mnrf_segment'
    WHERE crossing_subtype_source = 'mnrf_segment indicated a crossing was removed';

    UPDATE {dbSchema}.{table} SET crossing_subtype_source = 'stream order'
    WHERE crossing_subtype_source = 'crossing subtype set based on strahler order';

    ALTER TABLE {dbSchema}.{table} ADD COLUMN multipoint boolean;
    UPDATE {dbSchema}.{table} SET multipoint = true WHERE reviewer_comments ILIKE '%multipoint%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = NULL
    WHERE reviewer_comments ILIKE 'multipoint%'
    OR reviewer_comments IN ('Potentially a multipoint?', 'Potentially multipoint.');

    UPDATE {dbSchema}.{table} SET reviewer_comments = REPLACE(reviewer_comments, ' Multipoint', '')
    WHERE reviewer_comments LIKE '% Multipoint';

    UPDATE {dbSchema}.{table} SET reviewer_comments = NULL
    WHERE (regexp_match(reviewer_comments, '(?i)(looks like)|(may be)|(appears to be)|(could be)|(likely)|(possibly)|(potential)|(good chance)')) IS NOT NULL
    AND reviewer_comments NOT ILIKE 'stream or road network out of date%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = REPLACE(reviewer_comments, 'Stream was misrepresented', 'Stream or road network out of date')
    WHERE reviewer_comments ILIKE 'stream was misrepresented%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = REPLACE(reviewer_comments, 'Stream diverted', 'Stream or road network out of date')
    WHERE reviewer_comments ILIKE 'stream diverted%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = REPLACE(reviewer_comments, 'Stream misinterpreted', 'Stream or road network out of date')
    WHERE reviewer_comments LIKE 'Stream misinterpreted%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = REPLACE(reviewer_comments, 'stream misrepresented', 'Stream or road network out of date')
    WHERE reviewer_comments LIKE 'stream misrepresented%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Stream or road network out of date, ' || reviewer_comments
    WHERE reviewer_comments ILIKE 'feature located at%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Stream or road network out of date, ' || reviewer_comments
    WHERE reviewer_comments ILIKE 'actual bridge location%' OR reviewer_comments ILIKE 'actual crossing location%' OR reviewer_comments ILIKE 'actual culvert location%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Bridge removed' WHERE (regexp_match(reviewer_comments, '(?i)(bridge.*removed)')) is not null;

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Duplicate' WHERE (regexp_match(reviewer_comments, '(?i)(already represented)')) is not null;

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Duplicate' WHERE reviewer_comments ilike '%duplicate';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'Driveway culvert along a drainage ditch' WHERE reviewer_comments ILIKE '%Driveway culvert along a drainage ditch%';

    UPDATE {dbSchema}.{table} SET reviewer_comments = 'No crossing structure here'
        WHERE reviewer_comments = 'There does not seem to be a stream crossing structure here.'
        OR reviewer_comments ilike '%there doesn''t appear to be a crossing here%';
    
    UPDATE {dbSchema}.{table} SET comments = reviewer_comments;

    UPDATE {dbSchema}.{table} SET crossing_type =
        CASE
        WHEN crossing_subtype = 'bridge' THEN 'open-bottom structure'
        WHEN crossing_subtype IN ('culvert', 'multiple culvert') THEN 'closed-bottom structure'
        WHEN crossing_subtype = 'ford' THEN 'ford-like structure'
        ELSE 'unknown' END;

    CREATE INDEX IF NOT EXISTS {table}_geometry_idx ON {dbSchema}.{table} USING gist(geometry);
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

def joinLocation(conn, table):

    print("Joining location data to crossings")

    sql = f"""
    UPDATE {dbSchema}.{table} AS a SET province_territory_code = n.code FROM cabd.province_territory_codes AS n WHERE st_contains(n.geometry, a.geometry) AND province_territory_code IS NULL;
    UPDATE {dbSchema}.{table} AS a SET nhn_watershed_id = n.id FROM cabd.nhn_workunit AS n WHERE st_contains(n.polygon, a.geometry) AND nhn_watershed_id IS NULL;
    UPDATE {dbSchema}.{table} AS a SET municipality = n.csdname FROM cabd.census_subdivisions AS n WHERE st_contains(n.geometry, a.geometry) AND municipality IS NULL;
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

        schemaName = table.split('.')[0]
        tableName = table.split('.')[1]

        sql = f"""
            SELECT column_name FROM information_schema.columns WHERE table_schema = '{schemaName}' AND table_name = '{tableName}' AND column_name = 'id'
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

        if result:
            renameColumns(conn, table)
            loadData(conn, table)
        else:
            loadData(conn, table)

    createArchive(conn, targetTable)
    finalizeAttributes(conn, targetTable)
    joinLocation(conn, targetTable)

    print("Done!")

if __name__ == "__main__":
    main()
