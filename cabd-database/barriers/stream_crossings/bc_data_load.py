import argparse
import configparser
import getpass
import psycopg2 as pg2
from compute_stream_crossings import getStreamData

parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('--copystreams', dest='streams', action='store_true', help='stream data needs to be copied')
parser.add_argument('--ignorestreams', dest='streams', action='store_false', help='stream data is already present')
# parser.set_defaults(streams=False)
args = parser.parse_args()
configfile = args.c

#-- READ PARAMETERS FOR CONFIG FILE --
config = configparser.ConfigParser()
config.read(configfile)

#database settings - main
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

#database settings - bc
bcDbHost = '159.203.55.129'
bcDbPort = '5432'
bcDbName = 'bcfishpass'
bcDbUser = input('Enter username to access bcfishpass:\n')
bcDbPassword = getpass.getpass('Enter password to access bcfishpass:\n')

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']
mSRID  = config['SETTINGS']['mSRID']

def log(message):
    if 1:
        print(message)

#alternate ogr options
ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
#ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

print("Loading data from bcfishpass_server")

query = f"""
    DO
    $$BEGIN
        CREATE SERVER IF NOT EXISTS bcfishpass_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '{bcDbHost}', port '{bcDbPort}', dbname '{bcDbName}', sslmode 'require', extensions 'postgis');
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;$$;

    ALTER SERVER bcfishpass_server
        OWNER TO katherineo;
    GRANT USAGE ON FOREIGN SERVER bcfishpass_server TO developer;
    GRANT USAGE ON FOREIGN SERVER bcfishpass_server TO katherineo;

    CREATE USER MAPPING IF NOT EXISTS FOR public SERVER bcfishpass_server OPTIONS (user '{bcDbUser}', password '{bcDbPassword}');
"""

with conn.cursor() as cursor:
    cursor.execute(query)

query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    
    DROP FOREIGN TABLE IF EXISTS public.bc_crossings;

    CREATE FOREIGN TABLE public.bc_crossings (
        aggregated_crossings_id text NOT NULL,
        stream_crossing_id integer,
        dam_id uuid,
        user_barrier_anthropogenic_id bigint,
        modelled_crossing_id integer,
        crossing_source text,
        crossing_feature_type text,
        pscis_status text,
        crossing_type_code text,
        crossing_subtype_code text,
        modelled_crossing_type_source text[],
        barrier_status text,
        pscis_road_name text,
        pscis_stream_name text,
        pscis_assessment_comment text,
        pscis_assessment_date date,
        pscis_final_score integer,
        transport_line_structured_name_1 text,
        transport_line_type_description text,
        transport_line_surface_description text,
        ften_forest_file_id text,
        ften_file_type_description text,
        ften_client_number text,
        ften_client_name text,
        ften_life_cycle_status_code text,
        rail_track_name text,
        rail_owner_name text,
        rail_operator_english_name text,
        ogc_proponent text,
        dam_name text,
        dam_height double precision,
        dam_owner text,
        dam_use text,
        dam_operating_status text,
        linear_feature_id integer,
        blue_line_key integer,
        watershed_key integer,
        downstream_route_measure double precision,
        watershed_group_code text,
        gnis_stream_name text,
        stream_order integer,
        geom geometry(PointZM,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'bcfishpass', table_name 'crossings');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_modelled_stream_crossings;

    CREATE FOREIGN TABLE public.bc_modelled_stream_crossings (
        modelled_crossing_id integer NOT NULL,
        modelled_crossing_type character varying(5),
        modelled_crossing_type_source text[],
        transport_line_id integer,
        ften_road_section_lines_id integer,
        og_road_segment_permit_id integer,
        og_petrlm_dev_rd_pre06_pub_id integer,
        railway_track_id integer,
        linear_feature_id bigint,
        blue_line_key integer,
        downstream_route_measure double precision,
        watershed_group_code character varying(4),
        geom geometry(PointZM,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'bcfishpass', table_name 'modelled_stream_crossings');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_gba_railway_tracks_sp;
    
    CREATE FOREIGN TABLE public.bc_gba_railway_tracks_sp (
        railway_track_id integer NOT NULL,
        track_name character varying(100),
        number_of_tracks numeric,
        status character varying(20),
        operator_english_name character varying(100),
        owner_name character varying(100),
        geom geometry(MultiLineString,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'whse_basemapping', table_name 'gba_railway_tracks_sp');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_transport_line;

    CREATE FOREIGN TABLE public.bc_transport_line (
        transport_line_id integer NOT NULL,
        transport_line_type_code character varying(3),
        transport_line_surface_code character varying(1),
        structured_name_1 character varying(100),
        geom geometry(MultiLineStringZ,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'whse_basemapping', table_name 'transport_line');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_ften_road_section_lines_svw;

    CREATE FOREIGN TABLE public.bc_ften_road_section_lines_svw (
        objectid numeric,
        forest_file_id character varying(10),
        road_section_name character varying(100),
        client_name character varying(91),
        life_cycle_status_code character varying(10),
        geom geometry(MultiLineString,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'whse_forest_tenure', table_name 'ften_road_section_lines_svw');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_og_petrlm_dev_rds_pre06_pub_sp;

    CREATE FOREIGN TABLE public.bc_og_petrlm_dev_rds_pre06_pub_sp (
        og_petrlm_dev_rd_pre06_pub_id integer,
        petrlm_development_road_name character varying(50),
        proponent character varying(60),
        geom geometry(MultiLineString,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'whse_mineral_tenure', table_name 'og_petrlm_dev_rds_pre06_pub_sp');

    
    DROP FOREIGN TABLE IF EXISTS public.bc_og_road_segment_permit_spp;

    CREATE FOREIGN TABLE public.bc_og_road_segment_permit_spp (
        og_road_segment_permit_id integer,
        proponent character varying(120),
        geom geometry(MultiLineString,3005)
	)
	SERVER bcfishpass_server
    OPTIONS (schema_name 'whse_mineral_tenure', table_name 'og_road_segment_permit_sp');

    GRANT SELECT ON TABLE public.bc_crossings TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_modelled_stream_crossings TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_gba_railway_tracks_sp TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_transport_line TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_ften_road_section_lines_svw TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_og_petrlm_dev_rds_pre06_pub_sp TO PUBLIC;
    GRANT SELECT ON TABLE public.bc_og_road_segment_permit_spp TO PUBLIC;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

if args.streams:
    print("Copying streams into schema")
    getStreamData(conn)

else:
    print("Skipping copy of stream data")

conn.commit()

log("LOAD DONE")
