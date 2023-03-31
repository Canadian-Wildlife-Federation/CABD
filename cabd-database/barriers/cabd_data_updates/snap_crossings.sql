create index nb_data_eflowpath_geometry_idx on nb_data.eflowpath using gist(geometry);
create index nb_data_nrhn_streams_geometry_idx on nb_data.nrhn_streams using gist(geometry);

create index featurecopy_tidal_orig_point_idx on featurecopy.tidal_sites using gist(original_point);
create index featurecopy_nontidal_orig_point_idx on featurecopy.nontidal_sites using gist(original_point);

CREATE OR REPLACE FUNCTION stream_crossings.snap_crossings_to_network(
	src_schema character varying,
	src_table character varying,
	src_id_field character varying,
	raw_geom character varying,
	snapped_geom character varying,
	snap_to_schema character varying,
	snap_to_table character varying,
	snap_to_geometry character varying,
	max_distance_m double precision)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   pnt_rec RECORD;
   fp_rec RECORD;
BEGIN

        FOR pnt_rec IN EXECUTE format('SELECT %I as id, %I as rawg FROM %I.%I WHERE %I is not null', src_id_field, raw_geom, src_schema,
src_table,raw_geom)
        LOOP
                --RAISE NOTICE '%s: %s', pnt_rec.cabd_id, pnt_rec.rawg;
                FOR fp_rec IN EXECUTE format ('SELECT %I as geometry, st_distance(%L::geometry::geography, %I::geography) AS distance
                        FROM %I.%I WHERE st_expand(%L::geometry, 0.01) && %I and st_distance(%L::geometry::geography, %I::geography) < %s
                        ORDER BY distance ', snap_to_geometry, pnt_rec.rawg, snap_to_geometry, snap_to_schema, snap_to_table,
                        pnt_rec.rawg, snap_to_geometry, pnt_rec.rawg, snap_to_geometry,
max_distance_m)
                LOOP
                        EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry,
%L::geometry) )
                                WHERE %I = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, src_id_field,
                                pnt_rec.id);
                        --RAISE NOTICE '%s', fp_rec.distance;
                        EXIT;

                END LOOP;
        END LOOP;
END;
$BODY$;

ALTER FUNCTION stream_crossings.snap_crossings_to_network(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, double precision)
    OWNER TO cabd;

-- syntax
-- select stream_crossings.snap_crossings_to_network(
-- src_schema, src_table, src_id_field,
-- raw_geom, snapped_geom,
-- snap_to_schema, snap_to_table, snap_to_geometry, max_distance_m);

--snap to nhn

update stream_crossings.nontidal_sites
set snapped_point = NULL;

select stream_crossings.snap_crossings_to_network(
    'stream_crossings', 'nontidal_sites', 'cabd_id', 'original_point', 'snapped_point', 'nb_data', 'eflowpath', 'geometry', 20
    );

update stream_crossings.tidal_sites
set snapped_point = NULL;

select stream_crossings.snap_crossings_to_network(
    'stream_crossings', 'tidal_sites', 'cabd_id', 'original_point', 'snapped_point', 'nb_data', 'eflowpath', 'geometry', 20
    );

--snap to provincial network

update stream_crossings.nontidal_sites
set snapped_point_prov = NULL;

select stream_crossings.snap_crossings_to_network(
    'stream_crossings', 'nontidal_sites', 'cabd_id', 'original_point', 'snapped_point_prov', 'nb_data', 'nbhn_streams', 'geometry', 20
    );

update stream_crossings.tidal_sites
set snapped_point_prov = NULL;

select stream_crossings.snap_crossings_to_network(
    'stream_crossings', 'tidal_sites', 'cabd_id', 'original_point', 'snapped_point_prov', 'nb_data', 'nbhn_streams', 'geometry', 20
    );