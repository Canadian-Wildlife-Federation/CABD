-- cabd.community_data_staging_view source

CREATE OR REPLACE VIEW cabd.community_data_staging_view
AS SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'stream_crossings'::text AS feature_type,
        CASE
            WHEN b.status IS NULL THEN 'NEW'::character varying
            ELSE b.status::character varying
        END AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code,
        CASE
            WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
            WHEN c.original_point IS NOT NULL THEN c.original_point
            ELSE st_transform(st_setsrid(st_geomfromgeojson(a.data -> 'geometry'::text), 4326), 4617)
        END AS feature_geometry
   FROM stream_crossings.stream_crossings_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id
     LEFT JOIN stream_crossings.sites c ON c.cabd_id = a.cabd_id
UNION
 SELECT a.id,
    a.cabd_id,
    a.user_id,
    a.uploaded_datetime,
    a.data,
    'dams'::text AS feature_type,
    CASE
            WHEN b.status IS NULL THEN 'NEW'::character varying
            ELSE b.status::character varying
        END AS status,
        CASE
            WHEN b.passability_status_code IS NULL THEN 4
            ELSE b.passability_status_code::integer
        END AS passability_status_code,
        CASE
            WHEN c.snapped_point IS NOT NULL THEN c.snapped_point
            WHEN c.original_point IS NOT NULL THEN c.original_point
            ELSE st_transform(st_setsrid(st_geomfromgeojson(a.data -> 'geometry'::text), 4326), 4617)
        END AS feature_geometry
   FROM dams.dams_community_staging a
     LEFT JOIN cabd.community_holding b ON a.id = b.id
     LEFT JOIN dams.dams c ON c.cabd_id = a.cabd_id;