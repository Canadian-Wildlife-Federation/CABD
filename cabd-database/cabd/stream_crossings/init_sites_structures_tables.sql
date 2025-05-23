-- in progress - this will initial populate the stream crossings 

CREATE OR REPLACE FUNCTION init_stream_crossings() RETURNS void AS $$
BEGIN

    --1. populated sites from modelled crossings
    insert into stream_crossings.sites(
        cabd_id, data_source_code, data_source_id, municipality,
        stream_name, crossing_type_code, num_structures, site_type_code,
        crossing_comments, original_point, province_territory_code, nhn_watershed_id,
        strahler_order, assessment_type_code, addressed_status_code,
    )
    select 
    cabd_id, 'm', cabd_id, municipality, 
    stream_name_1, crossing_type_code, 1, 99, 
    comments, geometry, province_territory_code, nhn_watershed_id, 
    stream_order, assessment_type_code, addressed_status_code
    from modelled_crossings.modelled_crossings;

    --1a. populate structures from modelled crossings
    insert into stream_crossings.structures(
        structure_id, site_id, data_source_code, data_source_id,
        primary_structure, structure_number, passability_status_code
    )
    select gen_random_uuid(), cabd_id, 'm', cabd_id,
    true, 1, passability_status_code
    from modelled_crossings.modelled_crossings;

    


END;
$$;
