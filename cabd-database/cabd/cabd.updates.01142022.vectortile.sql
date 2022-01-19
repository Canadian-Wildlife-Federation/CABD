DROP table cabd.vector_tile_cache;
CREATE TABLE cabd.vector_tile_cache (
	"key" varchar(32) NOT NULL,
	tile bytea NULL,
	last_accessed timestamp default now(),
	CONSTRAINT vector_tile_cache_pkey PRIMARY KEY (key)
);

create function clear_cache() returns trigger as $a$ begin truncate cabd.vector_tile_cache; return NULL; end; $a$ language plpgsql ;

CREATE TRIGGER trg_clear_cache_dams AFTER INSERT or update or delete ON dams.dams FOR EACH STATEMENT execute procedure clear_cache();
CREATE TRIGGER trg_clear_cache_fw AFTER INSERT or update or delete ON fishways.fishways FOR EACH STATEMENT execute procedure clear_cache();
CREATE TRIGGER trg_clear_cache_waterfalls AFTER INSERT or update or delete ON waterfalls.waterfalls FOR EACH STATEMENT execute procedure clear_cache();

-- add a include_vector_tile column to the metadata to flag
-- which fields to include in the vector tiles
alter table cabd.feature_type_metadata add column include_vector_tile boolean default false;

update cabd.feature_type_metadata set include_vector_tile = true 
where 
(view_name = 'cabd.dams_view' and field_name = 'dam_name_en') or
(view_name = 'cabd.dams_view' and field_name = 'dam_name_fr') or
(view_name = 'cabd.dams_view' and field_name = 'passability_status_code') or
(view_name = 'cabd.dams_view' and field_name = 'passability_status') or
(view_name = 'cabd.dams_view' and field_name = 'province_territory_code') or
(view_name = 'cabd.dams_view' and field_name = 'province_territory') or
(view_name = 'cabd.dams_view' and field_name = 'operating_status_code') or
(view_name = 'cabd.dams_view' and field_name = 'operating_status') or
(view_name = 'cabd.dams_view' and field_name = 'ownership_type') or
(view_name = 'cabd.dams_view' and field_name = 'use_code') or
(view_name = 'cabd.dams_view' and field_name = 'dam_use') or
(view_name = 'cabd.dams_view' and field_name = 'size_class_code') or
(view_name = 'cabd.dams_view' and field_name = 'size_class') or
(view_name = 'cabd.dams_view' and field_name = 'height_m') or
(view_name = 'cabd.dams_view' and field_name = 'up_passage_type') or
(view_name = 'cabd.dams_view' and field_name = 'up_passage_type_code') or
(view_name = 'cabd.dams_view' and field_name = 'construction_year') or
(view_name = 'cabd.dams_view' and field_name = 'feature_type') or
(view_name = 'cabd.dams_view' and field_name = 'cabd_id') or

(view_name = 'cabd.waterfalls_view' and field_name = 'passability_status') or
(view_name = 'cabd.waterfalls_view' and field_name = 'passability_status_code') or
(view_name = 'cabd.waterfalls_view' and field_name = 'fall_height_m') or
(view_name = 'cabd.waterfalls_view' and field_name = 'province_territory_code') or
(view_name = 'cabd.waterfalls_view' and field_name = 'province_territory') or
(view_name = 'cabd.waterfalls_view' and field_name = 'fall_name_en') or
(view_name = 'cabd.waterfalls_view' and field_name = 'fall_name_fr') or
(view_name = 'cabd.waterfalls_view' and field_name = 'feature_type') or
(view_name = 'cabd.waterfalls_view' and field_name = 'cabd_id') or

(view_name = 'cabd.fishways_view' and field_name = 'feature_type') or
(view_name = 'cabd.fishways_view' and field_name = 'province_territory_code') or
(view_name = 'cabd.fishways_view' and field_name = 'province_territory') or
(view_name = 'cabd.fishways_view' and field_name = 'fishpass_type') or
(view_name = 'cabd.fishways_view' and field_name = 'fishpass_type_code') or
(view_name = 'cabd.fishways_view' and field_name = 'year_constructed') or
(view_name = 'cabd.fishways_view' and field_name = 'known_use') or
(view_name = 'cabd.fishways_view' and field_name = 'dam_id') or
(view_name = 'cabd.fishways_view' and field_name = 'cabd_id') or

(view_name = 'cabd.barriers_view' and field_name = 'cabd_id') or
(view_name = 'cabd.barriers_view' and field_name = 'name_en') or
(view_name = 'cabd.barriers_view' and field_name = 'name_fr') or
(view_name = 'cabd.barriers_view' and field_name = 'barrier_type') or
(view_name = 'cabd.fishways_view' and field_name = 'province_territory_code') or
(view_name = 'cabd.fishways_view' and field_name = 'province_territory') or
(view_name = 'cabd.waterfalls_view' and field_name = 'passability_status') or
(view_name = 'cabd.waterfalls_view' and field_name = 'passability_status_code');

