-- this update is a request to remove the urls for the dam ids at this time (may add back later)
update cabd.feature_type_metadata set data_type = 'uuid', is_link = false where view_name = 'cabd.fishways_view' and field_name = 'dam_id';
