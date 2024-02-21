create table cabd.attribute_set(name varchar, ft_metadata_col varchar, primary key (name));

insert into cabd.attribute_set(name, ft_metadata_col) values 
('all', 'vw_all_order'),
('limited', 'vw_simple_order'),
('mobile', 'vw_mobile_order');

alter table cabd.feature_type_metadata add column vw_mobile_order integer;

update cabd.feature_type_metadata set vw_mobile_order = 1 whree field_name = 'cabd_id';
update cabd.feature_type_metadata set vw_mobile_order = 2 whree field_name = 'passability_status';
update cabd.feature_type_metadata set vw_mobile_order = 3 whree field_name = 'feature_type';


-- should ensure only geometry columns don't show up in this query
select * from cabd.feature_type_metadata where vw_all_order is null;