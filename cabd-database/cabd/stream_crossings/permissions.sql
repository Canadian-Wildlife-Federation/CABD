-- Script to assign permissions to cwf users in cabd
-- run this after all other table-creating scripts
grant select on all tables in schema stream_crossings to cwf_user;
grant select on all tables in schema cabd to cwf_user;