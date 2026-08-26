-- this file contains the sql to run
-- after mainstem, elevation and smoothed elevation processes are run
-- on the chyf2 schema
-- it reconfigures the geometry column to have a type and srid


drop view chyf2.eflowpath_properties_vw;
drop view chyf2.nexus_vw;
drop view chyf2.eflowpath_vw;
   	
alter table chyf2.eflowpath alter column geometry type geometry(linestringzm, 4617);

CREATE OR REPLACE VIEW chyf2.eflowpath_properties_vw AS SELECT a.id,a.ef_type,a.ef_subtype,a.rank,a.length,a.rivernameid1,a.rivernameid2,a.aoi_id,a.from_nexus_id,a.to_nexus_id,a.ecatchment_id,a.nid,c.strahler_order,c.graph_id,c.mainstem_id,c.max_uplength,c.hack_order,c.horton_order,c.mainstem_seq,c.shreve_order,a.geometry FROM chyf2.eflowpath a JOIN chyf2.aoi b ON a.aoi_id = b.id LEFT JOIN chyf2.eflowpath_properties c ON c.id = a.id;
CREATE OR REPLACE VIEW chyf2.eflowpath_vw AS SELECT a.id,a.ef_type,a.ef_subtype,a.rank,a.length,a.rivernameid1,a.rivernameid2,a.aoi_id,a.from_nexus_id, a.to_nexus_id, a.ecatchment_id, a.nid, a.geometry FROM chyf2.eflowpath a JOIN chyf2.aoi b ON a.aoi_id = b.id WHERE b.display_status = 1;
CREATE OR REPLACE VIEW chyf2.nexus_vw AS SELECT id, nexus_type, bank_ecatchment_id, geometry FROM chyf2.nexus a WHERE (id IN ( SELECT eflowpath_vw.from_nexus_id FROM chyf2.eflowpath_vw UNION SELECT eflowpath_vw.to_nexus_id FROM chyf2.eflowpath_vw));

alter view chyf2.nexus_vw owner to chyf;
alter view chyf2.eflowpath_vw owner to chyf;
alter view chyf2.eflowpath_properties_vw owner to chyf;