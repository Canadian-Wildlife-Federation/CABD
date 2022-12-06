--fix up this view as the previous definition was missing headwater or sink nodes
CREATE OR REPLACE VIEW chyf2.nexus_vw
AS select a.id,
    a.nexus_type,
    a.bank_ecatchment_id,
    a.geometry
FROM chyf2.nexus a
WHERE a.id IN (
   SELECT from_nexus_id FROM chyf2.eflowpath_vw UNION  
   SELECT to_nexus_id FROM chyf2.eflowpath_vw 
)