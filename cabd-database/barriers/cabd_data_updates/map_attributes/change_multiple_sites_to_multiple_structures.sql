UPDATE featurecopy.nontidal_structures
SET 
	site_id = (SELECT DISTINCT site_id FROM featurecopy.nontidal_structures WHERE original_assessment_id = 'UK-MS-010'),
	primary_structure = false,
	structure_number = 2,
	passability_status_code = NULL
WHERE original_assessment_id = 'UK-MS-011';

DELETE FROM featurecopy.nontidal_sites
WHERE original_assessment_id = 'UK-MS-011';

--this prevents the site info accidentally being mapped from the other site later
UPDATE featurecopy.nontidal_sites_kwrc_master_2
SET cabd_id = NULL
WHERE original_assessment_id = 'UK-MS-011';

--set primary structure attributes for the site kept
UPDATE featurecopy.nontidal_structures
SET 
	primary_structure = true,
	structure_number = 1
WHERE original_assessment_id = 'UK-MS-010';

UPDATE featurecopy.nontidal_sites
SET structure_count = 2
WHERE original_assessment_id = 'UK-MS-010';

SELECT * FROM featurecopy.nontidal_structures
where original_assessment_id IN (
'UK-MS-010', 'UK-MS-011')
ORDER BY structure_number ASC;