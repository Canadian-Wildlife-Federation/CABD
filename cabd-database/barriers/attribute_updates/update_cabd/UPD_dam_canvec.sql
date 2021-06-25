--Source dataset: CanVec Series - Manmade features

--Update all fields
UPDATE 
    export.dams_cabd_on AS cabd
SET 
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.dam_name_fr ELSE cabd.dam_name_fr END,
    
    data_source = CASE WHEN 
                            (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) OR
                            (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL)
                       THEN cabd.data_source || ',' || origin.data_source
                       ELSE cabd.data_source END
FROM 
    load.dams_canvec AS origin
WHERE 
    cabd.cabd_id = origin.cabd_id;

--Check our work
SELECT
    origin.cabd_id,
    origin.dam_name_en,
    origin.dam_name_fr,
    origin.data_source,
    cabd.cabd_id,
    cabd.dam_name_en,
    cabd.dam_name_fr,
    cabd.data_source
FROM
    export.dams_cabd_on AS cabd,
    load.dams_canvec AS origin
WHERE
    origin.cabd_id IS NOT NULL
    AND cabd.cabd_id = origin.cabd_id;