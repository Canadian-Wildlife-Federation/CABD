--Source dataset: Ontario Dam Inventory

UPDATE
    export.dams_cabd_on AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    "owner" = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.ownership_type_code ELSE cabd.ownership_type_code END,
    comments = CASE WHEN (cabd.comments IS NULL AND origin.comments IS NOT NULL) THEN origin.comments ELSE cabd.comments END,

    data_source = CASE WHEN 
                            (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) OR
                            (cabd.owner IS NULL AND origin.owner IS NOT NULL) OR
                            (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) OR
                            (cabd.comments IS NULL AND origin.comments IS NOT NULL)
                       THEN cabd.data_source || ',' || origin.data_source
                       ELSE cabd.data_source END
FROM
    load.dams_odi AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--Check our work
SELECT
    origin.cabd_id,
    origin.dam_name_en,
    origin.owner,
    origin.ownership_type_code,
    origin.comments,
    origin.data_source,
    cabd.cabd_id,
    cabd.dam_name_en,
    cabd.owner,
    cabd.ownership_type_code,
    cabd.comments,
    cabd.data_source
FROM
    export.dams_cabd_on AS cabd,
    load.dams_odi AS origin
WHERE
    origin.cabd_id IS NOT NULL
    AND origin.dam_name_en IS NOT NULL
    AND cabd.cabd_id = origin.cabd_id;