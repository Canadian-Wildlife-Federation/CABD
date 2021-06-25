--Source dataset: [datasetname]

UPDATE
    export.dams_cabd AS cabd
SET
    [column1] = CASE WHEN (cabd.[column1] IS NULL AND origin.[column1] IS NOT NULL) THEN origin.[column1] ELSE cabd.[column1] END,
    [column2] = CASE WHEN (cabd.[column2] IS NULL AND origin.[column2] IS NOT NULL) THEN origin.[column2] ELSE cabd.[column2] END,

    data_source = CASE WHEN 
                            (cabd.[column1] IS NULL AND origin.[column1] IS NOT NULL) OR
                            (cabd.[column2] IS NULL AND origin.[column2] IS NOT NULL)
                       THEN cabd.data_source || ',' || origin.data_source
                       ELSE cabd.data_source END
FROM
    load.dams_[datasetid] AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;