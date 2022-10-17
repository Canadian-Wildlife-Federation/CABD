# Who are these scripts intended for?

These scripts are generally intended for use by CWF staff or by parties who are interested in setting up a database similar to the CABD for their own purposes.  

The following sections will detail methods used by CWF staff for processing and releasing barrier data. For more information on data compilation methods used, please visit the [Data Processing Methods](https://cabd-docs.netlify.app/docs_tech/docs_tech_feature_review.html) page on the CABD documentation site.

# General method for processing and releasing barrier data

1. Choose an area of interest and identify all the spatial datasets available for that area.  
2. Create a feature review geopackage with all the necessary fields. This geopackage can be pre-populated using various geoprocessing tools in QGIS or other GIS software.
3. Upload the feature review geopackage to the DB using the LOAD_`<featureType>`_review.py script (e.g., LOAD_dam_review.py)
4. Carry out manual review and de-duplication of features.
5. Run the `<featureType>`_feature_attribute_source script for your feature type.
6. Confirm all spatial datasets for your area of interest have been imported to the database. If not, import them using the LOAD_`<datasetid>`.py scripts.
7. Map attributes from spatial datasets using the MAP_`<featureType>`_`<datasetid>`.py scripts (e.g., MAP_dams_aep_bf_hy.py)
8. Check your work and make any corrections needed to attributes for your feature type (e.g., resolving inconsistent use codes or populating ownership type codes).
9. Run the `<featureType>`_final_attribute_update script (e.g., dams_final_attribute_update.py).
10. Run the `<featureType>`_copy_to_production script (e.g., dams_copy_to_production.py).

# Load datasets

- The Python scripts in the load_dataset folder load spatial datasets into the CWF database and map their attributes to the CABD data structure.
- Each data source has its own script. The scripts for data sources with multiple feature types (e.g., dams, waterfalls, and fishways) will split these data sources into separate tables for each feature type.
- There are also three scripts in this folder which load data points created by CWF staff instead of by external organizations:
    - LOAD_dam_review.py
    - LOAD_fishway_review.py
    - LOAD_waterfall_review.py
    - The data points used by these three scripts are de-duplicated points which represent each individual dam, waterfall, and fishway feature as a single point and record the different datasets this feature was found in.

## Load external spatial datasets

- Spatial datasets can be loaded into the CABD at any time during the data compilation process. The scripts to load spatial datasets assign a data_source value to all rows which references a uuid primary key for the corresponding record for that dataset in the cabd.data_source table. If a record for the dataset does not exist in this table, users may create a record and uuid in the cabd.data_source table before running the script, or modify the script to generate a record and uuid during the load process.
- Users should review the "date" field for spatial datasets in the cabd.data_source table before running these scripts. If your dataset is already present in the database, and has not been updated since it was first loaded, there is no need to re-upload the dataset.
- Each of these scripts has been tailored for the individual spatial datasets used as input to the CABD and the specific CABD data structure.
- The scripts will load information from spatial datasets into two schemas:
    1. A source_data schema, where the corresponding table will contain all feature types and original field names and values from the spatial dataset. Example table name: source_data.nrcan_nhn
    2. A working data schema, where the corresponding table (or tables, for multiple feature types) will only contain the fields and field values that can be mapped to the CABD data structure. The field names and values in this table will match the valid field names and values in the CABD. Example table name: featurecopy.dams_nrcan_nhn

### Scripts:
- LOAD_`<datasetid>`.py

### Script requirements: 
- Python 3.9.5 or newer, OGR tools, psycopg2
- Note for Windows users: QGIS installations come with a version of the ogr tools that can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

### To run these scripts:
- Set the following variables in LOAD_main.py:
    - ogr
    - dbHost
    - dbPort
    - dbName
    - sourceSchema (where the original spatial dataset should be stored)
    - workingSchema (where the table containing fields and field values that match the CABD should be stored)
    - speciesMapping (a table required for the cwf_canfish dataset that stores individual species names and assigns a unique id to each)
- Run the script providing the spatial dataset file and your database username and password as arguments:
    - py LOAD_`<datasetid>`.py "`<dataFile>`" `<dbUser>` `<dbPassword>`
        - e.g., py LOAD_aep_bf_hy.py "C:\temp\aep_bf_hy.gpkg" postgres sql 

## Load feature review files

*Please note that the scripts to load feature review files have only been tested using geopackages as the input file type.*

- Feature review files can be loaded into the database during the data compilation process to allow concurrent editing, or uploaded once data compilation is complete. Please see the [Data Processing Methods](https://cabd-docs.netlify.app/docs_tech/docs_tech_feature_review.html) page on the CABD documentation site for more details on the feature review process.

### Scripts:
- LOAD_dam_review.py
- LOAD_fishway_review.py
- LOAD_waterfall_review.py

### Feature review file requirements:
- A data_source_text field which contains a single spatial dataset name for each feature. The values in this field must match the "name" field for that dataset in the cabd.data_source table
    - Example: data_source_text = 'aep_bf_hy'
- A data_source_id field which contains the corresponding unique id for the feature from the spatial dataset specified in the data_source_text field.
    - Example: data_source_id = '123', where '123' is the unique id for a dam from the aep_bf_hy spatial dataset.
- Fields for each spatial dataset used in the data compilation process. Field names must match the "name" field for that dataset in the cabd.data_source table. Fields must contain the corresponding unique id for the feature from the spatial dataset specified in the field name.
    - Example: aep_bf_hy = '123'
- Point geometry for all features. **Note:** multipoints are not supported in the CABD at this time.

### Script requirements:
- Python 3.9.5 or newer, OGR tools, psycopg2
- Note for Windows users: QGIS installations come with a version of the ogr tools that can be used for this script (ex. C:\Program Files\QGIS 3.12\bin\ogr2ogr.exe)

### To run these scripts:
- Set the following variables in LOAD_`<featureType>`_review.py:
    - ogr
    - dbHost
    - dbPort
    - dbName
    - workingSchema (where your data is stored separately from live data)
- Run the script providing the feature review file, a province or territory code, and your database username and password as arguments:
    - py LOAD_`<featureType>`_review.py "`<dataFile>`" `<provinceCode>` `<dbUser>` `<dbPassword>`
        - e.g., py LOAD_dam_review.py "C:\temp\atlantic_dam_review.gpkg" atlantic postgres sql
    - Note: as geopackages can contain multiple tables, the provinceCode argument has been used to ensure the correct table is being imported. If your geopackage only contains a single table, users can modify the script to remove this variable.

## Populate the feature_source and attribute_source tables

These scripts should be run AFTER the data compilation process is complete, but BEFORE attributes are mapped. These scripts will add any missing rows from the feature_source and attribute_source tables and populate columns for these rows where appropriate.

The `<featureType>`_feature_source table records which spatial datasets each feature was found in.

The `<featureType>`_attribute_source table records which dataset (spatial or non-spatial) the attributes for each feature came from.

### Scripts:
- dams_feature_attribute_source.py
- fishways_feature_attribute_source.py
- waterfalls_feature_attribute_source.py

### Script technical requirements:
- Python 3.9.5 or newer, psycopg2

### Script data requirements:
- A feature review table has been loaded and completely populated for the feature type.
- The target table contains all the corresponding fields required for that feature type by the CABD data structure.
- The target table includes fields that records the unique ids from each spatial dataset that a feature was found in.

### To run these scripts:
- Confirm the following variables are set to the correct values in `<featureType>`_feature_attribute_source.py:
    - dbHost
    - dbPort
    - dbName
    - workingSchema (where your data is stored separately from live data)
- Run the script providing your database username and password as arguments:
    - py MAP_`<featureType>`_feature_attribute_source.py `<dbUser>` `<dbPassword>`
        - e.g., py dams_feature_attribute_source.py dams postgres sql

# Map attributes

- The Python scripts in the map_attributes folder will populate attributes for CABD features from spatial datasets.
- These scripts should only be run once the data compilation process is complete.
- The data compilation process is considered complete when the feature review tables contain points for each feature in the area of interest, and contain the unique ids from each spatial dataset a feature was found in.
- These scripts will do the following:
    - Match a cabd_id to rows in the spatial dataset table to rows from the feature review table, based on the unique ids provided in the feature review table
    - Record which attributes for a given feature come from each spatial dataset in the `<featureType>`_attribute_source table
    - Update the attributes for rows in the feature review table with the corresponding values from the spatial dataset table
- Because the scripts are designed to only populate NULL values, we recommend establishing a sequence to run the map_attributes scripts so that information from more reliable datasets is populated before information from less reliable or older datasets. For example, the CABD sequence populates information from Canadian government sources before populating information from Wikipedia.

### Scripts:
- MAP_attributes_main.py
- MAP_`<featureType>`_`<datasetid>`.py

### Script technical requirements:
- Python 3.9.5 or newer, psycopg2

### Script data requirements:
- The spatial dataset has been loaded into the database.
- A target table exists for the feature type that attributes will be mapped to (e.g., the dam review table loaded using `LOAD_dam_review.py`)
- The target table contains all the corresponding fields required for that feature type by the CABD data structure.
- The target table includes fields that records the unique ids from each spatial dataset that a feature was found in.

### To run these scripts:
- Set the following variables in MAP_attributes_main.py:
    - dbHost
    - dbPort
    - dbName
    - workingSchema (where your data is stored separately from live data)
- Run the script providing the feature type to be mapped to and your database username and password as arguments:
    - py MAP_`<featureType>`_`<datasetid>`.py `<featureType>` `<dbUser>` `<dbPassword>`
        - e.g., py MAP_dams_aep_bf_hy.py dams postgres sql
    - featureType must be one of 'dams', 'waterfalls', or 'fishways'.

# Non-spatial datasets

- The Python scripts in the nonspatial folder will populate attributes for CABD features from non-spatial datasets.
- These work very similarly to the scripts in the map_attributes folder, but have different data requirements as they are designed to be run on live data and their inputs generally do not contain any spatial information.

### Scripts:
- nonspatial.py
- `<datasetName>`.py

### Script technical requirements:
- Python 3.9.5 or newer, psycopg2

### Script data requirements:
- The non-spatial dataset has been loaded into the database.
- A cabd_id is present for all rows in the non-spatial dataset, and matches a cabd_id for a feature in the live data tables.
- The field names and values in the non-spatial dataset table match those required by the CABD data structure.
- The non-spatial dataset table only contains one feature type.

### To run these scripts:
- Set the following variables in nonspatial.py:
    - dbHost
    - dbPort
    - dbName
    - sourceSchema (where your non-spatial dataset table is saved)
    - liveDamSchema
    - liveFishSchema
- Confirm the information in Line 9 of the `<datasetName>`.py script matches what should be inserted to the cabd.data_source table.
- Run the script providing your database username and password as arguments:
    - py `<datasetName>`.py `<dbUser>` `<dbPassword>`

# Additional scripts

- `<featureType>`_feature_attribute_source scripts should be run after the data compilation process is complete and BEFORE running any scripts in the map_attributes folder.
- `<featureType>`_final_attribute_update scripts should be run after the scripts in the map_attributes folder.
- `<featureType>`_copy_to_production scripts should be run after the final_attribute_update scripts and will push data from the working data schema to the live data schemas.
- The fishway_associations script can be run at any time, and will associate fishways with dams based on a user-defined distance, and populate the passability_status_code and up_passage_type_code fields for dams. This script operates on the live data tables.