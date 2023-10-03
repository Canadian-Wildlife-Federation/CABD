# filter through CABD dam updates to check duplicates
# make sure to use an exported csv from the CABD data updates form

# usage: dams_dup_check.py <filepath>

import pandas as pd
import sys

# optional - better display of dataframes
pd.options.display.max_columns = None
pd.options.display.max_rows = None

dataFile = ""
dataFile = sys.argv[1]

data = pd.read_csv(dataFile)

# only check unprocessed updates
df = data[data['status'] != 'complete']

# list of columns that will usually have conflicting info
colskip = ['submitted_on', 'email', 'latitude', 'longitude', 'province_territory_code', 'entry_classification', 'data_source_short_name', 'use_analysis', 'Name', 'Organization', 'status', 'release_version']

# empty list for iteration
cabd = []

# filter down to only cabd_ids with duplicates, where cabd_ids are not null, and reset index
duplicates = df[df.duplicated('cabd_id', keep=False)]
duplicates = duplicates[duplicates['cabd_id'].notna()]
duplicates = duplicates.sort_values(by='cabd_id', ascending=True)
duplicates = duplicates.reset_index()
duplicates = duplicates.drop(columns=['index'])

duplicates.to_csv('duplicates.csv')

# find non unique entries for specified column for the same cabd_id
# TO DO: ignore nan values in columns - these should not be included as "duplicates"
for (colname, colval) in duplicates.items():

    if colname in colskip:
        continue
    
    else:
        df2 = duplicates[duplicates.groupby('cabd_id')[colname].transform('nunique').gt(1)] # TO DO: figure out why this is not properly grouping by cabd_id

        cabd.append(
            {
                'cabd_id': df2['cabd_id'].values.tolist(),
                'email': df2['email'].values.tolist(),
                'colname': colname,
                'colvalues': df2[colname].values.tolist()
            }
        )

df3 = pd.DataFrame(cabd)

#list of cabd_ids with conflicts and column values
df_filtered = df3[df3['cabd_id'].apply(len) > 0] # ignore any updates where length of cabd_id column is 0
df_filtered = df_filtered.reset_index()
df_filtered = df_filtered.drop(columns=['index'])

print(df_filtered)

df_filtered.to_csv('out.csv')

print("\nDone!")