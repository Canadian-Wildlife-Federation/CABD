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

# this dataframe holds the list of changes with conflicting info
cabd = pd.DataFrame(columns=['cabd_id', 'email', 'colname', 'conflict'])


# filter down to only cabd_ids with duplicates, where cabd_ids are not null, and reset index
duplicates = df[df.duplicated('cabd_id', keep=False)]
duplicates = duplicates[duplicates['cabd_id'].notna()]
duplicates = duplicates.sort_values(by='cabd_id', ascending=True)
duplicates = duplicates.reset_index()
duplicates = duplicates.drop(columns=['index'])

duplicates.to_csv('duplicates.csv')

df2 = duplicates.groupby('cabd_id')

# find non unique entries for specified column for the same cabd_id
# TO DO: ignore nan values in columns - these should not be included as "duplicates"
for (colname, colval) in duplicates.items():

    # ignore columns in colskip list
    if colname in colskip:
        continue

    else:
        # Find all rows with conflicting information in colname
        df2 = duplicates[duplicates.groupby('cabd_id')[colname].transform('nunique').gt(1)] 

        # Skip if no conflicts in this column
        if df2.empty:
            continue

        # Group duplicates by cabd_id, create a column listing conflicting change and a column for email of who last updated
        df2 = df2.groupby('cabd_id').agg(email = ('email', lambda x: list(x.unique())), 
                                        conflict = (colname, lambda x: list(x.unique())))
        df2 = df2.reset_index()

        # Add a column for which attribute has the conflicting info
        df2['colname'] = pd.Series(colname for x in range(len(df2.index)))

        cabd = pd.concat([cabd, df2], ignore_index=True)         # append this dataframe to cabd (list of all conflicts)

cabd.to_csv('out.csv')

print("\nDone!")