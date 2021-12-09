# /bin/python
import pandas as pd
import yaml

# Objects from these countries will be suppressed
NON_RELEVANT_COUNTRIES = ['Canada',
                          'China',
                          'Ecuador',
                          'Guatemala',
                          'Greece',
                          'Honduras',
                          'Italy',
                          'Korea',
                          'Malaysia',
                          'Mexico',
                          'Peru',
                          'South Africa',
                          'USA']

# Fetch working directory path from catalog and read file into Pandas dataframe
with open('../catalogs/catalog.yaml', 'r') as stream:
    catalog = yaml.safe_load(stream)
    path = catalog['sources']['yale_babylonian']['metadata']['working_directory']

df = pd.read_csv(f"{path}/data.csv")

# Filter out non relevant records and over write the csv
df = df[~df['geographic_country'].isin(NON_RELEVANT_COUNTRIES)]

df.to_csv(f"{path}/data.csv")
