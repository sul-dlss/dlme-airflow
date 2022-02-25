# /bin/python
import logging
import os
import pandas as pd

from utils.catalog import catalog_for_provider

def get_thumbnail_url(id):
    id = id.split(':')[-1]
    url = f"https://medihal.archives-ouvertes.fr/IFPOIMAGES/{id}"
    req = requests.get(url)
    return f"{req.url}/large"

def add_thumbnail_urls():
    # Fetch working directory path from catalog and read file into Pandas dataframe
    catalog = catalog_for_provider('ifpo')
    root_dir = os.path.dirname(os.path.abspath('metadata'))
    logging.info(f'Root directory path is {root_dir}.')
    data_path = catalog.metadata.get('data_path', 'ifpo/photographs')
    logging.info(f'Data path is {root_dir}.')
    working_csv = os.path.join(root_dir, 'working', data_path, 'data.csv')

    df = pd.read_csv(working_csv)

    # Build thumbnail url and use that to fetch larger image from redirect
    df['thumbnail'] = ['test']
    logging.info('The idfo_get_thumbnail_urls file is running.')
    #df.apply(lambda row : get_thumbnail_url(row['id']), axis = 1)

    df.to_csv(working_csv)
