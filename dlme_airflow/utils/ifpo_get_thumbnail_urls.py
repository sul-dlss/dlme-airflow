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

def add_thumbnail_urls(**kwargs):
    # Fetch working directory path from catalog and read file into Pandas dataframe
    catalog = catalog_for_provider('ifpo')
    root_dir = os.path.dirname(os.path.abspath('metadata'))
    data_path = catalog.metadata.get('data_path', 'ifpo/photographs')
    working_csv = os.path.join(root_dir, 'working', data_path, 'data.csv')
    df = pd.read_csv(working_csv)

    # A column can only be added to a data from of the same length, build an array
    # for thumbnails that is the appropriate length
    array_of_thumbnails = ['test'] * len(df.index)  # This is just an example with the value 'test'

    ### DO YOUR THUMBANIL WORK HERE AGAINST THE DATAFRAME ###

    # This assignment adds the array above to the dataframe with the header 'thumbnail'
    df = df.assign(thumbnail=array_of_thumbnails) 

    logging.info('The idfo_get_thumbnail_urls file is running.')
    #df.apply(lambda row : get_thumbnail_url(row['id']), axis = 1)

    df.to_csv(working_csv)
