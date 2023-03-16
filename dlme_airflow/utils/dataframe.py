import os
import pandas as pd

# TODO: should these maybe be methods on Collection?


def datafile_for_collection(collection):
    working_csv = os.path.join(
        os.path.abspath("working"), collection.data_path(), "data.csv"
    )

    return working_csv


def dataframe_from_file(collection) -> pd.DataFrame:
    """Returns existing DLME metadata for a collection as a Pandas dataframe

    @param -- collection
    """
    csv_path = datafile_for_collection(collection)
    if not os.path.isfile(csv_path):
        raise Exception(f"Unable to find CSV at f{csv_path}")
    return pd.read_csv(csv_path)


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(collection, last_harvest_start_date=None):
    working_csv = datafile_for_collection(collection)
    os.makedirs(os.path.dirname(working_csv), exist_ok=True)

    unique_id = (
        collection.catalog.metadata.get("fields")
        .get("id")
        .get("name_in_dataframe", "id")
    )

    # We need to set last_harvest_start_date here since the catalog metadata comes
    # from the YAML Intake catalog file, but in some cases (oai driver currently)
    # we need the Intake driver to know about the last time the harvest ran successfully.
    setattr(collection.catalog, "last_harvest_start_date", last_harvest_start_date)

    source_df = collection.catalog.read()

    # Ensure that the unique id is not a list or else the call to
    # drop_duplicates below will fail. If a list is the unique_id value will be
    # replaced with the first element of the list that is contained.
    if len(source_df[unique_id]) > 0 and type(source_df[unique_id][0]) == list:
        source_df[unique_id] = source_df[unique_id].apply(lambda l: l[0])

    source_df = source_df.drop_duplicates(subset=[unique_id], keep="first")

    source_df.to_csv(working_csv, index=False)

    return {"working_csv": working_csv, "source_df": source_df}
