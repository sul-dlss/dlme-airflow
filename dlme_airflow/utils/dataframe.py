import os

import pandas as pd

from dlme_airflow.utils.filter_dataframe import filter_by_field

# TODO: should these maybe be methods on Collection?


def dataframe_from_file(collection, format="csv") -> pd.DataFrame:
    """Returns existing DLME metadata for a collection as a Pandas dataframe

    @param -- collection
    """
    datafile_path = collection.datafile(format)
    if not os.path.isfile(datafile_path):
        raise FileNotFoundError(f"Unable to find {format.upper()} at {datafile_path}")

    if format == "json":
        return pd.read_json(datafile_path)

    return pd.read_csv(datafile_path)


# TODO: An Error is thrown on line 22 if working_directory is not found in
#       the metadata. Need to handle this error.
def dataframe_to_file(collection, last_harvest_start_date=None):
    working_csv = collection.datafile("csv")
    working_json = collection.datafile("json")
    os.makedirs(os.path.dirname(working_csv), exist_ok=True)

    unique_id = (
        collection.catalog.metadata.get("fields")
        .get("id")
        .get("name_in_dataframe", "id")
    )

    # We need to set last_harvest_start_date here since the catalog metadata comes
    # from the YAML Intake catalog file, but in some cases (oai driver currently)
    # we need the Intake driver to know about the last time the harvest ran successfully.
    collection.catalog.last_harvest_start_date = last_harvest_start_date

    source_df = collection.catalog.read()

    # Large sources that are split into several collections by a filter (e.g. one
    # museum-wide CSV per curatorial section) can opt in to filtering here, so the
    # full source is never written to disk or reloaded by the filter_data task.
    # Opt-in because filter_by_field expects scalar values, and some sources have
    # list values until they are written and re-read as JSON.
    if collection.catalog.metadata.get("filter_on_harvest"):
        for field, field_filters in collection.filters().items():
            source_df = filter_by_field(source_df, field, field_filters)
        source_df = source_df.reset_index(drop=True)

    # Ensure that the unique id is not a list or else the call to
    # drop_duplicates below will fail. If a list is the unique_id value will be
    # replaced with the first element of the list that is contained.
    if len(source_df[unique_id]) > 0 and type(source_df[unique_id][0]) is list:
        source_df[unique_id] = source_df[unique_id].apply(lambda source_record: source_record[0])

    source_df = source_df.drop_duplicates(subset=[unique_id], keep="first")

    source_df.to_csv(working_csv, index=False)
    source_df.to_json(working_json, orient="records", force_ascii=False)

    return {"working_csv": working_csv, "source_df": source_df}
