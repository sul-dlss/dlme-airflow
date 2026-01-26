# /bin/python
import pandas


def read_datafile_with_lists(path) -> pandas.DataFrame:
    """Reads a JSON datafile and returns a Pandas DataFrame after having converted
    lists serialized as strings back into lists again.
    """
    # the options turn off pandas trying to helpfully convert datatypes and dates
    # see: https://github.com/sul-dlss/dlme-airflow/issues/482
    df = pandas.read_json(path, convert_dates=False, dtype=False)
    df = df.map(lambda v: v if v else None)
    return df
