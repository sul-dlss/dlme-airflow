# /bin/python
import pandas


def read_datafile_with_lists(path) -> pandas.DataFrame:
    """Reads a JSON datafile and returns a Pandas DataFrame after having converted
    lists serialized as strings back into lists again.
    """
    df = pandas.read_json(path)
    df = df.applymap(lambda v: v if v else None)
    return df
