from dlme_airflow.drivers.sequential_csv import SequentialCsvSource


def test_one():
    src = SequentialCsvSource(urlpath="tests/data/csv/example1.csv")
    df = src.read()
    assert len(df) == 4


def test_multi():
    files = [
        "tests/data/csv/example1.csv",
        "tests/data/csv/example2.csv",
        "tests/data/csv/example3.csv",
        "tests/data/csv/example4.csv",
        "tests/data/csv/example5.csv",
    ]
    src = SequentialCsvSource(urlpath=files)
    df = src.read()
    assert len(df) == 20
