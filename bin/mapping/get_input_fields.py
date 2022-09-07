#!/usr/bin/python
import glob, os
import pandas as pd
from argparse import ArgumentParser

def main():
    """Takes the path the the provider as an input and returns a unique, sorted
    list of all column names found in all csv files. This is useful for ensuring that
    all fields get mapped in the traject config. Optionally use the -c flag with the column
    to return a list of csv files conaining that column.

    @example -- python dlme-airflow/working/aims -c author

    """
    column_names = []
    files = glob.glob(f"/{args.directory[0]}/*/*.csv")
    if args.c:
        for f in files:
            df = pd.read_csv(f)
            if args.c in list(df.columns):
                print(f)
    else:
        for f in files:
            df = pd.read_csv(f)
            [column_names.append(c) for c in (df.columns)]

        for i in sorted(set(column_names)):
            print(i)

if __name__ == '__main__':
    # CLI client options.
    parser = ArgumentParser()
    parser.add_argument(
        'directory',
        nargs='+',
        help='Which data provider directory do you want to parse?')
    parser.add_argument(
        '-c',
        help='This flag allows you to pass a column name in and returns source files containing that column.')
    args = parser.parse_args()
    main()
