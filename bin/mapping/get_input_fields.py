#!/usr/bin/python
import glob
import os
import pandas as pd
from argparse import ArgumentParser


def main():
    """Takes the path of the provider as an input and returns a unique, sorted
    list of all column names found in all csv files. This is useful for ensuring that
    all fields get mapped in the traject config. Optionally use the -c flag with the column
    to return a list of csv files conaining that column.

    @example -- python bin/mapping/get_input_fields.py aims -c author

    """
    column_names = []
    mapped_columns = []
    files = glob.glob(f"{os.getcwd()}/working/{args.directory[0]}/*/*.csv")
    if args.d:
        for file in files:
            df = pd.read_csv(file)
            [column_names.append(c) for c in (df.columns)]
        with open(
            f"{os.getcwd()}/../dlme-transform/traject_configs/{args.d}.rb", "r"
        ) as traject_config:
            lines = traject_config.readlines()
            for line in lines:
                if "column" in line:
                    mapped_columns.append(line.split("column('")[-1].split("')")[0])
            if args.r:
                for i in sorted(set(mapped_columns).difference(set(column_names))):
                    print(i)
            else:
                for i in sorted(set(column_names).difference(set(mapped_columns))):
                    print(i)
    else:
        if args.c:
            for file in files:
                df = pd.read_csv(file)
                if args.c in list(df.columns):
                    print(file)
        else:
            for file in files:
                df = pd.read_csv(file)
                [column_names.append(c) for c in (df.columns)]

            for i in sorted(set(column_names)):
                print(i)


if __name__ == "__main__":
    # CLI client options.
    parser = ArgumentParser()
    parser.add_argument(
        "directory",
        nargs="+",
        help="Which data provider directory do you want to parse?",
    )
    parser.add_argument(
        "-c",
        help="This flag allows you to pass a column name in and returns source files containing that column.",
    )
    parser.add_argument(
        "-d",
        help="""This flag allows you to pass a path to a traject config file in and returns a sorted list of fields
        in the provider data that are not in the config.""",
    )
    parser.add_argument(
        "-r",
        action="store_true",
        help="""This flag is used to reverse the order of search. It returns a list of fields in the traject
        config that are not column names in the source data.""",
    )
    args = parser.parse_args()
    main()
