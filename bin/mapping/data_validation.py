from argparse import ArgumentParser
import json


def main():
    """Takes the path of the intermediate represenation file as an input and returns a unique,
     checks for the existence of all required fields. In all records, makes sure urls are all valid,
     automatically resolves 5 preview urls and 5 resource urls, checks that if a lang code end in -Arab,
     it has Arabic characters, and checks that if lang code is en or ends in -Latn it has Latin characters

    @example -- python bin/mapping/data_validation.py path_to_file

    """
    REQUIRED_FIELDS = ["agg_preview", "agg_is_shown_at", "cho_dc_rights", "cho_edm_type", "cho_has_type"]
    URL_FIELDS = ["agg_preview", "agg_is_shown_at"]

    def check_required_fields(record):
        """Make sure all required fields are present.
        If not, capture record identifier for report."""
        bad_records = {}
        for field in REQUIRED_FIELDS:
            if not record.get(field):
                bad_records[field] = []
                bad_records[field].append(record["id"])
            elif field in URL_FIELDS:
                if not record.get(field).get("wr_id"):
                    bad_records[field] = []
                    bad_records[field].append(record["id"])

        return bad_records

    with open(args.data[0], "r") as file:
        bad_records = {}
        for line in file:
            if line.strip():  # if line is not empty
                record = json.loads(line)

                for key, value in check_required_fields(record).items():
                    if bad_records.get(key):
                        bad_records[key].extend(value)
                    else:
                        bad_records[key] = value

        for k, v in bad_records.items():
            print(k, v)


if __name__ == "__main__":
    # CLI client options.
    parser = ArgumentParser()
    parser.add_argument(
        "data",
        nargs="+",
        help="Which data data file do you want to parse?",
    )
    args = parser.parse_args()
    main()
