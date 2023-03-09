#!/usr/bin/python
import io
import json
import os
from collections import Counter, defaultdict
from datetime import date
import logging
import random

from dominate import document
from dominate.tags import style, h1, h2, div, attr, p, ul, li, tr, td, b, table
from PIL import Image
import requests
import validators
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dlme_airflow.utils.catalog import catalog_for_provider

# Constants for crosswalk
fields = [
    "cho_alternative",
    "cho_contributor",
    "cho_creator",
    "cho_date ",
    "cho_dc_rights",
    "cho_description",
    "cho_edm_type",
    "cho_extent",
    "cho_format",
    "cho_has_part",
    "cho_has_type",
    "cho_identifier",
    "cho_is_part_of",
    "cho_language",
    "cho_medium",
    "cho_provenance",
    "cho_publisher",
    "cho_relation",
    "cho_same_as",
    "cho_source",
    "cho_spatial",
    "cho_subject",
    "cho_temporal",
    "cho_title",
    "cho_type",
]

EXTRACT_MACROS = {
    "cambridge_dimensions": {
        "from_field": "/tei:extent/tei:dimensions",
        "transforms": "Extracts height and width into formated string.",
    },
    "extract_aub_description": {
        "from_field": "/dc:description",
        "transforms": "Ignores url values in the description field.",
    },
    "generate_edm_type": {
        "from_field": ".Classification or .ObjectName",
        "transforms": "Seperate values on ';', then downcase",
    },
    "json_title_plus": {
        "from_field": ".title and one other field",
        "transforms": "The title field was merged with the truncated value from the second field.",
    },
    "princeton_title_and_lang": {
        "from_field": ".title",
        "transforms": "The script of the title was programatically determined.",
    },
    "scw_has_type": {
        "from_field": "/*/mods:genre or /*/mods:typeOfResource or /*/mods:subject/mods:topic or /*/mods:extension/cdwalite:indexingMaterialsTechSet/"  # noqa: E501
        "cdwalite:termMaterialsTech",
        "transforms": "The output value was mapped to a value in a DLME controlled vocabulary.",
    },
    "xpath_title_or_desc": {
        "from_field": "/dc:title or /dc:description[3]",
        "transforms": "If no title found in /dc:title, map in truncated description.",
    },
    "xpath_title_plus": {
        "from_field": "the title field and a second field such as id or description",
        "transforms": "The title field was merged with the truncated value from the second field.",
    },
}

MODIFY_MACROS = {
    "prepend": "A literal value was prepended to provide context or to satisfy a consistent pattern requirement.",
    "translation_map": "The output value was mapped to a value in a DLME controlled vocabulary.",
}

IGNORE_FIELDS = [
    "agg_data_provider",
    "agg_data_provider_collection",
    "agg_data_provider_country",
    "agg_provider",
    "agg_provider_country",
    "cho_type_facet",
    "dlme_collection",
    "dlme_source_file",
    "id",
    "transform_version",
    "transform_timestamp",
]

IGNORE_VALUES = [
    "wr_dc_rights",
    "wr_edm_rights",
    "wr_is_referenced_by",
    "fields_covered",
]


def write_file(url, filename):
    with open(filename, "wb") as file:
        try:
            response = requests.get(url, allow_redirects=True)
            response.raise_for_status()
            file.write(response.content)
        except requests.exceptions.RequestException as exception:
            logging.error(f"Could not retrieve {url}: {exception}")
            raise


def count_fields(field, metadata):
    """Increment counts for fields"""
    if field not in IGNORE_FIELDS:
        counts[field].update({"fields_covered": 1})
        if isinstance(metadata, dict):
            for key, values in metadata.items():
                if isinstance(values, list):
                    counts[field].update({key: len(values)})
                else:
                    counts[field].update({key: 1})
        else:  # dates and other array or string values
            counts[field].update({"values": 1})


def thumbnail_report(image_sizes_list):
    """Takes a list of tuples as input and outputs a thumbnail image size report."""
    passed_records = 0
    failed_records = 0
    REC_SIZE = 400

    if not image_sizes_list:
        return "No thumbnail images were resolvable."
    else:
        for size in image_sizes_list:
            if size[0] >= REC_SIZE or size[1] >= REC_SIZE:
                passed_records += 1
            else:
                failed_records += 1

        return f"{round((passed_records/len(image_sizes_list))*100)}% of the {len(image_sizes_list)} thumbnail images sampled had a width or height of {REC_SIZE} or greater."  # noqa: E501


def image_size(content) -> tuple[int, int]:
    image = Image.open(io.BytesIO(content))

    return image.size


def validate_url(url):
    """Checks if url has valid form."""
    if validators.url(url):
        return True
    return False


def resolve_resource_url(record):
    """Check resolvability of resource URL"""
    url = record.get("agg_is_shown_at").get("wr_id")
    unresolvable_message = (
        f"Identifier {record['id']} from DLME file {record['dlme_source_file']}: {url}"
    )
    if validate_url(url):
        try:
            response = requests.head(
                url,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0"
                },
            )
            if not response.ok:  # status_code < 400
                unresolvable_resources.append(unresolvable_message)
        except requests.exceptions.RequestException:
            unresolvable_resources.append(unresolvable_message)
    else:
        unresolvable_resources.append(unresolvable_message)


def resolve_thumbnail_url(record):
    """Check resolvability of thumbnail URL"""
    if record.get("agg_preview"):
        url = record.get("agg_preview").get("wr_id")
    else:
        return
    unresolvable_message = (
        f"Identifier {record['id']} from DLME file {record['dlme_source_file']}: {url}"
    )
    if not validate_url(url):
        unresolvable_thumbnails.append(unresolvable_message)
        return
    try:
        response = requests.get(url, stream=True)
        if response.ok:  # status_code < 400
            thumbnail_image_urls.append(url)
        else:
            unresolvable_thumbnails.append(unresolvable_message)
    except requests.exceptions.RequestException:
        unresolvable_thumbnails.append(unresolvable_message)


def sample_image_sizes(thumbnail_urls) -> list[tuple[int, int]]:
    """Gets image sizes for a sample of thumbnails"""
    sizes = []
    if len(thumbnail_urls) > 5000:
        sample = random.sample(thumbnail_urls, 250)
    elif len(thumbnail_urls) > 100:
        sample = random.sample(thumbnail_urls, 50)
    else:
        sample = thumbnail_urls
    for url in sample:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            sizes.append(image_size(response.content))
        except requests.exceptions.RequestException:
            continue  # skip image

    return sizes


# Define variables for collecting data from main
thumbnail_image_urls: list[str] = []
thumbnail_image_sizes: list[tuple[int, int]] = []
unresolvable_resources: list[str] = []
unresolvable_thumbnails: list[str] = []
counts: defaultdict = defaultdict(Counter)


def harvest_report(**kwargs):  # input:, config:):
    """Captures all field value counts in counter object and writes report to html file."""
    record_count = 0
    # merge all records into single counter object and write field report

    provider_id = kwargs.get("provider")
    collection_id = kwargs.get("collection")
    data_path = kwargs.get("data_path").replace(
        "/", "-"
    )  # penn/egyptian-museum => penn-egyptian-museum

    catalog = catalog_for_provider(f"{provider_id}.{collection_id}")
    config_url = f"https://raw.githubusercontent.com/sul-dlss/dlme-transform/main/traject_configs/{catalog.metadata.get('config')}.rb"
    config_file = f"/tmp/{provider_id}_{collection_id}_config.rb"
    write_file(config_url, config_file)

    input_file = f"{os.environ.get('METADATA_OUTPUT_PATH')}/output-{data_path}.ndjson"

    with open(input_file, "r") as file:
        for line in file:
            if line.strip():  # if line is not empty
                record = json.loads(line)
                record_count += 1
                for field, metadata in record.items():
                    count_fields(field, metadata)

                # Resolve resource and thumbnail urls
                resolve_resource_url(record)
                resolve_thumbnail_url(record)
            else:  # if line is empty
                continue

    thumbnail_image_sizes = sample_image_sizes(thumbnail_image_urls)

    doc = document(title="DLME Metadata Report")

    with doc.head:
        style(
            """\
         body {
              font-family: sans-serif;
              margin: 3em 1em;
         }
         h1 {
              text-align: center;
         }
          .column {
              flex: 50%;
          }
         .report {
              border: 1px solid black;
              margin: 10px 25px 10px;
              padding: 5px 10px 5px;
         }
         .row {
              display: flex;
         }
     """
        )

    with doc:
        h1(f"DLME Metadata Report for {provider_id}")
        h2(f"{collection_id} ({date.today()})")

        with div():
            attr(cls="body")
            attr(cls="row")
            # column one
            with div():
                attr(cls="column")
                # coverage report
                with div():
                    attr(cls="report")
                    h2("Coverage Report")
                    for item, counter in sorted(counts.items()):
                        languages = {}

                        p(
                            b(
                                f"{item}: ({int(((counts[item]['fields_covered'])/record_count)*100)}% coverage)"
                            )
                        )

                        sub_field_list = ul()

                        for k, v in counter.items():
                            if k in IGNORE_VALUES:
                                continue
                            elif k == "values" or k == "wr_id":
                                sub_field_list += li(
                                    f"Average number of values: {round((v/record_count), 2)}"
                                )
                            else:
                                languages[k] = v
                        if languages:
                            sub_field_list += li(
                                f"Average number of values: {round((sum(languages.values())/record_count), 2)}"
                            )
                            lang_list = ul()
                            sub_field_list += li("Languages (average):")
                            sub_field_list += lang_list
                            for k, v in languages.items():
                                lang_list += li(f"{k}: {round(v/record_count, 2)}")

            # column two
            with div():
                attr(cls="column")
                # resource report
                with div():
                    attr(cls="report")
                    h2("Resource Report")
                    with ul() as u_list:
                        u_list.add(
                            li(
                                f"{counts['agg_preview']['wr_id']} of {record_count} records had valid urls to thumbnail images."
                            )
                        )
                        if len(unresolvable_thumbnails) > 0:
                            u_list.add(
                                li(
                                    "The following thumbnails urls were unresolvable when testing:"
                                )
                            )
                            unresolvable_thumbnails_list = u_list.add(ul())
                            for i in unresolvable_thumbnails:
                                unresolvable_thumbnails_list.add(li(i))

                        u_list.add(
                            li(
                                f"{counts['agg_is_shown_at']['wr_id']} of {record_count} records had valid urls to resources."
                            )
                        )
                        if len(unresolvable_resources) > 0:
                            u_list.add(
                                li(
                                    "The following resource urls were unresolvable when testing:"
                                )
                            )
                            unresolvable_resources_list = u_list.add(ul())
                            for i in unresolvable_resources:
                                unresolvable_resources_list.add(li(i))
                        u_list.add(
                            li(
                                f"{counts['agg_is_shown_at']['wr_is_referenced_by']} of {record_count} records had iiif manifests."
                            )
                        )

                # rights report
                with div():
                    attr(cls="report")
                    h2("Rights Report")

                    u_list = ul()
                    u_list.add(
                        li(
                            f"{counts['cho_dc_rights']['fields_covered']} of {record_count} records had a clearly expressed copyright status for the cultural heritage object."  # noqa: E501
                        )
                    )
                    if counts["agg_is_shown_at"]["wr_edm_rights"] > 0:
                        wr_count = counts["agg_is_shown_at"]["wr_edm_rights"]
                    else:
                        wr_count = counts["agg_is_shown_at"]["wr_dc_rights"]
                    u_list.add(
                        li(
                            f"{wr_count} of {record_count} records had a clearly expressed copyright status for the web resource."
                        )
                    )
                    u_list.add(
                        li(
                            f"{counts['agg_edm_rights']['fields_covered']} of {record_count} records had clearly expressed aggregation rights."  # noqa: E501
                        )
                    )

                # thumbnail quality report
                with div():
                    attr(cls="report")
                    h2("Thumbnail Quality Report")
                    u_list = ul()
                    u_list.add(li(thumbnail_report(thumbnail_image_sizes)))

        # metadata crosswalk
        with div():
            attr(cls="row")
            attr(cls="report")
            h2("Metadata Crosswalk")

            with table(style="border-collapse: collapse"):
                header = tr(style="border:1px solid black")
                header.add(td("Incoming Field", style="font-weight: bold"))
                header.add(td(style="padding: 0 15px;"))
                header.add(td("DLME Field", style="padding: 0 15px; font-weight: bold"))
                header.add(td(style="padding: 0 15px;"))
                header.add(
                    td("Transformations", style="padding: 0 15px; font-weight: bold")
                )

                # crosswalk code
                with open(config_file) as f:
                    lines = f.readlines()
                    for line in lines:
                        for field in fields:
                            if "to_field" in line:
                                if field in line:
                                    to_field = line.split(",")[0].strip("to_field ")
                                    transforms = []
                                    from_field = None
                                    for k, v in EXTRACT_MACROS.items():
                                        if k in line:
                                            from_field = EXTRACT_MACROS.get(k).get(
                                                "from_field"
                                            )
                                            transforms.append(
                                                EXTRACT_MACROS.get(k).get("transforms")
                                            )
                                    # if no keys found in EXTRACT_MACROS
                                    if from_field is None:
                                        if "literal(" in line:
                                            from_field = (
                                                "Assigned literal value: '{}'".format(
                                                    line.split("literal(")[-1].split(
                                                        "),"
                                                    )[0]
                                                )
                                            )
                                        else:
                                            from_field = (
                                                line.split("(")[1]
                                                .split(")")[0]
                                                .strip("'")
                                            )
                                    for k, v in MODIFY_MACROS.items():
                                        if k in line:
                                            transforms.append(MODIFY_MACROS.get(k))
                                    row = tr(style="border:1px solid black")
                                    row.add(td(from_field))
                                    row.add(td(">>", style="padding: 0 15px;"))
                                    row.add(td(to_field, style="padding: 0 15px;"))
                                    row.add(td(">>", style="padding: 0 15px;"))
                                    row.add(
                                        td(
                                            " ".join(transforms),
                                            style="padding: 0 15px;",
                                        )
                                    )

    return doc.render()


def build_harvest_report_task(collection, task_group: TaskGroup, dag: DAG):
    return PythonOperator(
        task_id=f"{collection.label()}_harvest_report",
        dag=dag,
        task_group=task_group,
        python_callable=harvest_report,
        op_kwargs={
            "provider": collection.provider.name,
            "collection": collection.name,
            "data_path": collection.data_path(),
        },
    )
