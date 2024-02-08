import argparse
from lingua import Language, LanguageDetectorBuilder
import json
import os
import pathlib
from collections import Counter

def populate_counter(files: list) -> dict:
    """
    populate_counter reads a list of json files and populates a counter object.

    :param files: a list of file paths to json files.
    :return: returns a counter object.
    """
    counter = Counter()
    for f in files:
        with open(f, 'r') as source_data:
            data = json.loads(source_data.read())
            for record in data:
                for k,v in record.items():
                    if v != None:
                        counter.update({k: 1})

    return counter

def get_files(paths: list) -> list:
    """
    get_files reads a list of directory paths and finds the json files in those directories.

    :param paths: a list of directory paths.
    :return: returns a list of json file paths.
    """
    all_files = []
    for p in paths:
        children = pathlib.Path(f"working/{p.replace('_', '-')}")
        files = list(children.rglob("*.json"))
        for f in files:
            all_files.append(os.fspath(f))
    return all_files

def populate_langs(files: list, data_dict: dict) -> dict:
    """
    populate_langs reads a list of directory paths and finds the json files in those directories.

    :param files: a list of file paths to json files.
    :param data_dict: a dictionary containing data fields and counts; counts an int.
    :return: returns a dictionary with the vales converted to tuples whese index 0 is
    the count int and index 1 is a list of languges values.
    """
    languages = [Language.ARABIC, Language.ARMENIAN, Language.ENGLISH, Language.PERSIAN]
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    for f in files:
        with open(f, 'r') as source_data:
            data = json.loads(source_data.read())
            for record in data:
                for k,v in record.items():
                    if record.get(k) != None:
                        value_langs = [detector.detect_language_of(i) for i in v]
                        for l in value_langs:
                            if l not in data_dict.get(k)[1]:
                                data_dict[k][1].extend([l])

    return data_dict

def write_report(files, traject_file, data_dict):
    """
    write_report writes a report for a traject file detailing what source data is mapped, what fields, counts,
    and languages are in the source data, and comparing fields in the traject config to those in the source data.

    :param files: a list of file paths to json files.
    :param traject_file: the traject config mapping file.
    :param data_dict: a dictionary containing data fields and counts; counts an int.
    :return: no return.
    """
    with open(f"{traject_file.split('/')[-1].split('.')[0]}-report.txt", 'w') as out:
        out.write(f"The {traject_file.split('/')[-1]} traject config is configured to map the following source files:")
        out.write('\n\n')
        for i in files:
            out.write('\t')
            out.write('- ')
            out.write(i)
            out.write('\n')

        out.write('\n')
        out.write(f"Below are the fields that were found in those files along with the counts of how many times the field appears and the predicted languages of the values in the field:")
        out.write('\n\n')
        for i, c in data_dict.items():
            out.write('\t')
            out.write('- ')
            out.write(f'{i}: {c}')
            out.write('\n')

def write_template(file_path, data_dict):
    """
    write_report writes a traject config template which includes all fields in the source data,
    predicted languages for the values in each field, and all traject requirements.

    :param file_path: the path to the traject config mapping file you wish to create.
    :param data_dict: a dictionary containing data fields and counts; counts an int.
    :return: no return.
    """
    with open(f"{file_path}", 'w') as out:
        out.write("# frozen_string_literal: true\n\n")
        out.write("require 'dlme_json_resource_writer'\n")
        out.write("require 'dlme_debug_writer'\n")
        out.write("require 'macros/collection'\n")
        out.write("require 'macros/date_parsing'\n")
        out.write("require 'macros/dlme'\n")
        out.write("require 'macros/each_record'\n")
        out.write("require 'macros/iiif'\n")
        out.write("require 'macros/language_extraction'\n")
        out.write("require 'macros/normalize_language'\n")
        out.write("require 'macros/normalize_type'\n")
        out.write("require 'macros/path_to_file'\n")
        out.write("require 'macros/prepend'\n")
        out.write("require 'macros/string_helper'\n")
        out.write("require 'macros/timestamp'\n")
        out.write("require 'macros/title_extraction'\n")
        out.write("require 'macros/version'\n")
        out.write("require 'traject_plus'\n\n")
        out.write("extend Macros::Collection\n")
        out.write("extend Macros::DLME\n")
        out.write("extend Macros::DateParsing\n")
        out.write("extend Macros::EachRecord\n")
        out.write("extend Macros::IIIF\n")
        out.write("extend Macros::LanguageExtraction\n")
        out.write("extend Macros::NormalizeLanguage\n")
        out.write("extend Macros::NormalizeType\n")
        out.write("extend Macros::PathToFile\n")
        out.write("extend Macros::Prepend\n")
        out.write("extend Macros::StringHelper\n")
        out.write("extend Macros::Timestamp\n")
        out.write("extend Macros::TitleExtraction\n")
        out.write("extend Macros::Version\n")
        out.write("extend TrajectPlus::Macros\n")
        out.write("extend TrajectPlus::Macros::JSON\n\n")
        out.write("settings do\n")
        out.write("  provide 'writer_class_name', 'DlmeJsonResourceWriter'\n")
        out.write("  provide 'reader_class_name', 'TrajectPlus::JsonReader'\n")
        out.write("end\n\n")
        out.write("# Set Version & Timestamp on each record\n")
        out.write("to_field 'transform_version', version\n")
        out.write("to_field 'transform_timestamp', timestamp\n\n")
        out.write("# File path\n")
        out.write("to_field 'dlme_source_file', path_to_file\n\n")
        out.write("to_field 'agg_data_provider_collection', path_to_file, split('/'), at_index(3), gsub('_', '-'), prepend('#####'), translation_map('agg_collection_from_provider_id'), lang('en')\n")
        out.write("to_field 'agg_data_provider_collection', path_to_file, split('/'), at_index(3), gsub('_', '-'), prepend('#####'), translation_map('agg_collection_from_provider_id'), translation_map('agg_collection_ar_from_en'), lang('ar-Arab')\n")
        out.write("to_field 'agg_data_provider_collection_id', path_to_file, split('/'), at_index(3), gsub('_', '-'), prepend('#####')\n\n")
        out.write("# CHO Required\n")
        out.write("to_field 'id', extract_json('.id'), '#####'\n")
        out.write("to_field 'cho_title', extract_json('#####'), strip, arabic_script_lang_or_default('und-Arab', 'en')\n\n")
        out.write("# CHO Other\n")
        for k,v in data_dict.items():
            out.write(f"to_field '#####', extract_json('.{k}'), strip, arabic_script_lang_or_default('#####', '#####') languages_detected = {v[1]}\n")
        out.write("\n")
        out.write("# Agg\n")
        out.write("to_field 'agg_data_provider', data_provider, lang('en')\n")
        out.write("to_field 'agg_data_provider', data_provider_ar, lang('ar-Arab')\n")
        out.write("to_field 'agg_data_provider_country', data_provider_country, lang('en')\n")
        out.write("to_field 'agg_data_provider_country', data_provider_country_ar, lang('ar-Arab')\n")
        out.write("to_field 'agg_is_shown_at' do |_record, accumulator, context|\n")
        out.write("  accumulator << transform_values(\n")
        out.write("    context,\n")
        out.write("    'wr_id' => [extract_json('#####')],\n")
        out.write("    'wr_is_referenced_by' => [extract_json('#####')]\n")
        out.write("  )\n")
        out.write("end\n")
        out.write("to_field 'agg_preview' do |_record, accumulator, context|\n")
        out.write("  accumulator << transform_values(\n")
        out.write("    context,\n")
        out.write("    'wr_id' => [extract_json('#####')],\n")
        out.write("    'wr_is_referenced_by' => [extract_json('#####')]\n")
        out.write("  )\n")
        out.write("end\n")
        out.write("to_field 'agg_provider', provider, lang('en')\n")
        out.write("to_field 'agg_provider', provider_ar, lang('ar-Arab')\n")
        out.write("to_field 'agg_provider_country', provider_country, lang('en')\n")
        out.write("to_field 'agg_provider_country', provider_country_ar, lang('ar-Arab')\n\n")
        out.write("each_record convert_to_language_hash(\n")
        out.write("  'agg_data_provider',\n")
        out.write("  'agg_data_provider_country',\n")
        out.write("  'agg_provider',\n")
        out.write("  'agg_provider_country',\n")
        out.write("  'cho_alternative',\n")
        out.write("  'cho_contributor',\n")
        out.write("  'cho_coverage',\n")
        out.write("  'cho_creator',\n")
        out.write("  'cho_date',\n")
        out.write("  'cho_dc_rights',\n")
        out.write("  'cho_description',\n")
        out.write("  'cho_edm_type',\n")
        out.write("  'cho_extent',\n")
        out.write("  'cho_format',\n")
        out.write("  'cho_has_part',\n")
        out.write("  'cho_has_type',\n")
        out.write("  'cho_is_part_of',\n")
        out.write("  'cho_language',\n")
        out.write("  'cho_medium',\n")
        out.write("  'cho_provenance',\n")
        out.write("  'cho_publisher',\n")
        out.write("  'cho_relation',\n")
        out.write("  'cho_source',\n")
        out.write("  'cho_spatial',\n")
        out.write("  'cho_subject',\n")
        out.write("  'cho_temporal',\n")
        out.write("  'cho_title',\n")
        out.write("  'cho_type',\n")
        out.write("  'agg_data_provider_collection'\n")
        out.write(")\n\n")
        out.write("# NOTE: call add_cho_type_facet AFTER calling convert_to_language_hash fields\n")
        out.write("each_record add_cho_type_facet\n")

def main(args):
    paths = []

    if args.path:
        counter = populate_counter(args.path)
        collections_data = dict(counter)
        for k,v in collections_data.items():
            collections_data.update({k: (v, [])})

        collections_data = populate_langs(files, collections_data)
    else:
        with open('../dlme-transform/config/metadata_mapping.json', 'r') as metadata_mapping:
            for i in json.loads(metadata_mapping.read()):
                if args.file.split('/')[-1] in i.get('trajects'):
                    paths = i.get('paths')

            files = get_files(paths)

            counter = populate_counter(files)
            collections_data = dict(counter)
            for k,v in collections_data.items():
                collections_data.update({k: (v, [])})

            collections_data = populate_langs(files, collections_data)

    if args.report:
        write_report(files, args.file, collections_data)

    if args.template:
        write_template(args.file, collections_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helper tools for mapping DLME collections")
    parser.add_argument('file', help='Traject file')
    parser.add_argument("-t", "--template", action='store_true', help="Generate a traject template from all all fields in the source data")
    parser.add_argument("-r", "--report", action='store_true', help="Generate a coverage report that compares the fields in the source data to those in the traject config")
    parser.add_argument("-p", "--path", help="Get a report from a specific file, instead of reading metadata_mappings.json")
    args = parser.parse_args()

    main(args)
