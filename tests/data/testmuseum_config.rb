# frozen_string_literal: true

require 'dlme_json_resource_writer'
require 'dlme_debug_writer'
require 'macros/collection'
require 'macros/csv'
require 'macros/date_parsing'
require 'macros/dlme'
require 'macros/each_record'
require 'macros/path_to_file'
require 'macros/timestamp'
require 'macros/version'
require 'traject_plus'

extend Macros::Collection
extend Macros::Csv
extend Macros::DLME
extend Macros::DateParsing
extend Macros::EachRecord
extend Macros::PathToFile
extend Macros::Timestamp
extend Macros::Version
extend TrajectPlus::Macros
extend TrajectPlus::Macros::Csv

settings do
  provide 'reader_class_name', 'TrajectPlus::CsvReader'
  provide 'writer_class_name', 'DlmeJsonResourceWriter'
end

# Set Version & Timestamp on each record
to_field 'transform_version', version
to_field 'transform_timestamp', timestamp

to_field 'agg_data_provider_collection', literal('ans'), translation_map('agg_collection_from_provider_id'), lang('en')
to_field 'agg_data_provider_collection', literal('ans'), translation_map('agg_collection_from_provider_id'), translation_map('agg_collection_ar_from_en'), lang('ar-Arab')
to_field 'agg_data_provider_collection_id', literal('ans')

# File path
to_field 'dlme_source_file', path_to_file

# CHO Required
to_field 'id', normalize_prefixed_id('RecordId')
to_field 'cho_title', column('Title'), strip, lang('en')

# CHO Other
to_field 'cho_contributor', column('Maker'), split('||'), strip, prepend('Maker: '), lang('en')
to_field 'cho_creator', column('Authority'), split('||'), strip, prepend('Authority: '), lang('en')
to_field 'cho_date', column('Ah'), split('.'), first_only, strip, append(' AH'), lang('en')
to_field 'cho_date_range_norm', csv_or_json_date_range('From Date', 'To Date')
to_field 'cho_date_range_hijri', csv_or_json_date_range('From Date', 'To Date'), hijri_range
to_field 'cho_dc_rights', literal('Public Domain'), lang('en')
to_field 'cho_description', column('Axis'), strip, prepend('Axis: '), lang('en')
to_field 'cho_description', column('Denomination'), strip, prepend('Denomination: '), lang('en')
to_field 'cho_description', column('Findspot'), strip, prepend('Findspot: '), lang('en')
to_field 'cho_description', column('Obverse Legend'), strip, prepend('Obverse legend: '), lang('en')
to_field 'cho_description', column('Obverse Type'), strip, prepend('Obverse type: '), lang('en')
to_field 'cho_description', column('Reverse Legend'), strip, prepend('Reverse legend: '), lang('en')
to_field 'cho_description', column('Reverse Type'), strip, prepend('Reverse type: '), lang('en')
to_field 'cho_edm_type', literal('Object'), lang('en')
to_field 'cho_edm_type', literal('Object'), translation_map('edm_type_ar_from_en'), lang('ar-Arab')
to_field 'cho_extent', column('Diameter'), strip, prepend('Diameter: '), lang('en')
to_field 'cho_extent', column('Weight'), strip, prepend('Weight: '), lang('en')
to_field 'cho_has_type', literal('Coins'), lang('en')
to_field 'cho_has_type', literal('Coins'), translation_map('has_type_ar_from_en'), lang('ar-Arab')
to_field 'cho_identifier', column('URI')
to_field 'cho_identifier', column('RecordId')
to_field 'cho_medium', column('Material'), strip, lang('en')
to_field 'cho_source', column('Reference'), strip, lang('en')
to_field 'cho_spatial', column('Mint'), split('||'), strip, prepend('Mint: '), lang('en')
to_field 'cho_spatial', column('Region'), split('||'), strip, prepend('Region: '), lang('en')
to_field 'cho_spatial', column('State'), split('||'), strip, prepend('State: '), lang('en')
to_field 'cho_temporal', column('Dynasty'), split('||'), strip, lang('en')
to_field 'cho_type', column('Object Type'), strip, lang('en')

# Agg
to_field 'agg_data_provider', data_provider, lang('en')
to_field 'agg_data_provider', data_provider_ar, lang('ar-Arab')

to_field 'agg_data_provider_country', data_provider_country, lang('en')
to_field 'agg_data_provider_country', data_provider_country_ar, lang('ar-Arab')
to_field 'agg_edm_rights', literal('https://creativecommons.org/share-your-work/public-domain/cc0/')
to_field 'agg_is_shown_at' do |_record, accumulator, context|
  accumulator << transform_values(context,
                                  'wr_id' => [column('RecordId'), prepend('http://numismatics.org/collection/')],
                                  'wr_dc_rights' => [literal('Public Domain')])
end
to_field 'agg_preview' do |_record, accumulator, context|
  accumulator << transform_values(context,
                                  'wr_id' => [column('Thumbnail_obv'), gsub('width175', 'width350')],
                                  'wr_dc_rights' => [literal('Public Domain')])
end
to_field 'agg_provider', provider, lang('en')
to_field 'agg_provider', provider_ar, lang('ar-Arab')
to_field 'agg_provider_country', provider_country, lang('en')
to_field 'agg_provider_country', provider_country_ar, lang('ar-Arab')

each_record convert_to_language_hash(
  'agg_data_provider',
  'agg_data_provider_country',
  'agg_provider',
  'agg_provider_country',
  'cho_alternative',
  'cho_contributor',
  'cho_coverage',
  'cho_creator',
  'cho_date',
  'cho_dc_rights',
  'cho_description',
  'cho_edm_type',
  'cho_extent',
  'cho_format',
  'cho_has_part',
  'cho_has_type',
  'cho_is_part_of',
  'cho_language',
  'cho_medium',
  'cho_provenance',
  'cho_publisher',
  'cho_relation',
  'cho_source',
  'cho_spatial',
  'cho_subject',
  'cho_temporal',
  'cho_title',
  'cho_type',
  'agg_data_provider_collection'
)

# NOTE: call add_cho_type_facet AFTER calling convert_to_language_hash fields
each_record add_cho_type_facet
