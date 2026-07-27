# Test config that exercises EXTRACT_MACROS in mapping_report
to_field 'cho_title', generate_edm_type('Title')
to_field 'cho_creator', xpath_title_plus('Creator')
to_field 'cho_subject', column('Subject'), prepend('Subject: ')
