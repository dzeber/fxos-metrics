"""
Functions to apply sanity checking and reformatting to specific fields
in the FxOS payloads.

Reformatting is an important deduplication step that ensures that records
containing get grouped together despite small differences in the string
identifiers.

Some of the formatting involves converting short codes to human-readable 
names. This is done using lookup tables provided in JSON format in the 'lookup' 
subdirectory. The lookup tables are loaded from file when first needed and 
cached. 

Reformatting is used both in the map-reduce processing of the raw data, and 
the conversion of downloaded data to a dashboard-ready CSV. In the map-reduce 
stage it is used for deduplication and cleansing of the values appearing
in the raw data. Most of this is done using the regexes and functions collected 
in formatting_rules.py. 

In the postprocessing step, it is mostly used for limiting the distinct values 
of a field to a set of the most relevant values, grouping together any other 
values as 'Other'. The main purpose of this is to limit the number of values 
appearing on the dashboard for usability and clarity. The whitelists of 
relevant field values to be retained in the dashboards are maintained in 
lookup/ftu-fields.json.
"""


import json
import os.path
from datetime import datetime, date

import formatting_rules as fmt


# Lookup table handling
# ---------------------


# The directory containing the lookup tables. 
lookup_dir = os.path.join(os.path.dirname(__file__), 'lookup')

# Container for the lookup tables, to be loaded as necessary.
lookup = {}

def load_whitelist():
    """Load the whitelists, converting each list to convenient formats."""
    with open(os.path.join(lookup_dir, 'ftu-fields.json')) as table_file:
        tables = json.load(table_file)
    # Country table will be straight lookup - use set.
    lookup['countrylist'] = set(tables['country'])
    # Device table contains string prefixes. Convert to tuple. 
    lookup['devicelist'] = tuple(tables['device'])
    # Operator table will be a set.
    lookup['operatorlist'] = set(tables['operator'])


def load_country_table():
    """Load the lookup table for country codes."""
    with open(os.path.join(lookup_dir, 'countrycodes.json')) as table_file:
        table = json.load(table_file)
    lookup['countrycodes'] = table


def load_country_names():
    """Load the table of recognized country names from the country code list."""
    if 'countrycodes' not in lookup:
        load_country_table()
    country_names = set(
        [ v['name'] for v in lookup['countrycodes'].itervalues() ])
    lookup['countrynames'] = country_names


def load_language_table():
    """Load the lookup table for locale codes."""
    with open(os.path.join(lookup_dir, 'language-codes.json')) as table_file:
        table = json.load(table_file)
    lookup['langcodes'] = table


def load_operator_table():
    """Load the lookup table for mobile codes."""
    with open(os.path.join(lookup_dir, 'mobile-codes.json')) as table_file:
        table = json.load(table_file)
    lookup['mobilecodes'] = table


#==============================================================

# General utility functions
# -------------------------


def make_all_subs(value, sub_list):
    """Apply all substitutions in the sequence sub_list to a value.
    
    Sequence is a list of dicts with entries named 'regex' and 'repl'.
    """
    for s in sub_list:
        value = s['regex'].sub(s['repl'], value, count = 1)
    return value


def make_one_sub(value, sub_list):
    """Apply at most one substitution in the sequence sub_list to a value. 
    
    Sequence is a list of dicts with entries named 'regex' and 'repl'.
    """
    for s in sub_list:
        formatted, n = s['regex'].subn(s['repl'], value, count = 1)
        if n > 0:
            value = formatted
            break
    return value


def ms_timestamp_to_date(val):
    """Convert millisecond timestamp to date."""
    val = int(val) / 1000
    return datetime.utcfromtimestamp(val).date()


def remove_leading_zeros(val):
    """Remove any leading zeros from a string of digits. 
    
    If the string is all zeros, return '0'.
    """
    val = unicode(val).strip()
    if len(val) == 0:
        return ''
    val = val.lstrip('0')
    if len(val) == 0:
        return '0'
    return val


#==============================================================

# Formatting to be applied in the map-reduce job
# ----------------------------------------------


def get_standard_channel(val):
    """Map custom channel strings to one of the standard channels."""
    std = fmt.standard_channels.search(unicode(val))
    if std is None:
        return 'other'
    return std.group()


def format_os_string(val):
    """Reformat OS string using regexes."""
    return make_all_subs(unicode(val), fmt.os_subs)


def format_device_string(val):
    """Reformat device name string based on regexes."""
    return make_one_sub(unicode(val), fmt.device_subs)


def lookup_country_code(val):
    """Convert country codes to recognizable names."""
    if 'countrycodes' not in lookup:
        load_country_table()
    
    geo = unicode(val).strip()
    if geo not in lookup['countrycodes']: 
        return None
    return lookup['countrycodes'][geo]['name']


def lookup_language(val):
    """Convert locale code to a recognizable language name.
    
    The locale code is standardized using regexes, and then looked up in 
    a table.
    """
    if 'langcodes' not in lookup:
        load_language_table()
    
    loc = unicode(val).strip()
    loc = fmt.locale_base_code['regex'].sub(
        fmt.locale_base_code['repl'], loc)
    
    return lookup['langcodes'].get(loc)


def format_operator_string(val):
    """Reformat operator name string using regexes."""
    return make_one_sub(val, fmt.operator_subs)


def lookup_mcc(mcc):
    """Look up the mobile country code in the lookup table.
    
    Return the country associated with the code, or None if the code did not 
    appear in the list.
    """
    if 'mobilecodes' not in lookup:
        load_operator_table()
    
    mcc = remove_leading_zeros(mcc)    
    if mcc not in lookup['mobilecodes']:
        return None
        
    return lookup['mobilecodes'][mcc]['country']


def lookup_mnc(mcc, mnc):
    """Look up mobile network code in the lookup table.
    
    Return the operator associated with the code, or None if the code did not 
    appear in the list. Note that looking up the network code requires both 
    the network code and the country code. 
    """
    if 'mobilecodes' not in lookup:
        load_operator_table()
    
    mcc = remove_leading_zeros(mcc)
    mnc = remove_leading_zeros(mnc)
    if mcc not in lookup['mobilecodes']:
        return None
        
    return lookup['mobilecodes'][mcc]['operators'].get(mnc)


def apply_general_formatting(datum):
    """Apply miscellaneous formatting rules to the payload values.
    
    This is intended to be applied during the map-reduce job. The input is
    the entire data record represented as a dict, after all the other specific 
    formatting has been applied. The formatting rules applied here can thus 
    depend on combinations of data values, and can be used to correct errors
    in the recorded data on a case-by-case basis.
    
    Currently the main effect of the formatting done here is to correctly
    identify Tarako builds.
    """
    datum = fmt.format_tarako(datum)
    datum = fmt.general_formatting(datum)
    return datum


#==============================================================

# Summarization to be applied in the postprocessing step
# ------------------------------------------------------


def summarize_os(val):
    """Convert formatted OS string to value that can be used in dashboard.
    
    A regex is used to identify valid recognized OS versions. Values that
    are not matched are 
    """
    if fmt.valid_os.match(val) is None:
        return 'Other'
    
    return val


def summarize_device(val):
    """Convert formatted device name to a value to be displayed in dashboard.
    
    The name is looked up in a table of relevant devices. If it is not found,
    it is replaced with 'Other'. If the device name was missing in the payload,
    it will be replaced with 'Unknown'.
    """
    if 'devicelist' not in lookup:
        load_whitelist()
    
    # If val was None in the FTU record, will be '' in the dump.
    if val == '':
        return 'Unknown'
    
    # Don't keep distinct name if does not start with recognized prefix.
    if not val.startswith(lookup['devicelist']): 
        return 'Other'
        
    return val


def summarize_country(val):
    """Convert country name to a value to be displayed in dashboard. 
    
    The name will be replaced with 'Unknown' if it is missing or not a 
    recognizable country name, or 'Other' if it is not in the list of relevant 
    countries.
    """
    if 'countrylist' not in lookup:
        load_whitelist()
    if 'countrynames' not in lookup:
        load_country_names()
    
    # If val was None in the FTU record, will be '' in the dump.
    if val == '':
        return 'Unknown'
    
    # Country will be name, or else country code if code was not recognized. 
    # Check whether val is the name of one of the recognized codes. 
    if val not in lookup['countrynames']: 
        return 'Unknown'
    
    # Don't keep distinct name if not in recognized list. 
    if val not in lookup['countrylist']: 
        return 'Other'
        
    return val


def summarize_operator(icc_network, icc_name, network_network, network_name):
    """Convert operator name to a value to be displayed in dashboard.
    
    Input is list of [icc.network, icc.name, network.network, network.name]. 
    The operator name is deduced based on presence of SIM card or network 
    fields using the following heuristic. If both network code and name string
    are present, the code is preferred. If both SIM information and network
    information are present, SIM information is preferred. 
    
    The name is then checked against table of relevant operators, and 
    replaced with 'Unknown' if it is missing, or 'Other' if it is not in the 
    list.
    """
    if 'operatorlist' not in lookup:
        load_whitelist()
    
    # Determine operator based on information first from SIM card,
    # then from network. 
    network_vals = [icc_network, icc_name, network_network, network_name]
    operator = ''
    for v in network_vals:
        if v != '':
            operator = v
            break
    
    if operator == '':
        return 'Unknown'
        
    # Don't keep name if not in recognized list. 
    if operator not in lookup['operatorlist']: 
        return 'Other'
    
    return operator


#==============================================================


# def lookup_operator_from_codes(fields):
    # """Look up mobile operator using mobile codes represented as a dict."""
    # if 'mcc' not in fields or 'mnc' not in fields:
        # # Missing codes. 
        # return None
    
    # return lookup_mnc(fields['mcc'], fields['mnc'])

# def lookup_operator_from_field(fields, key):
    # """Read the mobile operator name from the name string in the payload."""
    # operator = fields.get(key)
    # if operator is None:
        # return None
        
    # operator = unicode(operator).strip()
    # if len(operator) == 0:
        # return None
    
    # return operator

# def lookup_operator(icc_fields, network_fields):
    # """Identify the mobile operator name from the payload.
    
    # Try looking up operator from the SIM/ICC codes, if available. If that fails, 
    # try using SIM SPN (network name). 
    
    # If no SIM is present, look up operator from the network codes. If that 
    # fails, try reading network operator name field. 
    
    # If none of these are present, operator is 'Unknown'.
    # """
    # if icc_fields is not None:
        # # SIM is present. 
        # operator = lookup_operator_from_codes(icc_fields)
        # if operator is not None:
            # return operator
        
        # # At this point, we were not able to resolve the operator 
        # # from the codes.
        # # Try the name string instead.
        # operator = lookup_operator_from_field(icc_fields, 'spn')
        # if operator is not None:
            # return operator
    
    # # Lookup using SIM card info failed.
    # # Try using network info instead. 
    # if network_fields is not None:
        # operator = lookup_operator_from_codes(network_fields)
        # if operator is not None:
            # return operator
        
        # # Otherwise, try the name string instead.
        # operator = lookup_operator_from_field(network_fields, 'operator')
        # if operator is not None:
            # return operator
    
    # # Lookup failed - no operator information in payload.
    # return None


# def format_values(clean_values, payload):
    # """Apply other miscellaneous formatting rules.

    # These can be based on the combination of sanitized values and other 
    # raw payload values. Currently, no rules are applied.
    # """
    # return clean_values


# Formatting to be applied at local level
# while generating tables from raw data.
# Input is a data record as outputted from AWS job. 
# Ordering of values in data_row can be seen from dump_schema.py.
# def apply_post_formatting(data_row):
    # return data_row



