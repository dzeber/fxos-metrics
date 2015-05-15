# Sanitize/deduplicate field values to be counted.

import json
import os.path
from datetime import datetime, date

import formatting_rules as fmt



# Lookup table handling.

# The directory containing the lookup tables. 
lookup_dir = os.path.join(os.path.dirname(__file__), 'lookup')

# Container for the lookup tables, to be loaded as necessary.
lookup = {}

# Loading for whitelists. 
# Convert each list to convenient format for querying. 
def load_whitelist():
    with open(os.path.join(lookup_dir, 'ftu-fields.json')) as table_file:
        tables = json.load(table_file)
    # Country table will be straight lookup - use set.
    lookup['countrylist'] = set(tables['country'])
    # Device table contains string prefixes. Convert to tuple. 
    lookup['devicelist'] = tuple(tables['device'])
    # Operator table will be a set.
    lookup['operatorlist'] = set(tables['operator'])

# Loading for country codes. 
def load_country_table():
    with open(os.path.join(lookup_dir, 'countrycodes.json')) as table_file:
        table = json.load(table_file)
    lookup['countrycodes'] = table

# Loading for list of recognized country names from code list. 
def load_country_names():
    if 'countrycodes' not in lookup:
        load_country_table()
    country_names = set(
        [ v['name'] for v in lookup['countrycodes'].itervalues() ])
    lookup['countrynames'] = country_names

# Loading for locale codes. 
def load_language_table():
    with open(os.path.join(lookup_dir, 'language-codes.json')) as table_file:
        table = json.load(table_file)
    lookup['langcodes'] = table

# Loading for mobile codes. 
def load_operator_table():
    with open(os.path.join(lookup_dir, 'mobile-codes.json')) as table_file:
        table = json.load(table_file)
    lookup['mobilecodes'] = table


#--------------------

# Make all substitutions in sequence.
# Sequence is a list of dicts with entries named 'regex' and 'repl'.
def make_all_subs(value, sub_list):
    for s in sub_list:
        value = s['regex'].sub(s['repl'], value, count = 1)
    return value


# Make at most one substitution in sequence. 
# Sequence is a list of dicts with entries named 'regex' and 'repl'.
def make_one_sub(value, sub_list):
    for s in sub_list:
        formatted, n = s['regex'].subn(s['repl'], value, count = 1)
        if n > 0:
            value = formatted
            break
    return value


# Convert millisecond timestamp to date.
def ms_timestamp_to_date(val):
    val = int(val) / 1000
    return datetime.utcfromtimestamp(val).date()

    
# Remove any leading zeros, unless string is all zeros.
def remove_leading_zeros(val):
    val = unicode(val).strip()
    if len(val) == 0:
        return ''
    val = val.lstrip('0')
    if len(val) == 0:
        return '0'
    return val

    
#--------------------

# Processing for each individual field. 

# Convert the pingTime timestamp to a date.
# If an invalid condition occurs, throws ValueError with a custom message.
def get_ping_date(val):
    if val is None:
        raise ValueError('no ping time')
    
    try: 
        # pingTime is millisecond-resolution timestamp.
        pingdate = ms_timestamp_to_date(val)
    except Exception:
        raise ValueError('invalid ping time')
    
    # Enforce date range.
    if (pingdate < fmt.valid_dates['earliest'] or 
            pingdate > fmt.valid_dates['latest']):
        raise ValueError('outside date range')
    
    return pingdate.isoformat()

# Determine whether or not to keep an FTU record based on its date.

# def relevant_date(rdate):
    # if date == '':
        # return False    
    
    # return (rdate >= fmt.valid_dates['earliest'] and
        # rdate <= fmt.valid_dates['latest'])

#------------------

# Update channel

# Map custom channel strings to one of the standard channels.
def get_standard_channel(val):
    std = fmt.standard_channels.search(unicode(val))
    if std is None:
        return 'other'
    return std.group()

#------------------

# Language/locale.

# def lookup_language(val):
    # loc = unicode(val).strip()


#------------------
    
# OS version
    
# Format OS string using regexes.
def format_os_string(val):
    return make_all_subs(unicode(val), fmt.os_subs)

# Convert formatted OS string to value that can be used in dashboard.
# Checks format against regex.
def summarize_os(val):
    # Check OS against format regex. If not matching, class as 'Other'.
    if fmt.valid_os.match(val) is None:
        return 'Other'
    
    return val


# Parse OS version. 
# If an invalid condition occurs, throws ValueError with a custom message.
# If OS value is not recognized, class as "Other".
def get_os_version(val):    
    if val is None:
        raise ValueError('no os version')
    # Reformat to be more readable. 
    os = format_os_string(val)
    # Check OS against format regex. If not matching, class as 'Other'.
    os = summarize_os(os)
    
    return os


#------------------

# Device name
    
# Format device name string based on regexes.
def format_device_string(val):
    return make_one_sub(unicode(val), fmt.device_subs)

# Convert device name to a value to be displayed in dashboard. 
# Looks up name in table of relevant countries. 
def summarize_device(val):
    if 'devicelist' not in lookup:
        load_whitelist()
    
    # If val was None in the FTU record, will be '' in the dump.
    if val == '':
        return 'Unknown'
    
    # Don't keep distinct name if does not start with recognized prefix.
    if not val.startswith(lookup['devicelist']): 
        return 'Other'
        
    return val


# Format device name. 
# Only record distinct counts for certain recognized device names.
# List of recognized devices is expected to be a tuple. 
def get_device_name(val):
    if val is None:
        return 'Unknown'
    device = format_device_string(val)
    # Don't keep distinct name if does not start with recognized prefix.
    device = summarize_device(device)
    
    return device



#------------------

# Country name
    
# Convert country codes to names. 
def lookup_country_code(val):
    if 'countrycodes' not in lookup:
        load_country_table()
    
    geo = unicode(val).strip()
    if geo not in lookup['countrycodes']: 
        return None
    return lookup['countrycodes'][geo]['name']

# Convert country name to a value to be displayed in dashboard. 
# Checks that name is a recognized country, 
# and looks up name in table of relevant countries. 
def summarize_country(val):
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
    
    
# Look up country name from 2-letter code. 
# Only record counts for recognized countries. 
# List of recognized countries is expected to be a set.
def get_country(val):
    if val is None:
        return 'Unknown'
    geo = lookup_country_code(val)
    if geo is None:
        return 'Unknown'
    geo = summarize_country(geo)
        
    return geo


#------------------

# Linguistic locale

def lookup_language(val):
    if 'langcodes' not in lookup:
        load_language_table()
    
    loc = unicode(val).strip()
    loc = fmt.locale_base_code['regex'].sub(
        fmt.locale_base_code['repl'], loc)
    
    return lookup['langcodes'].get(loc)


#------------------

# Operator name

# Look up mobile country code.
# Returns the country associated with the code, 
# or None if the code did not appear in the list.
def lookup_mcc(mcc):
    if 'mobilecodes' not in lookup:
        load_operator_table()
    
    mcc = remove_leading_zeros(mcc)    
    if mcc not in lookup['mobilecodes']:
        return None
        
    return lookup['mobilecodes'][mcc]['country']

# Look up mobile network code.
# Returns the operator associated with the code, 
# or None if the code did not appear in the list.
def lookup_mnc(mcc, mnc):
    if 'mobilecodes' not in lookup:
        load_operator_table()
    
    mcc = remove_leading_zeros(mcc)
    mnc = remove_leading_zeros(mnc)
    if mcc not in lookup['mobilecodes']:
        return None
        
    return lookup['mobilecodes'][mcc]['operators'].get(mnc)

# Look up mobile operator using mobile codes.
def lookup_operator_from_codes(fields):
    if 'mcc' not in fields or 'mnc' not in fields:
        # Missing codes. 
        return None
    
    return lookup_mnc(fields['mcc'], fields['mnc'])


# Look up mobile operator from field in payload.
def lookup_operator_from_field(fields, key):
    operator = fields.get(key)
    if operator is None:
        return None
        
    operator = unicode(operator).strip()
    if len(operator) == 0:
        return None
    
    return operator
    

# Logic to look up operator name from payload.
# Try looking up operator from SIM/ICC codes, if available. 
# If that fails, try using SIM SPN. 
# If no SIM is present, look up operator from network codes.
# If that fails, try reading network operator name field. 
# If none of these are present, operator is 'Unknown'.
def lookup_operator(icc_fields, network_fields):
    if icc_fields is not None:
        # SIM is present. 
        operator = lookup_operator_from_codes(icc_fields)
        if operator is not None:
            return operator
        
        # At this point, we were not able to resolve the operator 
        # from the codes.
        # Try the name string instead.
        operator = lookup_operator_from_field(icc_fields, 'spn')
        if operator is not None:
            return operator
    
    # Lookup using SIM card info failed.
    # Try using network info instead. 
    if network_fields is not None:
        operator = lookup_operator_from_codes(network_fields)
        if operator is not None:
            return operator
        
        # Otherwise, try the name string instead.
        operator = lookup_operator_from_field(network_fields, 'operator')
        if operator is not None:
            return operator
    
    # Lookup failed - no operator information in payload.
    return None


# Format operator name string using regexes.
def format_operator_string(val):
    return make_one_sub(val, fmt.operator_subs)

# Convert operator name to a value to be displayed in dashboard. 
# Input is list of [icc.network, icc.name, network.network, network.name]. 
# Deduces operator name based on presence of SIM card or network fields. 
# Checks name against table of recognized operators.
def summarize_operator(icc_network, icc_name, network_network, network_name):
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


# Format operator name. 
# Only record counts for recognized operators.
# List of recognized operators is expected to be a set.
def get_operator(icc_fields, network_fields):
    if 'operatorlist' not in lookup:
        load_whitelist()
    
    # Look up operator name either using mobile codes 
    # or from name listed in the data.
    operator = lookup_operator(icc_fields, network_fields)
    if operator is None or len(operator) == 0:
        return 'Unknown'
    operator = format_operator_string(operator)
    # Don't keep name if not in recognized list. 
    if operator not in lookup['operatorlist']: 
        return 'Other'
    
    return operator


#--------------------

# Additional formatting to cover special cases. 

# Replacement rules draw on combination of sanitized values 
# and other raw payload values. 
def format_values(clean_values, payload):
    return clean_values


# Formatting to be applied at local level
# while generating tables from raw data.
# Input is a data record as outputted from AWS job. 
# Ordering of values in data_row can be seen from dump_schema.py.
# def apply_post_formatting(data_row):
    # return data_row


# General formatting to be applied during MR job, 
# that relies on combinations of data values.
def apply_general_formatting(datum):
    datum = fmt.format_tarako(datum)
    datum = fmt.general_formatting(datum)
    return datum

