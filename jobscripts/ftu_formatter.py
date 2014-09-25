# Sanitize/deduplicate field values to be counted.

# import re
import json
import os.path
from datetime import datetime
    # , timedelta

import formatting_rules as fmt



# Lookup table handling.

# The directory containing the lookup tables. 
lookup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lookup")

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

# Loading for mobile codes. 
def load_operator_table():
    with open(os.path.join(lookup_dir, 'mobile-codes.json')) as table_file:
        table = json.load(table_file)
    lookup['mobilecodes'] = table


#--------------------


# Add suffix to name separated by a space, if suffix is non-empty.
def add_suffix(name, suffix):
    if len(suffix) > 0:
        return name + ' ' + suffix
    return name

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

    
#--------------------

# Processing for each individual field. 

# Convert the pingTime timestamp to a date.
# If an invalid condition occurs, throws ValueError with a custom message.
def get_ping_date(val):
    if val is None:
        raise ValueError('no ping time')
    
    try: 
        # pingTime is millisecond-resolution timestamp.
        # val = int(val) / 1000
        # pingdate = date.fromtimestamp(val)
        pingdate = ms_timestamp_to_date(val)
    except Exception:
        raise ValueError('invalid ping time')
    
    # Enforce date range.
    if (pingdate < fmt.valid_dates['earliest'] or 
            pingdate > fmt.valid_dates['latest']):
        raise ValueError('outside date range')
    
    return pingdate

    
# Format OS string using regexes.
def format_os_string(val):
    return make_all_subs(unicode(val), fmt.os_subs)

# Parse OS version. 
# If an invalid condition occurs, throws ValueError with a custom message.
# If OS value is not recognized, class as "Other".
def get_os_version(val):    
    if val is None:
        raise ValueError('no os version')
    # os = unicode(val)
  
    # Check OS against expected format. 
    # if matches['valid_os'].match(os) is None:
        # raise ValueError('invalid os version')
    
    # Reformat to be more readable. 
    os = format_os_string(val)
    # Apply all patterns. 
    # for s in formatting_rules.os_subs:
        # os = s['regex'].sub(s['repl'], os, count = 1)
        
    # Check OS against format regex. If not matching, class as 'Other'.
    if fmt.valid_os.match(os) is None:
        return 'Other'
    
    return os


# Format device name string based on regexes.
def format_device_string(val):
    return make_one_sub(unicode(val), fmt.device_subs)

# Format device name. 
# Only record distinct counts for certain recognized device names.
# List of recognized devices is expected to be a tuple. 
def get_device_name(val):
    if 'devicelist' not in lookup:
        load_whitelist()
    
    if val is None:
        return 'Unknown'
    # device = unicode(val)
    
    # Make formatting consistent to avoid duplication.
    # Apply replacement regexes.
    # for s in formatting_rules.device_subs:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit loop. 
        # formatted, n = s['regex'].subn(s['repl'], device, count = 1)
        # if n > 0:
            # device = formatted
            # break
    # device = make_one_sub(device, fmt.device_subs)
    device = format_device_string(val)
    
    # Don't keep distinct name if does not start with recognized prefix.
    if not device.startswith(lookup['devicelist']): 
        return 'Other'
    
    return device

    
# Convert country codes to names. 
def lookup_country_code(val):
    if 'countrycodes' not in lookup:
        load_country_table()
    
    geo = unicode(val)
    if geo not in lookup['countrycodes']: 
        return None
    return lookup['countrycodes'][geo]['name']

# Look up country name from 2-letter code. 
# Only record counts for recognized countries. 
# List of recognized countries is expected to be a set.
def get_country(val):
    if 'countrylist' not in lookup:
        load_whitelist()
    
    if val is None:
        return 'Unknown'
    # geo = unicode(val)
    
    geo = lookup_country_code(val)
    if geo is None:
    # Look up country name. 
    # if geo not in lookup['countrycodes']: 
        return 'Unknown'
    
    # geo = lookup['countrycodes'][geo]['name']
    # Don't keep distinct name if not in recognized list. 
    if geo not in lookup['countrylist']: 
        return 'Other'
        
    return geo


# Look up mobile operator using mobile codes.
def lookup_operator_from_codes(fields):
    if 'mobilecodes' not in lookup:
        load_operator_table()
    
    if 'mcc' not in fields or 'mnc' not in fields:
        # Missing codes. 
        return None
    
    if fields['mcc'] not in lookup['mobilecodes']:
        # Country code is not recognized in lookup table.
        return None
    
    ops = lookup['mobilecodes'][fields['mcc']]['operators']
    return ops.get(fields['mnc'])

    
# Look up mobile operator from field in payload.
def lookup_operator_from_field(fields, key):
    operator = fields.get(key)
    if operator is None:
        return None
        
    operator = str(operator).strip()
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
def format_operartor_string(val):
    return make_one_sub(val, fmt.operator_subs)
    
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
        
    # Make formatting consistent to avoid duplication.
    # Apply replacement regexes.
    # for s in formatting_rules.operator_subs:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit loop. 
        # formatted, n = s['regex'].subn(s['repl'], operator, count = 1)
        # if n > 0:
            # operator = formatted
            # break
    # operator = make_one_sub(operator, fmt.operator_subs)
    operator = format_operartor_string(operator)
    
    # Don't keep name if not in recognized list. 
    if operator not in lookup['operatorlist']: 
        return 'Other'
    
    return operator


# Additional formatting to cover special cases. 
# Replacement rules draw on combination of sanitized values 
# and other raw payload values. 
def format_values(clean_values, payload):
    # Discard v1.5.
    # if clean_values['os'].startswith(('1.0', '1.5', '2.2')):
        # raise ValueError('Ignoring OS version')
    #
    # Tarako/India.
    # OS should either be standard or else one of the Tarako strings.
    # if clean_values['os'].lower().startswith(('ind_', 'intex_')):
        # If the Tarako devices are from India, record. 
        # if clean_values['country'] == 'India':
        # clean_values['os'] = '1.3T'
        # else: 
        # Discard.
            # raise ValueError('Ignoring non-India Tarako')
    #
    return clean_values






