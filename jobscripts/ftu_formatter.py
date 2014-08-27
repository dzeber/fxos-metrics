# Sanitize/deduplicate field values to be counted.

import re
from datetime import date

# Regular expressions.     

# Add suffix to name separated by a space, if suffix is non-empty.
def add_suffix(name, suffix):
    if len(suffix) > 0:
        return name + ' ' + suffix
    return name

# Regular expressions for checking validity and formatting values. 
matches = dict(
    valid_os = re.compile('^(\d\.){3}\d([.\-]prerelease)?$', re.I)    
)

# Substitution patterns for formatting field values.
subs = dict(
    format_os = [{
        'regex': re.compile('[.\-]prerelease$', re.I),
        'repl': ' (pre-release)'
    },{
        'regex': re.compile(
            '^(?P<num>[1-9]\.[0-9](\.[1-9]){0,2})(\.0){0,2}', re.I),
        'repl': '\g<num>'
    }],
    format_device = [{
        # One Touch Fire. 
        'regex': re.compile(
            '^.*one\s*touch.*fire\s*(?P<suffix>[ce]?)(?:\s+\S*)?$', re.I),
        'repl': lambda match: add_suffix('One Touch Fire', 
            match.group('suffix').upper())
    },{
       # Open.
        'regex': re.compile(
            '^.*open\s*(?P<suffix>[2c])(?:\\s+\\S*)?$', re.I),
        'repl': lambda match: 'ZTE Open ' + match.group('suffix').upper()
    },{
        # Flame.
        'regex': re.compile('^.*flame.*$', re.I),
        'repl': 'Flame'
    },{ 
        # Geeksphone.
        'regex': re.compile('^.*(keon|peak).*$', re.I),
        'repl': lambda match: 'Geeksphone ' + match.group(1).capitalize()
    },{
        # Emulators/dev devices
        'regex': re.compile('^.*android|aosp.*$', re.I),
        'repl': 'Emulator/Android'
    }]
)

# Date range for ping dates to be considered valid.
valid_dates = {
    'earliest': date(2014, 4, 1),
    # Latest: a few days before today's date. 
    'latest': date.today() - timedelta(3)
}



#--------------------

# Processing for each individual field. 

# Convert the pingTime timestamp to a date.
# If an invalid condition occurs, throws ValueError with a custom message.
def get_ping_date(val):
    if val is None:
        raise ValueError('no ping time')
    
    try: 
        # pingTime is millisecond-resolution timestamp.
        val = int(val) / 1000
        pingdate = date.fromtimestamp(val)
    except Exception:
        raise ValueError('invalid ping time')
        
    if pingdate < valid_dates['earliest'] or pingdate > valid_dates['latest']:
        raise ValueError('outside date range')
    
    return pingdate


# Parse OS version. 
# If an invalid condition occurs, throws ValueError with a custom message.
def get_os_version(val):    
    if val is None:
        raise ValueError('no os version')
    os = str(val)
  
    # Check OS against expected format. 
    if matches['valid_os'].match(os) is None:
        raise ValueError('invalid os version')
    
    # Reformat to be more readable. 
    for s in subs['format_os']:
        os = s['regex'].sub(s['repl'], os, count = 1)
        
    return os


# Format device name. 
# Only record distinct counts for certain recognized device names. 
def get_device_name(val, recognized_list):
    if val is None:
        return 'Unknown'
    device = str(val)
    
    # Make formatting consistent to avoid duplication.
    for s in subs['format_device']:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit loop. 
        formatted, n = s['regex'].subn(s['repl'], device, count = 1)
        if n > 0:
            device = formatted
            break
    
    # Don't keep distinct name if does not start with recognized prefix.
    if not device.startswith(recognized_list): 
        return 'Other'
    
    return device


# Look up country name from 2-letter code. 
# Only record counts for recognized countries. 
def get_country(val, recognized_list, country_codes):
    if val is None:
        return 'Unknown'
    geo = str(val)
    
    # Look up country name. 
    if geo not in country_codes: 
        return 'Unknown'
    
    geo = country_codes[geo]['name']
    # Don't keep distinct name if not in recognized list. 
    if geo not in recognized_list: 
        return 'Other'
        
    return geo


# Logic to look up operator name from payload.
# Try looking up operator from SIM/ICC codes, if available. 
# If that fails, try using SIM SPN. 
# If no SIM is present, look up operator from network codes.
# If that fails, try reading network operator name field. 
# If none of these are present, operator is 'Unknown'.
def get_operator(icc_fields, network_fields, recognized_list, mobile_codes):
    if icc_fields is not None:
        # SIM is present. 
        if 'mcc' in icc_fields and 'mnc' in icc_fields:
            mcc = icc_fields['mcc']
            mnc = icc_fields['mnc']
            if mcc in mobile_codes:
                operator = mobile_codes[mcc]['operators'].get(mnc)
                if operator is not None:
                    return operator
        
        # At this point, something didnt



# Format operator name. 
# Only record counts for recognized operators. 


