# Sanitize field values and count occurrences by date. 

import json
from datetime import date, timedelta
from collections import namedtuple
import re   
import copy


#-------------------

# Valid date range for ping dates.

# The earliest ping date to consider. 
earliest_valid_date = date(2014, 4, 1)

# The latest ping date to consider. 
# A few days before today's date. 
latest_valid_date = date.today() - timedelta(3)

#--------------------

# Custom context writing functionality. 

# Convert a dict to a format that can be used as map-reduce keys, 
# preserving the structure of keys mapping to objects. 
# Convert to a tuple of tuples of the form ("field_name", field_value).
# The elements will be sorted  alphabetically by field_name 
# to ensure that dicts containing the same keys are recorded as the same. 
def dict_to_key(d):
    d = d.items()
    d.sort(key=lambda e: e[0])
    return tuple(d)
    
# Write a dict of field names mapping to values as a key
# mapping to 1, in order to count occurrences. 
def write_fieldvals(context, d): 
    context.write(dict_to_key(d), 1)
    
# Increment a counter identified by a name optionally contained
# in a group by the specified value. 
# Key is of the form  {'counter': name, 'group': group}, 
# with group omitted if not specified. 
def increment_counter(context, name, group='', n=1):
    k = {'counter': name}
    if len(group) > 0: 
        # k.append(('group', group))
        k['group'] = group
    context.write(dict_to_key(k), n)
    
# Count occurrences of end conditions. 
# Key is of the form ('condition', condition). 
def write_condition(context, condition):
    context.write(('condition', condition), 1)

# Format for a dataset row. 
# colvars = 'pingdate, os, country, device'
# DataRow = namedtuple('DataRow', colvars)

# # Mapreduce counter.
# Counter = namedtuple('Counter', 'counter_group, counter_name')

# def increment_counter(context, name, group='', n=1):
    # context.write(Counter._make([group, name]), n)

# # Facility for counting end conditions.
# Condition = namedtuple('Condition', 'condition')

# def write_condition(context, condition):
    # context.write(Condition._make([condition]), 1)


#--------------------

# Regular expressions.     

# Add suffix to name separated by a space, if suffix is non-empty.
def add_suffix(name, suffix):
    if len(suffix) > 0:
        return name + ' ' + suffix
    else:
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
        
    if pingdate < earliest_valid_date or pingdate > latest_valid_date:
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
def get_device_name(val):
    if val is None:
        return 'unknown'
    
    device = str(val)
    
    # Make formatting consistent to avoid duplication.
    for s in subs['format_device']:
        # Device name patterns should be mutually exclusive.
        # If any regex matches, make the replacement and exit. 
        formatted, n = s['regex'].subn(s['repl'], device, count = 1)
        if n > 0:
            return formatted
    
    # Otherwise return the device name unchanged.
    return device

#--------------------

# Map-reduce job.
    
# Expand a dict to a list of dicts
# containing a copy of the original dict for each subset of its keys
# with keys in the subset mapping to 'All'.
def expand_all(d):
    if len(d) == 0:
        return [{}]
        
    k,v = d.popitem()
    # Expand over remaining items.
    expanded = expand_all(d)
    # For each of these, add expansion of first item.
    expanded2 = copy.deepcopy(expanded)
    for i in range(len(expanded)):
        expanded[i][k] = v
        expanded2[i][k] = 'All'
    return expanded + expanded2
    
# Mapper looks up and processes fields. 
def map(key, dims, value, context):
    # Add condition and counter writers to context
    # if not hasattr(context, "writecond"):
        # context.writecond = write_condition
    # if not hasattr(context, "increment"):
        # context.increment = increment_counter
    
    increment_counter(context, 'nrecords')
    
    try:
        data = json.loads(value)
        
        # Convert ping time to date. 
        # If missing or invalid, ignore record. 
        try: 
            ping_date = get_ping_date(data.get('pingTime'))
        except ValueError as e:
            write_condition(context, str(e))
            return
        
        # Create dataset row. 
        vals = {'pingdate': str(ping_date)}
        
        # Parse OS version string.
        # If missing or invalid, ignore record. 
        try:
            os = get_os_version(data.get('deviceinfo.os'))
        except ValueError as e:
            write_condition(context, str(e))
            return
        vals['os'] = os
        
        # Look up geo-country.
        vals['country'] = data.get('info').get('geoCountry', 'unknown')
        
        # Look up device name and reformat.
        vals['device'] = get_device_name(data.get('deviceinfo.product_model'))
        
        # Add entries for "All" by expanding combinations of fields. 
        vals = expand_all(vals)
        
        # Output data row.
        for v in vals: 
            write_fieldvals(context, v)
    
    except Exception as e:
        write_condition(context, str(e))
        return


# Summing reducer with combiner. 
def reduce(key, values, context):
    context.write(key, sum(values))

combine = reduce
