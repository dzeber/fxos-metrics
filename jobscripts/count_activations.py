# Sanitize field values and count occurrences by date. 

import json
from datetime import date, timedelta
from collections import namedtuple
import re   
import copy


# The earliest ping date to consider. 
earliest_valid_date = date(2014, 4, 1)

# The latest ping date to consider. 
# A few days before today's date. 
latest_valid_date = date.today() - timedelta(3)

# Format for a dataset row. 
colvars = 'pingdate, os, country, device'
DataRow = namedtuple('DataRow', colvars)

# Mapreduce counter.
Counter = namedtuple('Counter', 'counter_group, counter_name')

def increment_counter(context, name, group='', n=1):
    context.write(Counter._make([group, name]), n)

# Facility for counting end conditions.
Condition = namedtuple('Condition', 'condition')

def write_condition(context, condition):
    context.write(Condition._make([condition]), 1)


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

    
#-------------------------------------

# Map-reduce job.

    
def map(key, dims, value, context):
    # Add condition and counter writers to context
    if not hasattr(context, "writecond"):
        context.writecond = write_condition
    if not hasattr(context, "count"):
        context.count = increment_counter
    
    try:
        data = json.loads(value)
        
        # Convert ping time to date. 
        # If missing or invalid, ignore record. 
        try: 
            ping_date = get_ping_date(data.get('pingTime'))
        except ValueError as e:
            context.writecond(str(e))
            return
        
        # Create dataset row. 
        vals = {'pingdate': ping_date}
        
        # Parse OS version string.
        # If missing or invalid, ignore record. 
        try:
            os = get_os_version(data.get('deviceinfo.os'))
        except ValueError as e:
            context.writecond(str(e))
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
            context.write(DataRow(**v), 1)
    
    except Exception as e:
        context.writecond(str(e))
        return


# Summing reducer with combiner. 
def reduce(key, values, context):
    context.write(key, sum(values))

combine = reduce
