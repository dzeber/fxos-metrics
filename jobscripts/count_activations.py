# Run job to count records by date. 

import json
from datetime import date, timedelta
#from collections import namedtuple
import copy
import os.path

import ftu_formatter


# The directory containing the lookup tables. 
lookup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lookup")

# Loading for whitelists    . 
# Convert each list to convenient format for querying. 
def load_whitelist():
    with open(os.path.join(lookup_dir, 'ftu-fields.json')) as table_file:
        tables = json.load(table_file)
    # Country table will be straight lookup - use set.
    tables['country'] = set(tables['country'])
    # Device table contains string prefixes. Convert to tuple. 
    tables['device'] = tuple(tables['device'])
    return tables
    
    
# Loading for country codes. 
def load_country_table():
    with open(os.path.join(lookup_dir, 'countrycodes.json')) as table_file:
        table = json.load(table_file)
    return table

#-------------------

# Custom context writing functionality. 

# Convert a dict to a format that can be used as map-reduce keys, 
# preserving the structure of keys mapping to objects. 
# Convert to JSON format (as a string). 
# The items in the dict will be sorted alphabetically by field_name 
# to ensure that dicts containing the same keys are recorded as the same. 
def dict_to_key(d):
    # d = d.items()
    # d.sort(key=lambda e: e[0])
    # return tuple(d)
    return json.dumps(d, sort_keys=True)

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
# Key is of the form {'condition': condition}. 
def write_condition(context, condition):
    context.write(dict_to_key({'condition': condition}), 1)

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
    # Load lookup tables and join to context.
    if not hasattr(context, 'whitelist'):
        context.whitelist = load_whitelist()
    if not hasattr(context, 'country_table'):
        context.country_table = load_country_table()
    
    increment_counter(context, 'nrecords')
    
    try:
        data = json.loads(value)
        
        # Convert ping time to date.    
        # If missing or invalid, ignore record. 
        try: 
            ping_date = ftu_formatter.get_ping_date(data.get('pingTime'))
        except ValueError as e:
            write_condition(context, str(e))
            return
        
        # Create dataset row. 
        vals = {'pingdate': str(ping_date)}
        
        # Parse OS version string.
        # If missing or invalid, ignore record. 
        try:
            os = ftu_formatter.get_os_version(data.get('deviceinfo.os'))
        except ValueError as e:
            write_condition(context, str(e))
            return
        vals['os'] = os
        
        # Look up geo-country.
        vals['country'] = ftu_formatter.get_country(
            data.get('info').get('geoCountry'),
            context.whitelist['country'], 
            context.country_table)
        
        # Look up device name and reformat.
        vals['device'] = ftu_formatter.get_device_name(
            data.get('deviceinfo.product_model'),
            context.whitelist['device'])
        
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

