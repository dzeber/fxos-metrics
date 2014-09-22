# Run job to count records by date. 

import json
import copy
import os.path

import ftu_formatter
import mapred


# The directory containing the lookup tables. 
lookup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lookup")

# Loading for whitelists. 
# Convert each list to convenient format for querying. 
def load_whitelist():
    with open(os.path.join(lookup_dir, 'ftu-fields.json')) as table_file:
        tables = json.load(table_file)
    # Country table will be straight lookup - use set.
    tables['country'] = set(tables['country'])
    # Device table contains string prefixes. Convert to tuple. 
    tables['device'] = tuple(tables['device'])
    # Operator table will be a set.
    tables['operator'] = set(tables['operator'])
    return tables

    
# Loading for country codes. 
def load_country_table():
    with open(os.path.join(lookup_dir, 'countrycodes.json')) as table_file:
        table = json.load(table_file)
    return table

# Loading for mobile codes. 
def load_operator_table():
    with open(os.path.join(lookup_dir, 'mobile-codes.json')) as table_file:
        table = json.load(table_file)
    return table


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
    if not hasattr(context, 'operator_table'):
        context.operator_table = load_operator_table()
    
    mapred.increment_counter(context, 'nrecords')
    
    try:
        data = json.loads(value)
        
        # Convert ping time to date.    
        # If missing or invalid, ignore record. 
        try: 
            ping_date = ftu_formatter.get_ping_date(data.get('pingTime'))
        except ValueError as e:
            mapred.write_condition(context, str(e))
            return
        
        # Create dataset row. 
        vals = {'pingdate': str(ping_date)}
        
        # Parse OS version string.
        # If missing or invalid, ignore record. 
        try:
            os = ftu_formatter.get_os_version(data.get('deviceinfo.os'))
        except ValueError as e:
            mapred.write_condition(context, str(e))
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
            
        # Look up mobile operator.
        vals['operator'] = ftu_formatter.get_operator(
            data.get('icc'), data.get('network'),
            context.whitelist['operator'], 
            context.operator_table)
        
        # Apply any additional formatting based on sanitized values 
        # and other data fields. 
        try:
            vals = ftu_formatter.format_values(vals, data)
        except ValueError as e:
            mapred.write_condition(context, str(e))
            return
            
        # Add entries for "All" by expanding combinations of fields. 
        vals = expand_all(vals)
        
        # Output data row.
        for v in vals: 
            mapred.write_fieldvals(context, v)
    
    except Exception as e:
        mapred.write_condition(context, str(e))
        return


# Summing reducer with combiner. 
def reduce(key, values, context):
    context.write(key, sum(values))

combine = reduce

