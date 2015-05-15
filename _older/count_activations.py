# Run job to count records by date. 

import json
import copy

import ftu_formatter as ftu
import mapred


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
    # if not hasattr(context, 'whitelist'):
        # context.whitelist = load_whitelist()
    # if not hasattr(context, 'country_table'):
        # context.country_table = load_country_table()
    # if not hasattr(context, 'operator_table'):
        # context.operator_table = load_operator_table()
    
    mapred.increment_counter(context, 'nrecords')
    
    try:
        data = json.loads(value)
        
        # Convert ping time to date.    
        # If missing or invalid, ignore record. 
        try: 
            ping_date = ftu.get_ping_date(data.get('pingTime'))
        except ValueError as e:
            mapred.write_condition(context, str(e))
            return
        
        # Create dataset row. 
        vals = {'pingdate': str(ping_date)}
        
        # Parse OS version string.
        # If missing or invalid, ignore record. 
        try:
            os = ftu.get_os_version(data.get('deviceinfo.os'))
        except ValueError as e:
            mapred.write_condition(context, str(e))
            return
        vals['os'] = os
        
        # Look up geo-country.
        vals['country'] = ftu.get_country(
            data.get('info').get('geoCountry')
            # ,context.whitelist['country'] 
            # ,context.country_table
            )
        
        # Look up device name and reformat.
        vals['device'] = ftu.get_device_name(
            data.get('deviceinfo.product_model')
            # ,context.whitelist['device']
            )
            
        # Look up mobile operator.
        vals['operator'] = ftu.get_operator(
            data.get('icc'), data.get('network')
            # ,context.whitelist['operator'] 
            # ,context.operator_table
            )
        
        # Apply any additional formatting based on sanitized values 
        # and other data fields. 
        try:
            vals = ftu.format_values(vals, data)
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
reduce = mapred.summing_reducer
combine = reduce

# def reduce(key, values, context):
    # context.write(key, sum(values))

# combine = reduce

