"""
Utility functions for use in map-reduce jobs. 

These mainly provide a way to package and unpackage map-reduce output, based
around the concept of counting occurrences of collections of data values. The 
relevant data values in the payload are represented as a dict mapping field
names to values. The map phase outputs one such dict for each payload, and the
reduce phase counts occurrences of such unique dicts (as a data compression 
step). The dicts need to be represented as a Python object that works with
the telemetry-server map-reduce system. Two options are JSON strings and 
tuples with a specified ordering. Currently only the tuple approach is in use.

The functions provide shortcuts for outputting a collection of data values, 
incrementing counters, and counting occurrences of special conditions
identified by simple strings. There is also a parsing function for 
reconstituting the tuple-based map-reduce output back into dicts.

A simple summing reduce function is also defined here, for convenience.
"""

import json
import ast

def summing_reducer(key, values, context):
    """Reducer function that sums numeric values."""
    context.write(key, sum(values))


#==============================================================

# Tuple-based output for dicts.
# Dicts are converted to tuples according to the ordering 
# specified by a schema.
# The schema is an ordered list of dict keys.
# The first element of tuple-based output will be reserved 
# as an identifier for the type of record being outputted.


def write_fieldvals_tuple(context, d, schema): 
    """Write a dict of field names mapping to values as a key mapping to 1, 
    in order to count occurrences. 
    
    Key is of the form ('datum', ...).
    Values that get written and their order in the tuple is determined by 
    the schema, a list of keys to be looked up in d.
    Any items in d with keys not in the schema are omitted.
    Any keys in the schema that are missing from the dict 
    are added with the value ''.
    """
    vals = ['datum']
    for key in schema:
        if key not in d or d[key] is None:
            vals.append('')
        else:
            vals.append(d[key])
    
    context.write(tuple(vals), 1)


def increment_counter_tuple(context, name, group=None, n=1):
    """Increment a map-reduce counter by a specified value. 
    
    The name can optionally be contained in a group. The key is of the form 
    ('counter', <name>, <group>), where <group> is optional.
    """
    d = ['counter', name]
    if group is not None:
        d.append(group)
    context.write(tuple(d), n)


def write_condition_tuple(context, condition):
    """Count occurrences of end conditions. 
    
    Key is of the form ('condition', <condition>). 
    """    
    context.write(('condition', condition), 1)


def parse_output_tuple(output_file):
    """Parse back the output of a map-reduce job recorded using tuples.

    Read in output file containing one output record per line. 
    Separate records, conditions and counters.
    Records will be returned as lists with the count appended at the end.
    Output is a map with keys 'records', 'counters', 'conditions'.
    """
    # Initialize storage. 
    data = {}
    data['records'] = []
    data['counters'] = {}
    data['conditions'] = {}
    
    # Parse records line by line.
    for row in open(output_file):
        parsed_row = row.rstrip().rsplit('\t', 1)
        
        vals = list(ast.literal_eval(parsed_row[0]))
        # Strip the key type identifier.
        type = vals.pop(0)
        n = int(parsed_row[1])
        
        # Proceed according to key type. 
        if type == 'condition': 
            if 'conditions' not in data:
                data['conditions'] = {}
            data['conditions'][vals[0]] = n
            continue
        
        if type == 'counter':
            if 'counters' not in data:
                data['counters'] = {}
            # If the counter has a group, 
            # create a subdict for it.
            # Otherwise store as a single value.
            if len(vals) == 2:
                group_name = vals[1]
                if group_name not in data['counters']:
                    data['counters'][group_name] = {}
                data['counters'][group_name][vals[0]] = n
            else:
                data['counters'][vals[0]] = n
            continue
        
        # Otherwise we a have a data record.
        if 'records' not in data:
            data['records'] = []
        vals.append(n)
        data['records'].append(vals)
    
    return data


#==============================================================

# JSON-based map output for dicts.
# Dicts are converted to JSON strings with named fields.
# These JSON-based outputs are not currently used in the fxos-metrics jobs.


# def dict_to_key(d):
    # """Convert a dict to a format that can be used as a map-reduce key.
    
    # Convert to JSON format (as a string). The items in the dict will be sorted
    # alphabetically by field_name to ensure that dicts containing the same keys 
    # are recorded as the same. 
    # """
    # return json.dumps(d, sort_keys=True)


# def write_fieldvals(context, d): 
    # """Write a dict of field names mapping to values as a key mapping to 1,
    # in order to count occurrences. 
    # """
    # context.write(dict_to_key(d), 1)


# def increment_counter(context, name, group='', n=1):
    # """Increment a map-reduce counter by a specified value. 
    
    # The name can optionally be contained in a group. The key is of the form 
    # {'counter': name, 'group': group}, with group omitted if not specified. 
    # """
    # k = {'counter': name}
    # if len(group) > 0: 
        # k['group'] = group
    # context.write(dict_to_key(k), n)


# def write_condition(context, condition):
    # """Count occurrences of end conditions. 
    
    # Key is of the form {'condition': condition}. 
    # """
    # context.write(dict_to_key({'condition': condition}), 1)


