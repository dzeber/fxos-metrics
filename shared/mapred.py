# Helper functions for collecting data in a map-reduce context.

import json


# Reducer function that sums numeric values. 
def summing_reducer(key, values, context):
    context.write(key, sum(values))


#-------------------------------------
    
# JSON-based map output for dicts.
# Dicts are converted to JSON strings with named fields.

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


#-------------------------------------  

# Tuple-based output for dicts.
# Dicts are converted to tuples according to the ordering specified by a schema.
# The schema is an ordered list of dict keys.
# The first element of tuple-based output will be reserved 
# as an identifier for the type of record being outputted.

# Write a dict of field names mapping to values as a key
# mapping to 1, in order to count occurrences. 
# Key is of the form ('datum', ...).
# Values that get written and their order in the tuple is determined by 
# the schema, a list of keys to be looked up in d.
# Any items in d with keys not in the schema are omitted.
# Any keys in the schema that are missing from the dict 
# are added with the value ''.
def write_fieldvals_tuple(context, d, schema): 
    vals = ['datum']
    for key in schema:
        if key not in d or d[key] is None:
            vals.append('')
        else:
            vals.append(d[key])
    
    context.write(tuple(vals), 1)

# Increment a counter identified by a name optionally contained
# in a group by the specified value. 
# Key is of the form ('counter', <name>, <group>), 
# where <group> is optional.
def increment_counter_tuple(context, name, group=None, n=1):
    d = ['counter', name]
    if group is not None:
        d.append(group)
    context.write(tuple(d), n)

# Count occurrences of end conditions. 
# Key is of the form ('condition', <condition>). 
def write_condition_tuple(context, condition):
    context.write(('condition', condition), 1)

    
    
    

    
