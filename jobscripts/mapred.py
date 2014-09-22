# Helper functions for collecting data in a map-reduce context.

import json


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
