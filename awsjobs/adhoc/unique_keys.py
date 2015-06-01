"""
Find the list of keys that occur across the JSON payloads, counting occurrences.
"""

import json
import utils.mapred as mapred

def get_keys(obj, prefix, keys, exclude, sep = '|'):
    """ Find all the key paths leading down all levels of subdicts in JSON-like
        hierarchical structure.
        
        obj - a dict containing possibly nested subdicts
        prefix - a prefix to append to all keys, used to maintain key paths
        keys - storage list to which keypaths will be appended
        exclude - a list of keypaths that should not be searched further
    """
    if isinstance(obj, dict):
        for k in obj:
            keypath = prefix + sep + k if prefix else k
            keys.append(keypath)
            if keypath not in exclude:
                get_keys(obj[k], keypath, keys, exclude)
    # Base case: if the object is not a dict, is it a data value.
    # Nothing to do.


def map(key, dims, value, context):
    """Parse the JSON, and traverse each dict level, emitting key names.
    """
    mapred.increment_counter_tuple(context, 'nrecords')
    
    try:
        r = json.loads(value)
        keys = list()
        get_keys(r, '', keys, ['info|apps'])
        skeys = set(keys)
        if len(skeys) != len(keys):
            mapred.write_condition_tuple(context, 'nonunique')
        for k in skeys:
            context.write(('datum', 'k'), 1)
    except Exception as e:
        mapred.write_condition_tuple(context, type(e).__name__ + ' ' + str(e))

# Summing reducer with combiner. 
reduce = mapred.summing_reducer
combine = reduce

            