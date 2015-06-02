"""
Find the list of keys that occur across the JSON payloads, counting occurrences.
"""

import json
import utils.mapred as mapred
import payload_utils as util


def map(key, dims, value, context):
    """Parse the JSON, and traverse each dict level, emitting key names.
    """
    mapred.increment_counter_tuple(context, 'nrecords')
    
    try:
        r = json.loads(value)
        keys = util.get_keypaths(r, (['info','apps'], ['info','searches'],))
        skeys = set(keys)
        if len(skeys) != len(keys):
            mapred.write_condition_tuple(context, 'nonunique')
        for k in skeys:
            context.write(('datum', k), 1)
    except Exception as e:
        mapred.write_condition_tuple(context, type(e).__name__ + ' ' + str(e))

# Summing reducer with combiner. 
reduce = mapred.summing_reducer
combine = reduce

            