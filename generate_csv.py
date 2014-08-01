# Convert the ouptput of the count_activations job to a CSV 
# suitable for loading into dashboard. 

import json

outfile = 'test_act.out'

records = []
conditions = {}
counters = {}

# Parse output file.
for row in open(outfile):
    parsed_row = row.rstrip().rsplit('\t', 1)
    key = json.loads(parsed_row[0])
    n = int(parsed_row[1])
    if 'condition' in key:
        # Store mapping of condition counts. 
        conditions[key['condition']] = n
    elif 'counter' in key:
        # Store list of counters. 
        if 'group' in key:
            # If the counter has a container group, 
            # add the counter in a subdict keyed by group name. 
            group_name = key['group']
            if group_name not in counters:
                counters[group_name] = {}
            grouplist = counters[group_name]
            grouplist[key['counter']] = n
        else:
            counters[key['counter']] = n    
    else:
        key['count'] = n
        records.append(key)

