# Convert the ouptput of the count_activations job to a CSV 
# suitable for loading into dashboard. 

# Arguments supplied should be the input data file and the output CSV file. 

import json
import csv
import sys

job_output = sys.argv[1]
csv_file = sys.argv[2]
headers = ['pingdate', 'os', 'country', 'device', 'count']
final_headers = ['date', 'os', 'country', 'device', 'activations']


records = []
conditions = {}
counters = {}

# Parse output file.
for row in open(job_output):
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
        if key['pingdate'] != 'All':
            key['count'] = n
            records.append(key)

# Ouput CSV. 
with open(csv_file, 'w') as outfile:
    outfile.write(','.join(final_headers))
    outfile.write('\n')
    writer = csv.DictWriter(outfile, headers, extrasaction='ignore')
    # writer.writeheader()
    writer.writerows(records)

