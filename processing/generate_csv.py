# Convert the ouptput of the count_activations job to a CSV 
# suitable for loading into dashboard. 

# Arguments supplied should be the input data file and the output CSV file. 

import json
import csv
import sys

job_output = sys.argv[1]
csv_file = sys.argv[2]
# The keys to write to the CSV output. 
headers = ['pingdate', 'os', 'country', 'device', 'operator', 'count']
# The keys that should not be encoded.
not_encoded = ['count', 'pingdate']
# The headings to use in the CSV, as expected by the display page script, 
# corresponding to the keys in header by order.
final_headers = ['date', 'os', 'country', 'device', 'operator', 'activations']

# The complete set of parsed records. 
# Each record is represented as a dict. 
# In particular, there will be counts for every combination of fields 
# with the value all.
records = []
# Error conditions encountered during the job. 
conditions = {}
# Counters recorded by the job. 
counters = {}

# Parse output file.

for row in open(job_output):
    parsed_row = row.rstrip().rsplit('\t', 1)
    vals = json.loads(parsed_row[0])
    n = int(parsed_row[1])
    if 'condition' in vals:
        # Store mapping of condition counts. 
        conditions[vals['condition']] = n
    elif 'counter' in vals:
        # Store list of counters. 
        if 'group' in vals:
            # If the counter has a container group, 
            # add the counter in a subdict keyed by group name. 
            group_name = vals['group']
            if group_name not in counters:
                counters[group_name] = {}
            grouplist = counters[group_name]
            grouplist[vals['counter']] = n
        else:
            counters[vals['counter']] = n    
    else:
        vals['count'] = n
        records.append(vals)

            
# TODO: Find unique values of fields. 


# Write CSV for main activations plot.

# Custom row writing function to allow subsetting and additional formatting. 
# Accepts the writer object and elements of records list.
def write_row(csv_writer, row):
    if row['pingdate'] != 'All' and row['operator'] == 'All':
        # Reorder.
        row = [(row[key] if key in row else None) for key in headers]
        # Encode explicitly to try to avoid errors.
        row = [unicode(r).encode('utf-8', 'replace') for r in row]
        csv_writer.writerow(row)

with open(csv_file, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(final_headers)
    for row in records:
        write_row(writer, row)
         

