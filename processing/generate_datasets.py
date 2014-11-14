# Extract datasets from the data source (output of the dump job).
# This script will generate a CSV to power the daily activation dashboard,
# as well as a CSV of recent dump data.

# Arguments supplied should be the input data file,
# the dashboard CSV to output to, and the dump CSV to output to,
# in that order.

import os.path
import sys
import csv
from datetime import date, timedelta

import mapred
import ftu_formatter as ftu
import dump_schema as schema

job_output = sys.argv[1]
dashboard_csv = sys.argv[2]
dump_csv = sys.argv[3]


# Each datum will now be a tuple whose order is determined by 
# schema.final_keys, rather than a dict.
# Create a mapping of field names to indices for clear reference. 
field_index = dict(
    zip(schema.final_keys, range(0, len(schema.final_keys) - 1)))


# Cutoff dates for inclusion in datasets.
# No later than yesterday. 
latest_date = (date.today() - timedelta(days = 1)).isoformat()
# No earlier than 180 days ago. 
earliest_date = (date.today() - timedelta(days = 180)).isoformat()
# Cutoff date for inclusion in dump csv is 3 months before today.
earliest_for_dump = (date.today() - timedelta(days = 90)).isoformat()


# Encode explicitly to try to avoid errors.
def encode_for_csv(val):
    return unicode(val).encode('utf-8', 'replace')


# Convert raw row to datum for inclusion in dashboard dataset. 
# Accumulate counts.
def accumulate_dashboard_row(dataset, raw_row):
    new_row = []
    # Extract relevant fields, and check them against lookup tables.
    # See dump_schema.py for list indices.
    # Add date first. 
    new_row.append(raw_row[field_index['submissionDate']])
    # OS
    new_row.append(encode_for_csv(
        ftu.summarize_os(raw_row[field_index['os']])))
    # Country name.
    new_row.append(encode_for_csv(
        ftu.summarize_country(raw_row[field_index['country']])))
    # Device name.
    new_row.append(encode_for_csv(
        ftu.summarize_device(raw_row[field_index['product_model']])))
    # Operator name. 
    new_row.append(encode_for_csv(
        ftu.summarize_operator(
            raw_row[field_index['icc.network']], 
            raw_row[field_index['icc.name']], 
            raw_row[field_index['network.network']], 
            raw_row[field_index['network.name']])))
    
    new_row = tuple(new_row)
    count = raw_row[-1]
    
    if new_row not in dataset:
        dataset[new_row] = count
    else:
        dataset[new_row] = dataset[new_row] + count


#-----------------------------

# Parse in output file.
data = mapred.parse_output_tuple(job_output)

# Cutoff date for inclusion in dump csv is 3 months before today.
# dump_cutoff_date = date.today() - timedelta(days = 90)
# dump_cutoff_date = dump_cutoff_date.isoformat()

# Accumulate subsets of data to be converted to CSV.
# Dump rows will be stored as a list of row lists. 
dump_rows = []
# Dashboard rows will be stored as a mapping of value tuples to a count.
dash_rows = {}

for r in data['records']:
    record_date = r[field_index['submissionDate']]
    if record_date == '':
        continue
    if record_date > latest_date or record_date < earliest_date:
        continue
    
    # Apply formatting to be done outside MR job. 
    r = ftu.apply_post_formatting(r)
    
    # Add to dashboard data. 
    accumulate_dashboard_row(dash_rows, r)
    # Add to dump CSV if required. 
    if record_date >= earliest_for_dump:
        dump_rows.append(r)


# Write to output files.
headers = schema.dashboard_csv_headers
with open(dashboard_csv, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(headers)
    for r in dash_rows:
        next_row = list(r)
        next_row.append(dash_rows[r])
        writer.writerow(next_row)
        
print('Wrote dashboard CSV: %s rows\n' % len(dash_rows))

headers = schema.dump_csv_headers
with open(dump_csv, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(headers)
    writer.writerows(dump_rows)
    
print('Wrote dump CSV: %s rows\n' % len(dump_rows))

# Output counters and diagnostics.
print('Counters:')
for name in data['counters']:
    counter = data['counters'][name]
    # If this is a group, print all subcounters.
    if type(counter) is dict:
        for cname in counter:
            print(name + ' | ' + cname + ' :  ' + str(counter[cname]))
    else:
        print(name + ' :  ' + str(counter))

print('\nError conditions:')
for name in data['conditions']:
    print(name + ' :  ' + str(data['conditions'][name]))

