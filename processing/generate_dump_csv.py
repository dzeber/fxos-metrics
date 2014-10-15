# Convert the ouptput of the dump job to a CSV.

# Arguments supplied should be the input data file and the output CSV file. 

import os.path
import sys
import csv

# Hack to be able to import from required directory. 
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'shared'))

import mapred
import dump_schema as schema
from datetime import date, timedelta

job_output = sys.argv[1]
csv_file = sys.argv[2]

# Parse in output file.
data = mapred.parse_output_tuple(job_output)

# Cutoff date is within 3 months of today.
cutoff_date = date.today() - timedelta(days = 90)
cutoff_date = cutoff_date.isoformat()
data_records = [ r for r in data['records'] if r[1] >= cutoff_date ]

# Output records to CSV.
headers = schema.csv_headers
headers.append('count')

with open(csv_file, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(headers)
    writer.writerows(data_records)

print('CSV written.')

# Output meta info to stdout. 
print('Counters:')
for name in data['counters']:
    counter = data['counters'][name]
    # If this is a group, print all subcounters.
    if type(counter) is dict:
        for cname in counter:
            print(name + ' | ' + cname + ' :  ' + str(counter[cname]))
    else:
        print(name + ' :  ' + str(counter))

print('Error conditions:')
for name in data['conditions']:
    print(name + ' :  ' + str(data['conditions'][name]))

