# Convert the ouptput of the dump job to a CSV.

# Arguments supplied should be the input data file and the output CSV file. 

import csv
import sys
# from ..shared import mapred
# from shared import mapred
import mapred
import dumpschema as schema

job_output = sys.argv[1]
csv_file = sys.argv[2]

# Parse in output file.
data = mapred.parse_output_tuple(job_output)

# Output records to CSV.
headers = schema.csv_headers
headers.append('count')

with open(csv_file, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(headers)
    writer.writerows(data['records'])

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

