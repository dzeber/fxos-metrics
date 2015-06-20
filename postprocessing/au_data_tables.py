"""
Load the AU data outputted by the map-reduce job, and convert to CSV.

The script expects the following command-line args:
- the path to the map-reduce output file, which is the input to this script
- the path to the output CSV to contain top-level device info
- the path to the output CSV to contain app usage data
- the path to the output CSV to contain search counts
"""

# import os.path
import sys
import csv
import ast
# from datetime import date, timedelta

import utils.mapred as mapred
# import utils.ftu_formatter as ftu
import utils.dump_schema as schema
import output_utils as util
from collections import defaultdict

# # Each datum will now be a tuple whose order is determined by 
# # schema.final_keys, rather than a dict.
# # Create a mapping of field names to indices for clear reference. 
# field_index = dict(
    # zip(schema.final_keys, range(0, len(schema.final_keys) - 1)))

# # The number of days before today the dashboard dataset should cover.
# dashboard_range = 180
# # The number of days before today the dump dataset should cover.
# dump_range = 90

# # Cutoff dates for inclusion in datasets.
# # No later than yesterday. 
# latest_date = (date.today() - timedelta(days = 1)).isoformat()
# # No earlier than 180 days ago. 
# earliest_date = (date.today() - timedelta(days = dashboard_range)).isoformat()
# # Cutoff date for inclusion in dump csv is 3 months before today.
# earliest_for_dump = (date.today() - timedelta(days = dump_range)).isoformat()


# def accumulate_dashboard_row(dataset, raw_row):
    # """Convert a raw datum to a row for the dashboard CSV, and add to dataset.
    
    # The relevant values for the dashboard CSV are extracted from raw_row and 
    # summarized if necessary. The reduced data row is then added to dataset,
    # a dict mapping rows to occurrence counts, and the count is updated if
    # necessary.     
    # """
    # new_row = []
    # # Extract relevant fields, and check them against lookup tables.
    # # See dump_schema.py for list indices.
    # new_row.append(raw_row[field_index['submissionDate']])
    # new_row.append(ftu.summarize_os(raw_row[field_index['os']]))
    # new_row.append(ftu.summarize_country(raw_row[field_index['country']]))
    # new_row.append(ftu.summarize_device(raw_row[field_index['product_model']]))
    # new_row.append(ftu.summarize_operator(
        # raw_row[field_index['icc.network']], 
        # raw_row[field_index['icc.name']], 
        # raw_row[field_index['network.network']], 
        # raw_row[field_index['network.name']]
    # ))
    # new_row = tuple(new_row)
    # # Add occurrence count from the original data.
    # count = raw_row[-1]
    # # Add new row to dashboard dataset, accumulating counts if necessary.
    # if new_row not in dataset:
        # dataset[new_row] = count
    # else:
        # dataset[new_row] = dataset[new_row] + count


def main(job_output, info_csv, app_csv, search_csv):
    """Load map-reduce output and split records into tables.
    
    Count duplicates and write relevant subsets to CSVs.
    """
    output = mapred.parse_output_tuple(job_output)
    data = []
    for r in output['records']:
        vals = ast.literal_eval(r.pop())
        data.append(r + list(vals))
    
    # Split records into tables for info, app activity, and search.
    # Also keep count of duplicate records and cases with multiple records.
    tables = defaultdict(list)
    # Maintain separate counts for dogfooders and others.
    duplicate_counts = defaultdict(lambda: defaultdict(int))
    multiple_info = []
    for d in data:
        type = d.pop()
        if type == 'info':
            # First check for the multiple rows tag.
            if d[0].startswith('multiple:'):
                # In this case, save these records separately.
                multiple_info.append(d)
                continue
            n = d.pop()
            if n > 1:
                # Make a note of any duplicates.
                dupes = duplicate_counts['dogfood' if d[-1] else 'general']
                dupes['payloads'] += 1
                dupes['total'] += n
        tables[type].append(d)
    
    # Print some statistics about the job and the dataset.
    print('Counters:')
    util.print_counter_info(output['counters'])
    print('\nError conditions:')
    util.print_condition_info(output['conditions'])
    # Duplicates.
    if len(duplicate_counts) > 0:
        print('\nDuplicates:')
        for group, counts in duplicate_counts.items():
            print(('* %s payloads had duplicate submissions in the %s group; ' +
                '%s duplicate records were removed for these payloads.') %
                (counts['payloads'], group, counts['total'] - counts['payloads']))
    # Multiple records.
    if len(multiple_info) > 0:
        print('\nSome payloads had multiple unique info records:')
        for r in multiple_info:
            print(r)
    
    # Write output CSVs.
    with open(info_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_info_csv)
        for r in tables['info']:
            util.write_unicode_row(writer, r)
    print('\nWrote info CSV: %s rows' % len(tables['info']))
    with open(app_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_app_csv)
        for r in tables['app']:
            util.write_unicode_row(writer, r)
    print('\Wrote app CSV: %s rows' % len(tables['app']))
    with open(search_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_search_csv)
        for r in tables['search']:
            util.write_unicode_row(writer, r)
    print('\Wrote search CSV: %s rows' % len(tables['search']))


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(2)
    main(*sys.argv[1:5])
    sys.exit(0)

