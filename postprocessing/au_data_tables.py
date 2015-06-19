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
# from datetime import date, timedelta

import utils.mapred as mapred
# import utils.ftu_formatter as ftu
import utils.dump_schema as schema
import output_utils as util

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
    """Load map-reduce output, and write relevant subsets to CSVs.
    """
    data = mapred.parse_output_tuple(job_output)
    
    # Split records into tables for info, app activity, and search.
    # Also make a count of duplicate payloads for diagnostic purposes.
    tables = {}
    # A mapping of payload ID to occurrence counts for each type.
    payload_counts = {}
    for r in data['records']:
        type = r.pop(0)
        
        # Keep track of counts.
        n = r.pop()
        count_key = tuple(r[:(len(schema.au_ping_identifier_keys) + 1)])
        if count_key not in payload_counts:
            payload_counts[count_key] = {}
        if type not in payload_counts[count_key]:
            payload_counts[count_key][type] = {}
        counts = payload_counts[count_key][type]
        # Count number of unique records/table rows for this payload ID.
        counts['rows'] = counts.get('rows', 0) + 1
        # Count number of total records for this payload ID including
        # duplicate payloads.
        counts['total'] = counts.get('total', 0) + n
        
        # Store row.
        if type not in tables:
            tables[type] = []
        tables[type].append(r)
    
    with open(info_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_info_csv)
        for r in tables['info']:
            util.write_unicode_row(writer, r)
    with open(app_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_app_csv)
        for r in tables['app']:
            util.write_unicode_row(writer, r)
    with open(search_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_search_csv)
        for r in tables['search']:
            util.write_unicode_row(writer, r)
    
    # print('Wrote dump CSV: %s rows\n' % len(dump_rows))
    
    # Output counters and diagnostics.
    print('Counters:')
    util.print_counter_info(data['counters'])
    print('\nError conditions:')
    util.print_condition_info(data['conditions'])
    # Check for duplicates and print info.
    print('\nDuplicates:')
    print(('\n* %s unique payload IDs (device ID, start timestamp, ' + 
        'stop timestamp)') % len(payload_counts))
    dupes = {'repeatedapporsearch': 0, 'differentinfo': 0, 'duplicateinfo': 0}
    for v in payload_counts.itervalues():
        if v['info']['rows'] > 1:
            dupes['differentinfo'] += 1
        elif v['info']['total'] > 1:
            dupes['duplicateinfo'] += 1
        elif (('app' in v and v['app']['total'] > v['app']['rows']) or 
                ('search' in v and v['search']['total'] > v['search']['rows'])):
            dupes['repeatedapporsearch'] += 1
    print('\n* %s payload IDs with multiple info records' % 
        dupes['differentinfo'])
    print('\n* %s payload IDs with repeated info records' % 
        dupes['duplicateinfo'])
    print('\n* %s payload IDs with repeated app or search records' % 
        dupes['repeatedapporsearch'])


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(2)
    main(*sys.argv[1:5])
    sys.exit(0)

