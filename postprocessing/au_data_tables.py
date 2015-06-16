"""
Load the AU data outputted by the map-reduce job, and convert to CSV.

The script expects the following command-line args:
- the path to the map-reduce output file, which is the input to this script
- the path to the output CSV containing top-level device info
- the path to the output CSV containing dates of activity
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


def main(job_output, ping_info_csv, active_dates_csv):
    """Load map-reduce output, and write relevant subsets to CSVs.
    """
    data = mapred.parse_output_tuple(job_output)
    
    # Split info records from the active dates records.
    # Join info records by (deviceID,dogfood).
    # info = {}
    info = []
    activity = []
    for r in data['records']:
        type = r.pop(0)
        if type == 'info':
            # r.pop(1)
            info.append(r)
            # Map deviceID to device info.
            # infokey = (r[0], r[2])
            # if infokey not in info:
                # info[infokey] = []
            # info[infokey].append(r[3:])
        else:
            # For active dates, record (deviceID, dogfood, date).
            activity.append([r[0], r[2], r[3]])
    
    
    # # Accumulate subsets of data to be converted to CSV.
    # # Dump rows will be stored as a list of row lists. 
    # dump_rows = []
    # # Dashboard rows will be stored as a mapping of value tuples to a count.
    # dash_rows = {}
    # for r in data['records']:
        # record_date = r[field_index['submissionDate']]
        # if record_date == '':
            # continue
        # if record_date > latest_date or record_date < earliest_date:
            # continue
        # # Add to dashboard data. 
        # accumulate_dashboard_row(dash_rows, r)
        # # Add to dump CSV if required. 
        # if record_date >= earliest_for_dump:
            # dump_rows.append(r)
    
    # # Write to output files.
    # headers = schema.dashboard_csv_headers
    # with open(dashboard_csv, 'w') as outfile:
        # writer = csv.writer(outfile)
        # writer.writerow(headers)
        # for r in dash_rows:
            # next_row = list(r)
            # next_row.append(dash_rows[r])
            # write_unicode_row(writer, next_row)
    
    # print('Wrote dashboard CSV: %s rows\n' % len(dash_rows))
    
    # headers = schema.dump_csv_headers
    with open(ping_info_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_info_csv)
        for r in info:
            util.write_unicode_row(writer, r)
    
    with open(active_dates_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_activity_csv)
        for r in activity:
            util.write_unicode_row(writer, r)
    
    # print('Wrote dump CSV: %s rows\n' % len(dump_rows))
    
    # Output counters and diagnostics.
    print('Counters:')
    util.print_counter_info(data['counters'])
    print('\nError conditions:')
    util.print_condition_info(data['conditions'])


if __name__ == "__main__":
    job_output = sys.argv[1]
    ping_info_csv = sys.argv[2]
    active_dates_csv = sys.argv[3]
    main(job_output, ping_info_csv, active_dates_csv)

