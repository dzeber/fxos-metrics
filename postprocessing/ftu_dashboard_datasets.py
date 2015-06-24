"""
Load the data outputted by the map-reduce job, and store as CSVs to be passed 
to the dashboards.

The rows of data are loaded from the map-reduce output. Rows submitted within 
the last three months are written as-is to the dump CSV, and rows submitted
within the last six months are summarized and written to the CSV powering the
dashboard.

The dashboard dataset only retains a small subset of the original columns, and
values that are not considered relevant (eg. countries that have not had a 
launch, or non-standard devices) are replaced by 'Other'. The reduced dataset
then gets reaggregated and written to CSV. 

The script expects the following command-line args:
- the path to the map-reduce output file, which is the input to this script
- the path to the dashboard CSV to be generated
- the path to the dump CSV to be generated.
"""

import os.path
import sys
import csv
from datetime import date, timedelta

import utils.mapred as mapred
import utils.ftu_formatter as ftu
import utils.dump_schema as schema
import output_utils as util

# Each datum will now be a tuple whose order is determined by 
# schema.final_keys, rather than a dict.
# Create a mapping of field names to indices for clear reference. 
field_index = dict(
    zip(schema.final_keys, range(0, len(schema.final_keys) - 1)))

# The number of days before today the dashboard dataset should cover.
dashboard_range = 180
# The number of days before today the dump dataset should cover.
dump_range = 180

# Cutoff dates for inclusion in datasets.
# No later than yesterday. 
latest_date = (date.today() - timedelta(days = 1)).isoformat()
# No earlier than 180 days ago. 
earliest_date = (date.today() - timedelta(days = dashboard_range)).isoformat()
# Cutoff date for inclusion in dump csv is 3 months before today.
earliest_for_dump = (date.today() - timedelta(days = dump_range)).isoformat()


def accumulate_dashboard_row(dataset, raw_row):
    """Convert a raw datum to a row for the dashboard CSV, and add to dataset.
    
    The relevant values for the dashboard CSV are extracted from raw_row and 
    summarized if necessary. The reduced data row is then added to dataset,
    a dict mapping rows to occurrence counts, and the count is updated if
    necessary.     
    """
    new_row = []
    # Extract relevant fields, and check them against lookup tables.
    # See dump_schema.py for list indices.
    new_row.append(raw_row[field_index['submissionDate']])
    new_row.append(ftu.summarize_os(raw_row[field_index['os']]))
    new_row.append(ftu.summarize_country(raw_row[field_index['country']]))
    new_row.append(ftu.summarize_device(raw_row[field_index['product_model']]))
    new_row.append(ftu.summarize_operator(
        raw_row[field_index['icc.network']], 
        raw_row[field_index['icc.name']], 
        raw_row[field_index['network.network']], 
        raw_row[field_index['network.name']]
    ))
    new_row = tuple(new_row)
    # Add occurrence count from the original data.
    count = raw_row[-1]
    # Add new row to dashboard dataset, accumulating counts if necessary.
    if new_row not in dataset:
        dataset[new_row] = count
    else:
        dataset[new_row] = dataset[new_row] + count


def main(job_output, dashboard_csv, dump_csv):
    """Load map-reduce output, and write relevant subsets to CSVs.
    
    The dump CSV includes full rows from the job output, but limited to a 
    specific submission date range. The dashboard CSV is limited by submission 
    date, but also restricted to certain columns.
    
    Input args are the path to the file containing the map-reduce job output,
    stored using the tuple-based formatting defined in utils/mapred.py, and 
    paths to the dashboard CSV and dump CSV to be written. 
    """
    data = mapred.parse_output_tuple(job_output)
    
    # Accumulate subsets of data to be converted to CSV.
    # Dump rows will be stored as a list of row lists. 
    dump_rows = []
    # Dashboard rows will be stored as a mapping of value tuples to a count.
    dash_rows = {}
    for r in data['records']:
        # Make sure the count is numeric.
        r[-1] = int(r[-1])
        record_date = r[field_index['submissionDate']]
        if record_date == '':
            continue
        if record_date > latest_date or record_date < earliest_date:
            continue
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
            util.write_unicode_row(writer, next_row)
    
    print('Wrote dashboard CSV: %s rows\n' % len(dash_rows))
    
    headers = schema.dump_csv_headers
    with open(dump_csv, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)
        for r in dump_rows:
            util.write_unicode_row(writer, r)
    
    print('Wrote dump CSV: %s rows\n' % len(dump_rows))
    
    # Output counters and diagnostics.
    print('Counters:')
    util.print_counter_info(data['counters'])
    print('\nError conditions:')
    util.print_condition_info(data['conditions'])


if __name__ == "__main__":
    job_output = sys.argv[1]
    dashboard_csv = sys.argv[2]
    dump_csv = sys.argv[3]
    main(job_output, dashboard_csv, dump_csv)

