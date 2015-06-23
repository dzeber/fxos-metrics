"""
Load the AU data outputted by the map-reduce job, and convert to CSV.

The script expects the following command-line args:
- the path to the map-reduce output file, which is the input to this script
- the dir path to contain the output CSVs.
"""

# import os.path
import sys
import csv
import ast
import os.path
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

info_csv = 'info.csv'
app_csv = 'app.csv'
search_csv = 'search.csv'
dogfood_details_csv = 'dogfood_details.csv'
dogfood_appusage_csv = 'dogfood_appusage.csv'


def main(job_output, csv_dir):
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
    if duplicate_counts:
        print('\nDuplicates:')
        for group, counts in duplicate_counts.items():
            print(('* %s payloads had duplicate submissions in the %s group; ' +
                '%s duplicate records were removed for these payloads.') %
                (counts['payloads'], group, counts['total'] - counts['payloads']))
    # Multiple records.
    if multiple_info:
        print('\nSome payloads had multiple unique info records:')
        for r in multiple_info:
            print(r)
    
    # Write output CSVs of flattened raw data.
    with open(os.path.join(csv_dir, info_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_info_csv)
        for r in tables['info']:
            util.write_unicode_row(writer, r)
    print('\nWrote info CSV: %s rows' % len(tables['info']))
    with open(os.path.join(csv_dir, app_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_app_csv)
        for r in tables['app']:
            util.write_unicode_row(writer, r)
    print('\Wrote app CSV: %s rows' % len(tables['app']))
    with open(os.path.join(csv_dir, search_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_search_csv)
        for r in tables['search']:
            util.write_unicode_row(writer, r)
    print('\Wrote search CSV: %s rows' % len(tables['search']))
    
    # Next, summarize/aggregate data and write tables.
    # First check ping submissions for overlap.
    # Map device ID to its associated pings.
    # Also set up a reference list of which devices are dogfooding participants.
    pings_by_device = defaultdict(list)
    is_dogfood_device = {}
    inconsistent_dogfooding_flag = 0
    for row in tables['info']:
        device_id = row[0]
        if device_id not in is_dogfood_device:
            is_dogfood_device[device_id] = row[-1]
        else:
            if is_dogfood_device[device_id] != row[-1]:
                # Shouldn't happen, but check anyway.
                inconsistent_dogfooding_flag += 1
        # Map deviceID to (start time, stop time).
        pings_by_device[device_id].append((row[1], row[2]))
    if inconsistent_dogfooding_flag:
        print('\nThere were inconsistent dogfooding flags')
    
    # Ping times should be sequential: 
    # previous stop time no later than current start time.
    # Any ping time ranges completely contained in another ping time range
    # can be removed as redundant.
    # Otherwise, we may have non-trivial overlapping time ranges
    # (a bug condition).
    # In this case, don't remove the ping, but make a note of occurrences.
    condition_counts = defaultdict(lambda: defaultdict(int))
    # final_pings = {}
    # Tolerance for overlap: 5 seconds.
    overlap_tolerance = 5000
    for device_id in pings_by_device:
        pings_to_keep = []
        all_pings = pings_by_device[device_id]
        # Sort by increasing start time and then by decreasing stop time.
        all_pings.sort()
        for current_ping in all_pings:
            should_keep = True
            # Remove pings with start time later than stop time. 
            # Means either a bug or system time was changed.
            if current_ping[0] > current_ping[1]:
                should_keep = False
                # pingstoremove[devid].append(currentping)
                condition_counts['clockskew'][device_id] += 1
            # Otherwise check times against last good ping, if any.
            elif pings_to_keep:
                last_stop = pings_to_keep[-1][1]
                if current_ping[0] < last_stop:
                    # There is some overlap.
                    if current_ping[1] <= last_stop:
                        # Current ping is contained in last ping.
                        # Remove and record occurrence.
                        # pingstoremove[devid].append(currentping)
                        should_keep = False
                        condition_counts['nested'][device_id] += 1
                        # continue
                    else:
                        overlap_ms = last_stop - current_ping[0]
                        if overlap_ms < overlap_tolerance:
                            # Overlap is negligible (within tolerance).
                            # Keep ping but record occurrence.
                            condition_counts['negligibleoverlap'][device_id] += 1
                        else:
                            # Otherwise, overlap is non-trivial.
                            # Keep curent ping but record occurrence.
                            condition_counts['overlap'][device_id] += 1
            if should_keep:
                pings_to_keep.append(current_ping)
        if pings_to_keep:
            pings_by_device[device_id] = pings_to_keep

    # Print statistics about overlaps.
    if condition_counts:
        print('\nOverlaps:')
        if 'clockskew' in condition_counts:
            count_map = condition_counts['clockskew']
            dogfooders = [device_id for device_id in count_map 
                            if is_dogfood_device[device_id]]
            addendum = (', including %s pings from %s dogfood devices:' %
                    (sum([count_map[devid] for devid in dogfooders]), 
                    len(dogfooders))
                if dogfooders else '.')
            print('* %s pings with clock skew were removed from %s devices%s' %
                (sum(count_map.values()), 
                len(count_map), 
                addendum))
            if dogfooders:
                for devid in dogfooders:
                    print('\t%s' % devid)
        if 'nested' in condition_counts:
            count_map = condition_counts['nested']
            dogfooders = [device_id for device_id in count_map 
                            if is_dogfood_device[device_id]]
            addendum = (', including %s pings from %s dogfood devices:' %
                    (sum([count_map[devid] for devid in dogfooders]), 
                    len(dogfooders))
                if dogfooders else '.')
            print('* %s nested pings were removed from %s devices%s' %
                (sum(count_map.values()), 
                len(count_map), 
                addendum))
            if dogfooders:
                for devid in dogfooders:
                    print('\t%s' % devid)
        if 'overlap' in condition_counts:
            count_map = condition_counts['overlap']
            dogfooders = [device_id for device_id in count_map 
                            if is_dogfood_device[device_id]]
            addendum = (', including %s pings from %s dogfood devices:' %
                    (sum([count_map[devid] for devid in dogfooders]), 
                    len(dogfooders))
                if dogfooders else '.')
            print(('* %s pings from %s devices had non-negligible overlap' +
                ' with the previous ping (but were not removed)%s') %
                (sum(count_map.values()), 
                len(count_map), 
                addendum))
            if dogfooders:
                for devid in dogfooders:
                    print('\t%s' % devid)
        if 'negligibleoverlap' in condition_counts:
            count_map = condition_counts['negligibleoverlap']
            dogfooders = [device_id for device_id in count_map 
                            if is_dogfood_device[device_id]]
            addendum = (', including %s pings from %s dogfood devices:' %
                    (sum([count_map[devid] for devid in dogfooders]), 
                    len(dogfooders))
                if dogfooders else '.')
            print(('* %s pings from %s devices had negligible overlap' +
                ' with the previous ping (but were not removed)%s') %
                (sum(count_map.values()), 
                len(count_map), 
                addendum))

    # Create a list of device info for dogfooding devices.
    dogfood_info = defaultdict(list)
    dogfood_app = defaultdict(list)
    for row in tables['info']:
        device_id = row[0]
        if (is_dogfood_device[device_id] and 
                    (row[1], row[2]) in pings_by_device[device_id]):
            # These can actually be added directly to pings_to_keep above,
            # skipping this step.
            # Instead, go through keys of pings_by_device, keeping those
            # which have dogfooding.
            # Not for app rows though - have to do this step for those.
            dogfood_info[device_id].append(row[1:-1])
    for row in tables['app']:
        device_id = row[0]
        if (is_dogfood_device[device_id] and 
                    (row[1], row[2]) in pings_by_device[device_id]):
            dogfood_app[device_id].append(row[1:-1])
    
    # Summparize device info and app usage for each dogfooding device.
    dogfood_details = {}
    for device_id, payloads in dogfood_info.iteritems():
        device_details = {}
        payloads.sort()
        get_device_info = lambda r: r[5:]
        # List of unique collections of device info fields with earliest
        # timestamp.
        deviceinfo = [(payloads[0][0], get_device_info(payloads[0]))]
        for i in range(1, len(payloads)):
            newinfo = get_device_info(payloads[1])
            if newinfo != deviceinfo[-1][1]:
                deviceinfo.append((payloads[i][0], newinfo))
        # Store full latest device info.
        device_details['info'] = deviceinfo[-1][1]
        # Earliest and latest measurement ranges.
        device_details['earliest_start'] = payloads[0][0]
        device_details['latest_stop'] = payloads[-1][1]
        # Earliest and latest ping submission dates.
        submission_dates = [p[2] for p in payloads]
        device_details['earliest_submission'] = min(submission_dates)
        device_details['latest_submission'] = max(submission_dates)
        device_details['num_pings'] = len(payloads)
        device_details['changed_info'] = len(deviceinfo) > 1
        device_details['earliest_appusage'] = ''
        device_details['latest_appusage'] = ''
        dogfood_details[device_id] = device_details
    
    dogfood_appusage = {}
    for device_id, payloads in dogfood_app.iteritems():
        app_data = {}
        for p in payloads:
            # App rows are identified by app URL and usage date.
            app_key = (p[2], p[3])
            if app_key not in app_data:
                app_data[app_key] = p[4:]
            else:
                # Aggregate across values already present.
                for i in range(4):
                    app_data[app_key][i] += p[4+i]
                # Join activities counts represented as strings.
                if p[-1]:
                    app_data[app_key][-1] = (p[-1] if not app_data[app_key][-1]
                        else ';'.join([app_data[app_key][-1], p[-1]]))
        dogfood_appusage[device_id] = app_data
        usage_dates = [k[1] for k in app_data]
        dogfood_details[device_id]['earliest_appusage'] = min(usage_dates)
        dogfood_details[device_id]['latest_appusage'] = max(usage_dates)
    
    with open(os.path.join(csv_dir, dogfood_details_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_dogfood_details_csv)
        for device_id, vals in dogfood_details.iteritems():
            row = [device_id]
            row += [vals[k] for k in schema.au_dogfood_details_csv[1:9]]
            row += vals['info']
            util.write_unicode_row(writer, row)
    print('\nWrote dogfood details CSV: %s rows' % len(dogfood_details))
    with open(os.path.join(csv_dir, dogfood_appusage_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_dogfood_appusage_csv)
        for device_id, app_rows in dogfood_appusage.iteritems():
            for app_key, vals in app_rows.iteritems():
                row = [device_id] + list(app_key) + vals
                util.write_unicode_row(writer, row)
    print('\nWrote dogfood details CSV: %s rows' % 
        sum(map(len, dogfood_appusage.values())))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(2)
    main(*sys.argv[1:2])
    sys.exit(0)

