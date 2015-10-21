"""
Load the AU data outputted by the map-reduce job, and convert to CSV.

The script expects the following command-line args:
- the path to the map-reduce output file, which is the input to this script
- the dir path to contain the output CSVs.
"""

import sys
import csv
import ast
import os.path

import utils.mapred as mapred
import utils.dump_schema as schema
import output_utils as util
from collections import defaultdict

# Output CSV naming.
info_csv = 'info.csv'
app_csv = 'app.csv'
search_csv = 'search.csv'
dogfood_details_csv = 'dogfood_details.csv'
dogfood_appusage_csv = 'dogfood_appusage.csv'


def main(job_output, csv_dir):
    """Load map-reduce output and split records into tables.
    
    Count duplicates and write relevant subsets to CSVs.
    
    Data from the raw payloads are split into three groups that are each
    recorded in a separate table: 
    - top-level device/OS info ("info")
    - daily app usage ("app")
    - daily search counts ("search")
    These are recorded per payload, which are identified using (deviceID,
    start time, stop time).
    
    Next, the payloads from foxfood devices are joined by device, 
    and aggregate top-level info and daily app usage data are reported 
    separately.
    
    Some diagnostics are also reported around joining pings from the same
    device. Ideally the start-to-stop time periods should be sequential with
    negligible overlap, although this is not always the case.
    
    Currently fields in the key and value are referred to by positional index,
    which is quick but non-transparent and non-robust. The ordering for the 
    fields are determined by the 'au_{...}_{...}_keys' lists in 
    ../utils/dump_schema.py.
    """
    # Parse raw data
    # -------------- 
    
    output = mapred.parse_output_tuple(job_output)
    data = []
    for r in output['records']:
        # The MR value is a list or tuple stored as its string representation.
        vals = ast.literal_eval(r.pop())
        # Convert the MR value to a list and join it to the key, a tuple.
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
    
    # Dump flattened raw data and counts
    # ----------------------------------
    
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
    print('Wrote app CSV: %s rows' % len(tables['app']))
    with open(os.path.join(csv_dir, search_csv), 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(schema.au_search_csv)
        for r in tables['search']:
            util.write_unicode_row(writer, r)
    print('Wrote search CSV: %s rows' % len(tables['search']))
    
    # Group pings by device, and report on conditions such as bad overlap.
    # --------------------------------------------------------------------
    
    # Summarize/aggregate data and write tables.
    # First check ping submissions for overlap.
    # Map device ID to its associated pings identified by (start, stop) times.
    # Also set up a reference list of which devices are dogfooding participants.
    pings_by_device = defaultdict(list)
    is_dogfood_device = {}
    inconsistent_dogfooding_flag = 0
    for row in tables['info']:
        device_id = row[0]
        # If device has not yet been seen, record whether it is a foxfooder.
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
                condition_counts['clockskew'][device_id] += 1
            # Otherwise check times against last good ping, if any.
            elif pings_to_keep:
                last_stop = pings_to_keep[-1][1]
                if current_ping[0] < last_stop:
                    # There is some overlap.
                    if current_ping[1] <= last_stop:
                        # Current ping is contained in last ping.
                        # Remove and record occurrence.
                        should_keep = False
                        condition_counts['nested'][device_id] += 1
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
            # Update list of pings by device with new sanitized list.
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
    
    # Aggregate data for foxfood devices and report.
    # ----------------------------------------------
    
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
            # Drop deviceID from the beginning and dogfood flag from the end.
            dogfood_info[device_id].append(row[1:-1])
    for row in tables['app']:
        device_id = row[0]
        if (is_dogfood_device[device_id] and 
                    (row[1], row[2]) in pings_by_device[device_id]):
            # Drop deviceID from the beginning and dogfood flag from the end.
            dogfood_app[device_id].append(row[1:-1])
    
    # Summparize device info and app usage for each dogfooding device.
    dogfood_details = {}
    for device_id, payloads in dogfood_info.iteritems():
        device_details = {}
        # Sort by all fields, which sorts first by start then by stop times 
        # (ie chronologically), and then by values of other fields.
        payloads.sort()
        # Extract the actual device info fields, from 'os' to 
        # 'developer.menu.enabled' in au_device_info_keys
        get_device_info = lambda r: r[5:]
        # List of unique collections of device info fields with start
        # timestamp.
        deviceinfo = [(payloads[0][0], get_device_info(payloads[0]))]
        for i in range(1, len(payloads)):
            newinfo = get_device_info(payloads[i])
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
                # Add a new record.
                # Store values in a dict for convenient aggregation, and 
                # convert to strings at the end.
                app_data[app_key] = {
                    'counts': [0, 0, 0, 0, 0, 0],
                    # Maintain set of unique addon flag values seen for this app
                    # and date. Should be either empty or a single value.
                    'addon_flag': set(),
                    # Maintain a mapping of activity identifiers to counts.
                    'activities': defaultdict(lambda: 0)
                }
            for i in range(6):
                # Add in new numerical values.
                if p[4+i]:
                    app_data[app_key]['counts'][i] += p[4+i]
            if p[10] != '':
                app_data[app_key]['addon_flag'].add(p[10])
            if p[11]:
                # If we have activity counts, increment.
                current_activities = p[11].split(';')
                for curr_act in current_activities:
                    curr_act = curr_act.rsplit(':', 1)
                    app_data[app_key]['activities'][curr_act[0]] += (
                        int(curr_act[1]))
        # Convert app data values to strings.
        for app_key in app_data:
            app_data_values = [str(v) for v in app_data[app_key]['counts']]
            app_data_values.append(';'.join(
                [str(v) for v in sorted(app_data[app_key]['addon_flag'])]))
            app_data_values.append(';'.join(sorted(['%s:%s' % x
                for x in app_data[app_key]['activities'].iteritems()])))
            app_data[app_key] = app_data_values
        dogfood_appusage[device_id] = app_data
        # Add app usage dates summary to dogfood_details.
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
    print('Wrote dogfood details CSV: %s rows' % 
        sum(map(len, dogfood_appusage.values())))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(2)
    main(*sys.argv[1:3])
    sys.exit(0)

