"""
Map-reduce job to download recent FxOS AU records, sanitize and reformat field
values, and count occurrences of unique combinations.

For each record, the mapper parses the record and stores relevant values in a
dict, after reformatting and sanity-checking. The reducer then counts 
occurrences of unique sets of field/value combinations.

The output from the job can be thought of as rows, each row representing
a unique segment defined by its field values, together with a column of counts
indicating how many records were found belonging to the segment.
"""

import json
from datetime import datetime
import re

import utils.ftu_formatter as fmt
import utils.mapred as mapred
import utils.dump_schema as schema
import utils.payload_utils as payload


def consistent_au(r):
    """Simple sanity check.
    
    Check that the payloads have correct 'reason' ('appusage') and 'appName' 
    ('FirefoxOS').
    """
    return r.get('appName') == 'FirefoxOS' and r.get('reason') == 'appusage'

def is_dogfood_device(r):
    """Determine whether or not the payload comes from a dogfooding device.
    
    AU pings from dogfooding devices are those whose deviceID is a 15-digit
    IMEI number rather than the usual UUID.
    """
    return ('deviceID' in r and 
                re.match('^[0-9]{15}$', r['deviceID']) is not None)

def map(key, dims, value, context):
    """Parse the raw JSON payloads, reformat values, and output.
    
    After parsing, the field values in the payload are flattened and 
    stored as a dict. Various formatting is applied to the values, and short
    codes are converted to readable string values. The dict is then represented
    as an ordered tuple using the functions in utils/mapred.py, and passed
    to the reducer for counting.
    """    
    mapred.increment_counter_tuple(context, 'nrecords')
    try:
        r = json.loads(value)
        # Check basic consistency. 
        if 'info' not in r:
            # All the data is inside the info object.
            mapred.write_condition_tuple(context, 'noinfo')
            return
        r = r['info']
        if not consistent_au(r):
            mapred.write_condition_tuple(context, 'inconsistent')
            return
        # Make sure payloads have necessary identifiers - device ID and 
        # recording start time.
        for k in 'deviceID', 'start':
            if k not in r:
                mapred.write_condition_tuple(context, 'missing' + k)
                return
        
        #-----
        
        # Rearrange.
        
        # Separate apps and searches from other fields.
        apps = r.pop('apps', {})
        searches = r.pop('searches', {})
        
        # Remove telemetry-server fields.
        for k in ('reason', 'appName', 'appVersion', 'appUpdateChannel',
                    'appBuildID'):
            if k in r:
                del r[k]
        
        # Flatten deviceinfo subdict, stripping 'deviceinfo' prefix.
        if 'deviceinfo' in r:
            di = r.pop('deviceinfo')
            for k in di:
                newk = k[11:] if k.startswith('deviceinfo.') else k
                r[newk] = di[k]
        
        # Flatten screen info subdict.
        if 'screen' in r:
            s = r.pop('screen')
            for k in s:
                newk = 'screenWidth' if k == 'width' else (
                    'screenHeight' if k == 'height' else k)
                r[newk] = s[k]
        
        # Flatten network info.
        if 'simInfo' in r:
            si = r.pop('simInfo')
            for nw in 'icc', 'network':
                nwvals = si.get(nw)
                if nwvals is not None:
                    for k in nwvals:
                        r[nw + '.' + k] = si[nw][k]            
        
        # There should not be more than 1 level.
        for (k,v) in r.iteritems():
            if isinstance(v, dict):
                mapred.write_condition_tuple(context, 'multiple nesting')
                return
        
        #-----
        
        # Format individual entries. 
        
        # Remove any null entries.
        nullval_keys = [k for (k,v) in r.iteritems() if v is None]
        for k in nullval_keys:
            del r[k]
        
        # Convert dates.
        if 'start' in r:
            r['startDate'] = (
                fmt.ms_timestamp_to_date(r['start']).isoformat())
        if 'stop' in r:
            r['stopDate'] = (
                fmt.ms_timestamp_to_date(r['stop']).isoformat())
        sdate = payload.get_submission_date(dims)
        if sdate is not None:
            r['submissionDate'] = (
                datetime.strptime(sdate, '%Y%m%d').date().isoformat())
        
        # Merge update channel fields. 
        # If both are present, note occurrence but don't replace.
        if 'app.update.channel' in r:
            if 'update_channel' in r:
                # Both values are present.
                if r['app.update.channel'] != r['update_channel']:
                    # Note the disparity.
                    mapred.write_condition_tuple(context, 
                        'multiple channels: ' + 
                        'update_channel = ' + r['update_channel'] +
                        ', app.update.channel = ' + r['app.update.channel'])
            else:
                # Only 'app.update.channel' is present.
                # Save the value as 'update_channel'.
                r['update_channel'] = r['app.update.channel']
            # Keep 'update_channel'.
            del r['app.update.channel']
        
        # Add simplified form of update channel - 
        # more useful for separating them.
        if 'update_channel' in r:
            r['update_channel_standardized'] = fmt.get_standard_channel(
                r['update_channel'])
        
        # Apply substitutions:
        if 'os' in r:
            r['os'] = fmt.format_os_string(r['os'])
        
        if 'product_model' in r:
            r['product_model'] = fmt.format_device_string(r['product_model'])
        
        if 'geoCountry' in r:
            country_name = fmt.lookup_country_code(r['geoCountry'])
            # If lookup fails, keep original geo code.
            r['country'] = (country_name if country_name is not None else
                r['geoCountry'])
        
        # Convert locale code to language name.
        # Keep original locale code for reference.
        if 'locale' in r:
            r['language'] = fmt.lookup_language(r['locale'])
        
        # Keep original network codes, but try looking them up.
        for prefix in 'icc','network':
            mcc_key = prefix + '.mcc'
            mnc_key = prefix + '.mnc'
            if mcc_key in r:
                # Look up country code.
                r[prefix + '.country'] = fmt.lookup_mcc(r[mcc_key])
                if mnc_key in r:
                    # Look up network code, and format string.
                    nw = fmt.lookup_mnc(r[mcc_key], r[mnc_key])
                    if nw is not None:
                        nw = fmt.format_operator_string(nw)
                    r[prefix + '.network'] = nw
        
        # Format network name strings. 
        if 'icc.spn' in r:
            r['icc.name'] = fmt.format_operator_string(r['icc.spn'])
        if 'network.operator' in r:
            r['network.name'] = fmt.format_operator_string(r['network.operator'])
        
        # Opportunity for general formatting rules 
        # based on combinations of values. 
        # In particular, setting OS to '1.3T' for Tarako devices.
        r = fmt.apply_general_formatting(r)
        
        # Is this a dogfooding device?
        r['dogfood'] = is_dogfood_device(r)
        
        #-----
        
        # Find the set of dates on which the device was active.
        # First find the set of unique dates on which apps were used.
        unique_dates = set()
        for a in apps:
            for d in apps[a]:
                unique_dates.add(d)
        # Format the dates.
        active_dates = list()
        for d in unique_dates:
            try:
                d = datetime.strptime(d, '%Y%m%d').date().isoformat()
                active_dates.append(d)
            except ValueError:
                continue
        
        #----
        
        # Identifier values for this payload.
        payload_id = mapred.dict_to_ordered_list(r, 
                                            schema.au_ping_identifier_keys)
        
        # Output one row per payload with top-level info.
        # Start with the record type identifier.
        info = ['info']
        # Add the payload identifier.
        info.extend(payload_id)
        # Add the date fields for the payload.
        info.extend(mapred.dict_to_ordered_list(r, schema.au_ping_dates_keys))
        # Add the rest of the device info.
        info.extend(mapred.dict_to_ordered_list(r, schema.au_device_info_keys))
        mapred.write_datum_tuple(context, info)
        
        # Output one row per date with recorded app activity.
        # Record type identifier. 
        active = ['activedates']
        # Payload identifier.
        active.extend(payload_id)
        # For each date in turn, set it as the last element of 'active' and 
        # output the record.
        datepos = len(active)
        active.append(None)
        for d in active_dates:
            active[datepos] = d
            mapred.write_datum_tuple(context, active)
        
        # info = r.copy()
        # info['type'] = 'info'
        # mapred.write_fieldvals_tuple(context, info, schema.au_info_keys)
        # Output each active date associated with this device.
        # adates = { 'type': 'activedates', 'deviceID': r['deviceID'] }
        # for d in active_dates:
            # adates['date'] = d
            # mapred.write_fieldvals_tuple(context, adates, 
                                                # schema.au_active_date_keys)
    
    except Exception as e:
        mapred.write_condition_tuple(context, type(e).__name__ + ' ' + str(e))
        return


# Summing reducer with combiner. 
reduce = mapred.summing_reducer
combine = reduce

