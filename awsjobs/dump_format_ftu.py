"""
Map-reduce job to download recent FxOS records, sanitize and reformat field
values, and count occurrences of unique combinations.

For each record, the mapper parses the record and stores relevant values in a
dict, after reformatting and sanity-checking. The reducer then counts 
occurrences of unique sets of field/value combinations.

The output from the job can be thought of as rows, each row representing
a unique segment defined by its field values, together with a column of counts
indicating how many records were found belonging to the segment.
"""

import json
from datetime import datetime, date

import utils.ftu_formatter as ftu
import utils.mapred as mapred
import utils.dump_schema as schema


def consistent_ftu(r):
    """Simple sanity check.
    
    Check that the payloads have correct 'reason' ('ftu') and 'appName' 
    ('FirefoxOS'), and check that the other 'info' fields are consistent with 
    those prefixed with 'deviceinfo'.
    """
    if 'info' not in r:
        return False
    info = r['info']
    
    return (info.get('appName') == 'FirefoxOS' and
        info.get('reason') == 'ftu' and
        ('deviceinfo.update_channel' not in r or
            info.get('appUpdateChannel') == r['deviceinfo.update_channel']) and
        info.get('appVersion') == r['deviceinfo.platform_version'] and
        info.get('appBuildID') == r['deviceinfo.platform_build_id'])


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
        if not consistent_ftu(r):
            mapred.write_condition_tuple(context, 'inconsistent')
            return
        
        #-----
        
        # Rearrange.
        
        # Keep only geo code from info.
        if 'geoCountry' in r['info']:
            r['country'] = r['info']['geoCountry']
        del r['info']
        
        # Strip deviceinfo prefix when it occurs. 
        dikeys = [k for k in r if k.startswith('deviceinfo.')]
        for k in dikeys:
            v = r[k]
            del r[k]
            r[k[11:]] = v
        
        # Flatten sub-dicts. 
        dictvals = [(k,v) for (k,v) in r.iteritems() if isinstance(v, dict)]
        for (k, dictval) in dictvals:
            del r[k]
            for (subkey, subval) in dictval.iteritems():
                r[k + '.' + subkey] = subval
        
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
        if 'activationTime' in r:
            r['activationDate'] = (
                ftu.ms_timestamp_to_date(r['activationTime']).isoformat())
        if 'pingTime' in r:
            r['pingDate'] = (
                ftu.ms_timestamp_to_date(r['pingTime']).isoformat())
        if len(dims) == 6:
            r['submissionDate'] = (
                datetime.strptime(dims[5], '%Y%m%d').date().isoformat())
        
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
                # else:
                # del r['app.update.channel']    
            else:
                # Only 'app.update.channel' is present.
                # Save the value as 'update_channel'.
                r['update_channel'] = r['app.update.channel']
            # Keep 'update_channel'.
            del r['app.update.channel']
        
        # Add simplified form of update channel - 
        # more useful for separating them.
        if 'update_channel' in r:
            r['update_channel_standardized'] = ftu.get_standard_channel(
                r['update_channel'])
        
        # Apply substitutions:
        if 'os' in r:
            r['os'] = ftu.format_os_string(r['os'])
            
        if 'product_model' in r:
            r['product_model'] = ftu.format_device_string(r['product_model'])
        
        if 'country' in r:
            country_name = ftu.lookup_country_code(r['country'])
            # If lookup fails, keep original geo code.
            if country_name is not None:
                r['country'] = country_name
                
        # Convert locale code to language name.
        # Keep original locale code for reference.
        if 'locale' in r:
            r['language'] = ftu.lookup_language(r['locale'])
        
        
        # Keep original network codes, but try looking them up.
        for prefix in 'icc','network':
            mcc_key = prefix + '.mcc'
            mnc_key = prefix + '.mnc'
            if mcc_key in r:
                # Look up country code.
                r[prefix + '.country'] = ftu.lookup_mcc(r[mcc_key])
                if mnc_key in r:
                    # Look up network code, and format string.
                    nw = ftu.lookup_mnc(r[mcc_key], r[mnc_key])
                    if nw is not None:
                        nw = ftu.format_operator_string(nw)
                    r[prefix + '.network'] = nw
        
        # Format network name strings. 
        if 'icc.spn' in r:
            r['icc.name'] = ftu.format_operator_string(r['icc.spn'])
        if 'network.operator' in r:
            r['network.name'] = ftu.format_operator_string(r['network.operator'])
        
        # Opportunity for general formatting rules 
        # based on combinations of values. 
        # In particular, setting OS to '1.3T' for Tarako devices.
        r = ftu.apply_general_formatting(r)
        
        #-----
        
        # Output specific keys.
        mapred.write_fieldvals_tuple(context, r, schema.final_keys)
    
    except Exception as e:
        mapred.write_condition_tuple(context, type(e).__name__ + ' ' + str(e))
        return


# Summing reducer with combiner. 
reduce = mapred.summing_reducer
combine = reduce

