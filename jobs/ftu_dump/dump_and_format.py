
# Download recent FxOS records, sanitize values, and aggregate occurences.

import json
from datetime import datetime, date

import ftu_formatter as ftu
import mapred
import dump_schema as schema

def consistent_ftu(r):
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
        r['country'] = r['info']['geoCountry']
        del r['info']
        # if 'ver' in r:
            # del r['ver']
        
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
                ftu.ms_timestamp_to_date(r['pingTime']).isoformat()
        if len(dims) == 6:
            r['submissionDate'] = (
                datetime.strptime(dims[5], '%Y%m%d').date().isoformat())
        
        # Merge update channel fields. 
        # If both are present, note occurrence but don't replace.
        if 'app.update.channel' in r:
            if 'update_channel' in r:
                # Both values are present.
                if r['app.update.channel'] != r['update_channel']:
                    # Note the disparity and keep both.
                    mapred.write_condition_tuple(context, 'multiple channel')
                else:
                    # Keep the common value as 'update_channel'.
                    del r['app.update.channel']    
            else:
                # Only 'app.update.channel' is present.
                # Save the value as 'update_channel'.
                r['update_channel'] = r['app.update.channel']
                del r['app.update.channel']
        
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
        
        #-----
        
        # Output specific keys.
        mapred.write_fieldvals_tuple(context, r, schema.final_keys)
    
    except Exception as e:
        mapred.write_condition_tuple(context, str(e))
        return


# Summing reducer with combiner. 
reduce = mapred.summing_reducer
combine = reduce

# def reduce(key, values, context):
    # context.write(key, sum(values))

# combine = reduce


