
# Schema to be shared by modules working with output of FTU dump job.

# The final set of keys that should be outputted 
# from the restructured and formatted FTU payload.
# Any of these that are missing will be added as None.
# Extraneous keys will be removed.
# Final values will be combined into a tuple in the order given here.

final_keys = [
    'pingDate', 'submissionDate',
    'os',
    'country',
    'product_model',
    'locale',
    'update_channel', 'app.update.channel',
    'platform_version', 'platform_build_id',
    'icc.mcc', 'icc.mnc',
    'icc.country', 'icc.network', 'icc.name',
    'network.mcc', 'network.mnc',
    'network.country', 'network.network', 'network.name',
    'screenWidth', 'screenHeight', 'devicePixelRatio',
    'software', 'hardware', 'firmware_revision', 'activationDate'
]

csv_headers = [
    'ping_date', 'submission_date',
    'os',
    'country',
    'device',
    'locale',
    'update_channel', 'update_channel_other',
    'platform_version', 'platform_build_id',
    'sim_mcc', 'sim_mnc',
    'sim_mcc_country', 'sim_mnc_network', 'sim_network_name',
    'network_mcc', 'network_mnc',
    'network_mcc_country', 'network_mnc_network', 'network_network_name',
    'screen_width', 'screen_height', 'device_pixel_ratio',
    'software', 'hardware', 'firmware_revision', 'activation_date'
]
