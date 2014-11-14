
# Schema to be shared by modules working with output of FTU dump job.

# The final set of keys that should be outputted 
# from the restructured and formatted FTU payload.
# Any of these that are missing will be added as None.
# Extraneous keys will be removed.
# Final values will be combined into a tuple in the order given here.

final_keys = [
    'pingDate', 
    'submissionDate',
    'os',
    'country',
    'product_model',
    # 5 
    'locale',
    'update_channel', 
    'app.update.channel',
    'platform_version', 
    'platform_build_id',
    # 10
    'icc.mcc', 
    'icc.mnc',
    'icc.country', 
    'icc.network', 
    'icc.name',
    # 15
    'network.mcc', 
    'network.mnc',
    'network.country', 
    'network.network', 
    'network.name',
    # 20
    'screenWidth', 
    'screenHeight', 
    'devicePixelRatio',
    'software', 
    'hardware', 
    # 25
    'firmware_revision', 
    'activationDate'
]

dump_csv_headers = [
    'ping_date', 
    'submission_date',
    'os',
    'country',
    'device',
    # 5
    'locale',
    'update_channel', 
    'update_channel_other',
    'platform_version', 
    'platform_build_id',
    # 10
    'sim_mcc', 
    'sim_mnc',
    'sim_mcc_country', 
    'sim_mnc_network', 
    'sim_network_name',
    # 15
    'network_mcc', 
    'network_mnc',
    'network_mcc_country', 
    'network_mnc_network', 
    'network_network_name',
    # 20
    'screen_width', 
    'screen_height', 
    'device_pixel_ratio',
    'software', 
    'hardware', 
    # 25
    'firmware_revision', 
    'activation_date',
    'count'
]

dashboard_csv_headers = [
    'date', 
    'os', 
    'country', 
    'device', 
    'operator', 
    'activations'
]
