"""
Schemas (lists of keys) to be used for the outputs of MR jobs.

The schemas are intended to be used together with 
utils.mapred.write_fieldvals_tuple(). Since the MR outputs contain the values 
of these fields but not the keys, the schemas are maintained centrally for 
transparency.
"""

# The set of keys outputted from the restructured and formatted FTU payload.
final_keys = [
    'pingDate',
    'submissionDate',
    'os',
    'country',
    'product_model',
    # 5 
    'locale',
    'language',
    'update_channel',
    # 'app.update.channel',
    'update_channel_standardized',
    'platform_version',
    # 10
    'platform_build_id',
    'icc.mcc',
    'icc.mnc',
    'icc.country',
    'icc.network',
    # 15
    'icc.name',
    'network.mcc',
    'network.mnc',
    'network.country',
    'network.network',
    # 20
    'network.name',
    'screenWidth',
    'screenHeight',
    'devicePixelRatio',
    'software',
    # 25
    'hardware',
    'firmware_revision',
    'activationDate'
]

# The column names and ordering for the dump CSV generated from the FTU 
# ping data.
dump_csv_headers = [
    'ping_date',
    'submission_date',
    'os',
    'country',
    'device',
    # 5
    'locale',
    'language',
    'update_channel',
    # 'update_channel_other',
    'update_channel_standardized',
    'platform_version',
    # 10
    'platform_build_id',
    'sim_mcc',
    'sim_mnc',
    'sim_mcc_country',
    'sim_mnc_network',
    # 15
    'sim_network_name',
    'network_mcc',
    'network_mnc',
    'network_mcc_country',
    'network_mnc_network',
    # 20
    'network_network_name',
    'screen_width',
    'screen_height',
    'device_pixel_ratio',
    'software',
    # 25
    'hardware',
    'firmware_revision',
    'activation_date',
    'count'
]

# The column names and ordering for the dashboard CSV generated from the FTU 
# ping data.
dashboard_csv_headers = [
    'date',
    'os',
    'country',
    # 'language',
    'device',
    'operator',
    'activations'
]

#------------------------------------------------------------------------

# AU pings should be identifiable using these fields.
au_ping_identifier_keys = [
    'deviceID',
    'start',
    'stop'
    # 'dogfood'
]

# The dates covered by a AU ping.
au_ping_dates_keys = [
    'submissionDate',
    'startDate',
    'stopDate'
]

# The top-level device information to be recorded for AU payloads.
# This should generally remain constant across AU pings from the same device,
# although some values may change (eg. network info or OS version).
au_device_info_keys = [
    # 'type',
    # 'deviceID',
    # 'submissionDate',
    # 'startDate',
    # 'stopDate',
    'os',
    'country',
    'product_model',
    'locale',
    'language',
    'update_channel',
    'update_channel_standardized',
    'platform_version',
    'platform_build_id',
    'icc.mcc',
    'icc.mnc',
    'icc.country',
    'icc.network',
    'icc.name',
    'network.mcc',
    'network.mnc',
    'network.country',
    'network.network',
    'network.name',
    'screenWidth',
    'screenHeight',
    'devicePixelRatio',
    'software',
    'hardware',
    # 'firmware_revision',
    'developer.menu.enabled'
]

au_app_data_keys = [
    'usageTime',
    'invocations',
    'installs',
    'uninstalls',
    'activities'
]

au_search_count_keys = [
    'count'
]
    

# au_active_date_keys = [
    # 'type',
    # 'deviceID',
    # 'date'
# ]

au_info_csv = [
    'deviceID',
    'start_timestamp',
    'stop_timestamp',
    'is_dogfood',
    'submission_date',
    'start_date',
    'stop_date',
    'os',
    'country',
    'device',
    'locale',
    'language',
    'update_channel',
    'update_channel_standardized',
    'platform_version',
    'platform_build_id',
    'sim_mcc',
    'sim_mnc',
    'sim_mcc_country',
    'sim_mnc_network',
    'sim_network_name',
    'network_mcc',
    'network_mnc',
    'network_mcc_country',
    'network_mnc_network',
    'network_network_name',
    'screen_width',
    'screen_height',
    'device_pixel_ratio',
    'software',
    'hardware',
    'developer_menu_enabled'
]

au_app_csv = [
    'deviceID',
    'start_timestamp',
    'stop_timestamp',
    'is_dogfood',
    'app_url',
    'date',
    'usage_time',
    'invocations',
    'installs',
    'uninstalls',
    'activities'
]

au_search_csv = [
    'deviceID',
    'start_timestamp',
    'stop_timestamp',
    'is_dogfood',
    'provider',
    'date',
    'searches'
]

# au_activity_csv = [
    # 'deviceID',
    # 'is_dogfood',
    # 'activity_date'
# ]

