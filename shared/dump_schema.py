
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


