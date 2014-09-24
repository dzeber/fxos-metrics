
# A simple job to download all records.

import mapred

def map(key, dims, value, context):
    # Add submission date to raw JSON, if available.
    if len(dims) == 6:
        value = value.rstrip('}')
        value = value + ',"submissionDate":"' + dims[5] + '"}'
    # else:
        # write_condition(context, 'Irregular dims: ' + str(dims))
    context.write(key, value)
