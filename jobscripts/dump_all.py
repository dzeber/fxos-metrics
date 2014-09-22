
# A simple job to download all records.

def map(key, dims, value, context):
    # Add submission date to raw JSON.
    value = value.rstrip('}')
    value = value + ',"submissionDate":"' + dims.pop() + '"}'
    context.write(key, value)
