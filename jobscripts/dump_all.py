
# A simple job to download all records.

def map(key, dims, value, context):
    context.write(key, value)

