"""
Simple job to download all raw records as unparsed JSON strings.

No reducer is necessary.
"""

def map(key, dims, value, context):
    """Emit raw JSON records, appending server-side submission date.""" 
    if len(dims) == 6:
        value = value.rstrip('}')
        value = value + ',"submissionDate":"' + dims[5] + '"}'
    context.write(key, value)
