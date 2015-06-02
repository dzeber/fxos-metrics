"""
Simple job to download all raw records as unparsed JSON strings.

No reducer is necessary.
"""

import payload_utils as util

def map(key, dims, value, context):
    """Emit raw JSON records, appending server-side submission date.""" 
    value = util.insert_submission_date(value, dims)
    context.write(key, value)
