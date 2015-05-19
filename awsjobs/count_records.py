"""
Simple job to count records, to be used for testing.
"""

from utils.mapred import summing_reducer

def map(key, dims, value, context):
    """Count records by emitting 1 for each record."""
    context.write("num_records", 1)

# def reduce(key, values, context):
    # context.write(key, sum(values))

reduce = summing_reducer
    
combine = reduce
