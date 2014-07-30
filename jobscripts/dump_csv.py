import json

# Sanitize records and output as CSV.


def map(key, dims, value, context):
    data = json.loads(value)
    
 
# Set CSV output. 
def setup_reduce(context):
    context.field_separator = ','


def reduce(key, values, context):
    context.writecsv(values)