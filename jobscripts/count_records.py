# import json

def map(key, dims, value, context):
    context.write("num_records", 1)

def reduce(key, values, context):
    context.write(key, sum(values))
    
combine = reduce
