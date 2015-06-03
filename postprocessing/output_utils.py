"""
Utilities for processing the output of MR jobs and writing to files.
"""


def encode_for_csv(val):
    """Encode a value as UTF-8."""
    return unicode(val).encode('utf-8', 'backslashreplace')


def write_unicode_row(writer, row):
    """Write CSV row after encoding values explicity (to avoid errors)."""
    writer.writerow([encode_for_csv(v) for v in row])


def print_counter_info(counters):
    """Format and print the counters recorded in a MR job using utils.mapred."""
    for name in counters:
        counter = counters[name]
        # If this is a group, print all subcounters.
        if type(counter) is dict:
            for cname in counter:
                print(name + ' | ' + cname + ' :  ' + str(counter[cname]))
        else:
            print(name + ' :  ' + str(counter))


def print_condition_info(conditions):
    """Format and print the conditions recorded in a MR job using utils.mapred."""
    for name in conditions:
        print(name + ' :  ' + str(conditions[name]))

