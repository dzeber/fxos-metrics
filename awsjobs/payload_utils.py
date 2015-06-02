"""
A collection of useful functions for working with FxOS Metrics data payloads.

These will generally be called in the map function of an AWS job.
"""

import re


def get_submission_date(dims):
    """Extract the server-side submission date from the MR dims list.
    
    Returns the date as a string of the form yyyymdd, or else None if it was
    not found.
    """
    sdate = dims[5] if len(dims) == 6 else None
    if sdate is None or re.match('^[0-9]{8}$', sdate) is None:
        return None
    return sdate


def insert_submission_date(value, dims):
    """Add the payload submission date to the JSON payload.
    
    If the submission date could be found, it is inserted at the end of the 
    raw JSON payload as a value with the key 'submissionDate'.
    """
    sdate = get_submission_date(dims)
    if sdate is not None:
        value = value.rstrip('}')
        value = value + ',"submissionDate":"' + sdate + '"}'
    return value


def search_nested_dict(obj, storage, keypath = '', exclude = (), sep = '|', 
                                                            keysonly = False):
    """Recursively follow paths down to the terminal data values of a 
    hierarchical structure of nested dicts (leaf nodes in a tree).
    
    Depending on the value of keysonly, collects either a flattened version
    of the dict with keypaths mapping to terminal values, or the list of 
    keypaths alone. A keypath is constructed by joining the list of dict keys 
    leading to a terminal value using a separator character.
    
    For convenience, some keypaths can be excluded from further investigation.
    If these lead to a dict, they will be treated as terminal nodes whose 
    value is an empty dict.
        
    obj - the object to be investigated
    keypath - the current key path being investigated
    storage - an object to contain the output: a list if keysonly is True,
              and a dict otherwise
    exclude - a tuple of keypaths that should not be searched further
    sep - the separator to use in joining the keys into a keypath
    keyonly - should only the keypaths be returned, or keys and terminal values
    """
    # Treat excluded subpaths as pointing to an empty dict.
    if keypath in exclude:
        obj = {}
    if isinstance(obj, dict) and len(obj) > 0:
        # If we have a non-empty subdict, investigate its elements.
        for k in obj:
            newkeypath = keypath + sep + k if keypath else k
            search_nested_dict(obj[k], storage, newkeypath, exclude, sep, 
                                                                    keysonly)
    else:
        # We have a terminal data value. 
        # Store the keypath and the value if required.
        if keysonly:
            storage.append(keypath)
        else:
            storage[keypath] = obj


# keys = list()
keys = {}
search_nested_dict(aa, keys,
    # keysonly = True)
    keysonly = False)
# for k in keys: print(k)
for i in keys.items(): print(i)


def get_keypaths(adict, exclude = (), sep = '|'):
    """ Recursively follow paths down to the terminal data values of a 
    hierarchical structure of nested dicts (leaf nodes in a tree).
    
    Collect only the keypaths leading down to the terminal nodes, and 
    return them as a list. The keypath strings are constructed by joining the 
    key strings using the sep character.
    
    If any paths should be excluded from further search, they can be specified
    using the 'exclude' arg. This should be a tuple of paths, where each path
    is represented as a tuple of key strings.
    """
    keys = list()
    if len(exclude) > 0:
        exclude = [sep.join(path) for path in exclude]
    search_nested_dict(adict, keys, exclude = exclude, sep = sep,
                                                            keysonly = True)
    return keys


def flatten_nested_dict(adict, exclude = (), sep = '|'):
    """ Recursively follow paths down to the terminal data values of a 
    hierarchical structure of nested dicts (leaf nodes in a tree).
    
    Collect keypaths and terminal node values, and return them as a dict. 
    The keypath strings are constructed by joining the key strings using the 
    sep character.
    
    If any paths should be excluded from further search, they can be specified
    using the 'exclude' arg. This should be a tuple of paths, where each path
    is represented as a tuple of key strings.
    """
    flattened = {}
    if len(exclude) > 0:
        exclude = [sep.join(path) for path in exclude]
    search_nested_dict(adict, flattened, exclude = exclude, sep = sep)
    return flattened




