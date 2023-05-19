#!/usr/bin/python3

from urllib.parse import urlparse

def cleanup_dollar_value(value):
    if type(value) == str:
        return value.replace(",", "").replace("$", "").replace("(", "").replace(")", "").strip()

    return value

def cleanup_values(values):
    return list(map(lambda value: cleanup_dollar_value(value), values))

def pad_rev_code_if_needed(rev_code):
    if type(rev_code) == str and rev_code != 'na':
        if len(rev_code) == 3:
            return '0' + rev_code
        elif len(rev_code) == 2:
            return '00' + rev_code
        elif len(rev_code) == 1:
            return '000' + rev_code

    return rev_code

def derive_ein_from_filename(filename):
    ein = filename.split("_")[0]
    ein = ein[:2] + "-" + ein[2:]
    return ein

def derive_filename_from_url(url):
    o = urlparse(url)
    filename = o.path.split("/")[-1]
    return filename
