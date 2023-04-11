def cleanup_dollar_value(value):
    if type(value) == str:
        return value.replace(",", "").replace("$", "").replace("(", "").replace(")", "")

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
