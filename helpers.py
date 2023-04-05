def cleanup_dollar_value(value):
    if type(value) == str:
        return value.replace(",", "").replace("$", "")

    return value

def cleanup_values(values):
    return list(map(lambda value: cleanup_dollar_value(value), values))
