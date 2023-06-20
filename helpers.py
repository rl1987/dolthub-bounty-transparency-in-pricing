#!/usr/bin/python3

from urllib.parse import urlparse
import re


def cleanup_dollar_value(value):
    if type(value) == str:
        return (
            value.replace(",", "")
            .replace("$", "")
            .replace("(", "")
            .replace(")", "")
            .strip()
        )

    return value


def cleanup_values(values):
    return list(map(lambda value: cleanup_dollar_value(value), values))


def pad_rev_code_if_needed(rev_code):
    if type(rev_code) == str and rev_code != "na":
        if len(rev_code) == 3:
            return "0" + rev_code
        elif len(rev_code) == 2:
            return "00" + rev_code
        elif len(rev_code) == 1:
            return "000" + rev_code

    return rev_code


def derive_ein_from_filename(filename):
    ein = filename.split("_")[0]
    ein = ein[:2] + "-" + ein[2:]
    return ein


def derive_filename_from_url(url):
    o = urlparse(url)
    filename = o.path.split("/")[-1]
    filename = filename.replace("%20", " ")
    return filename


# https://regexlib.com/REDetails.aspx?regexp_id=3084&AspxAutoDetectCookieSupport=1
def code_is_cpt(code):
    if type(code) != str:
        return False

    m = re.match(r"^\d{4,4}[A-Z0-9]$", code)
    return m is not None


# https://regex101.com/library/sY0wA0
def code_is_hcpcs(code):
    if type(code) != str:
        return False

    m = re.match(r"^[a-zA-Z]\d{4}$", code)
    return m is not None


# https://www.johndcook.com/blog/2019/05/05/regex_icd_codes/
def code_is_icd9(code):
    if type(code) != str:
        return False

    N = "\d{3}\.?\d{0,2}"
    E = "E\d{3}\.?\d?"
    V = "V\d{2}\.?\d{0,2}"
    icd9_regex = "|".join([N, E, V])

    m = re.match(icd9_regex, code)
    return m is not None


def code_is_icd10(code):
    if type(code) != str:
        return False

    m1 = re.match(r"[A-TV-Z][0-9][0-9AB]\.?[0-9A-TV-Z]{0,4}", code)
    # https://stackoverflow.com/a/68761242
    m2 = re.match(r"^[A-HJ-NP-Z\d]{7}$", code)
    return m1 is not None or m2 is not None


def code_is_ms_drg(code):
    if type(code) != str:
        return False

    m = re.match(r"^\d{3}$", code)
    return m is not None
