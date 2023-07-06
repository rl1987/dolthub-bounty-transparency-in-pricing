#!/usr/bin/python3

import csv
from urllib.parse import urlparse
import subprocess
import sys
import json

import pandas as pd
import requests

from helpers import *

TARGET_COLUMNS = [
    "hospital_id",
    #'row_id',
    "line_type",
    "description",
    "rev_code",
    "local_code",
    "code",
    "ms_drg",
    "apr_drg",
    "eapg",
    "hcpcs_cpt",
    "modifiers",
    "alt_hcpcs_cpt",
    "thru",
    "apc",
    "icd",
    "ndc",
    "drug_hcpcs_multiplier",
    "drug_quantity",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "billing_class",
    "setting",
    "payer_category",
    "payer_name",
    "plan_name",
    "standard_charge",
    "standard_charge_percent",
    "contracting_method",
    "additional_generic_notes",
    "additional_payer_specific_notes",
]


def get_input_data(hpi_url):
    def_id = urlparse(hpi_url).path.split("/")[-1].replace("or", "")

    json_data = {
        "defId": def_id,
        "priceStatus": "published",
        "listName": "machineRead",
    }

    resp = requests.post(
        "https://api.hospitalpriceindex.com/itemList/detail", json=json_data
    )
    print(resp.url)

    json_dict = resp.json()
    results = json_dict.get("result")
    assert results is not None
    assert type(results) == list
    assert len(results) > 0

    result = json_dict.get("result")[0]

    ein = result.get("eiNumber")
    ein = ein[:2] + "-" + ein[2:]

    mrf_url = result.get("extractFile").replace("\\", "/")

    filename = derive_filename_from_url(mrf_url)

    subprocess.run(["wget", "--no-clobber", mrf_url, "-O", filename])

    in_f = open(filename, "r")
    json_content = json.load(in_f)
    in_f.close()

    last_updated = json_content[0].get("lastUpdated")

    df_in = pd.DataFrame(json_content[0].get("item"))

    return df_in, mrf_url, filename, last_updated, ein


def code_is_rev_code(code):
    if type(code) != str:
        return False

    return len(code) == 4 and code.isnumeric()


def split_codes(row):
    codes = row.get("code")
    codes = codes.split(",")

    for code in codes:
        code = code.strip()

        if code_is_rev_code(code):
            row["rev_code"] = code
        elif code_is_cpt(code) or code_is_hcpcs(code):
            if row.get("hcpcs_cpt") is None:
                row["hcpcs_cpt"] = code
            else:
                row["alt_hcpcs_cpt"] = code
        elif code_is_ms_drg(code):
            row["ms_drg"] = code

    return row


def convert_dataframe(ccn, df_in):
    df_mid = pd.DataFrame(df_in)

    df_mid = df_mid.rename(
        columns={
            "iobSelection": "setting",
            "Associated_Codes": "code",
            "payer": "payer_name",
        }
    )

    money_columns = df_mid.columns[2:-2].to_list()
    remaining_columns = set(df_mid.columns.to_list()) - set(money_columns)
    remaining_columns = list(remaining_columns)

    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name2",
        value_name="standard_charge",
    )

    df_mid.loc[df_mid["payer_name2"] != "Payer_Allowed_Amount", "payer_name"] = df_mid[
        df_mid["payer_name2"] != "Payer_Allowed_Amount"
    ]["payer_name2"]
    del df_mid["payer_name2"]

    df_mid = df_mid[df_mid["standard_charge"] != "N/A"]
    df_mid = df_mid[df_mid["standard_charge"].notnull()]

    df_mid["setting"] = df_mid["setting"].str.lower()

    df_mid["ms_drg"] = None
    df_mid["hcpcs_cpt"] = None
    df_mid["alt_hcpcs_cpt"] = None
    df_mid["rev_code"] = None

    df_mid = df_mid.apply(split_codes, axis=1)

    df_mid["payer_category"] = "payer"
    df_mid.loc[df_mid["payer_name"] == "Avg_Gross_Charge", "payer_category"] = "gross"
    df_mid.loc[df_mid["payer_name"] == "Cash_Discount_Price", "payer_category"] = "cash"
    df_mid.loc[
        df_mid["payer_name"] == "Deidentified_Min_Allowed", "payer_category"
    ] = "min"
    df_mid.loc[
        df_mid["payer_name"] == "DeIdentified_Max_Allowed", "payer_category"
    ] = "max"

    df_mid["hospital_id"] = ccn
    df_mid["line_type"] = None
    df_mid["local_code"] = None
    df_mid["apr_drg"] = None
    df_mid["eapg"] = None
    df_mid["modifiers"] = None
    df_mid["thru"] = None
    df_mid["apc"] = None
    df_mid["icd"] = None
    df_mid["ndc"] = None
    df_mid["drug_hcpcs_multiplier"] = None
    df_mid["drug_quantity"] = None
    df_mid["drug_unit_of_measurement"] = None
    df_mid["drug_type_of_measurement"] = None
    df_mid["billing_class"] = None
    df_mid["plan_name"] = None
    df_mid["standard_charge_percent"] = None
    df_mid["contracting_method"] = None
    df_mid["additional_generic_notes"] = None
    df_mid["additional_payer_specific_notes"] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out


def perform_task(h_f, ccn, hpi_url, transparency_page):
    df_in, mrf_url, filename, last_updated, ein = get_input_data(hpi_url)

    df_out = convert_dataframe(ccn, df_in)
    print(df_out)

    df_out.to_csv("rate_" + ccn + ".csv", index=False)

    query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
        ein, last_updated, filename, mrf_url, transparency_page, ccn
    )

    h_f.write(query)
    h_f.write("\n")


def main():
    if len(sys.argv) != 2:
        print("Usage:")
        print("{} <tasks_csv>".format(sys.argv[0]))
        return

    tasks_csv = sys.argv[1]

    in_f = open(tasks_csv)

    csv_reader = csv.DictReader(in_f)

    h_f = open("hospitals.sql", "w")

    for row in csv_reader:
        transparency_page = row.get("transparency_page")
        ccn = row.get("ccn")
        app_url = row.get("app_url")

        try:
            perform_task(h_f, ccn, app_url, transparency_page)
        except Exception as e:
            print(e)

            try:
                perform_task(h_f, ccn, app_url, transparency_page)
            except Exception as e:
                print(e)

    h_f.close()
    in_f.close()


if __name__ == "__main__":
    main()
