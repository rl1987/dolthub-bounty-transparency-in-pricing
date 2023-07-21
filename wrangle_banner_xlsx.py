#!/usr/bin/python3

import base64
from io import BytesIO
from dateutil.parser import parse as parse_datetime

import pandas as pd
import requests
from lxml import html

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

TRANSPARENCY_PAGE = "https://www.bannerhealth.com/patients/billing/pricing-resources/hospital-price-transparency"

TASKS = {
    "060126": "https://www.cdmpricing.com/37cdb181c73f9a24e83a4b98eaac3589/standard-charges",
    "530012": [
        "https://www.cdmpricing.com/44cfd428729a1d6ff451462c0c696037/standard-charges",
        "https://www.cdmpricing.com/af4369c01c9f4ed50a279839e12e32eb/standard-charges",
    ],
    "061303": "https://www.cdmpricing.com/b6da3d185092cf080ca03f47d845d33c/standard-charges",
    "030105": "https://www.cdmpricing.com/acc1ddbb6da6484a54c68fb1a6e7b373/standard-charges",
    "030065": "https://www.cdmpricing.com/09b90da1083f2414ce45e5834909e66a/standard-charges",
}

# Note: this returns a tuple of:
# 1. Pandas DataFrame
# 2. last_updated value.
# 3. File name
# 4. EIN
def get_input_dataframe(mrf_url):
    hospital_id = mrf_url.split("/")[-2]
    api_url = "https://apim.services.craneware.com/api-pricing-transparency/api/public/{}/metadata/cdmFile".format(
        hospital_id
    )

    resp = requests.get(api_url)
    print(resp.url)

    base64_str = resp.json().get("contentBytes")
    content_type = resp.json().get("contentType")
    filename = resp.json().get("fileDownloadName")

    ein = derive_ein_from_filename(filename)

    content = base64.b64decode(base64_str)

    out_f = open(filename, "wb")
    out_f.write(content)
    out_f.close()

    b_f = BytesIO(content)
    df_in = pd.read_excel(b_f)

    if df_in.columns[0] != "Code":
        first_line = df_in.columns[0]
        date_str = first_line.replace("Updated on ", "").replace("Updated on: ", "")
        last_updated = parse_datetime(date_str).isoformat().split("T")[0]
        df_in = pd.read_excel(b_f, header=1)
    else:
        last_updated = "2021-01-01"

    return df_in, last_updated, filename, ein


def recognise_codes(row):
    line_type = row["line_type"]
    code = row["code"]

    if line_type == "Charge Code":
        row["local_code"] = code
    elif line_type == "HCPCS/CPT" or code_is_cpt(code) or code_is_hcpcs(code):
        row["hcpcs_cpt"] = code

    return row


def payer_category_from_payer_name(payer_name):
    payer_name = payer_name.strip()
    if payer_name == "Gross Charge":
        return "gross"
    elif payer_name == "Discounted Cash Price":
        return "cash"
    elif payer_name == "De-identified min contracted rate":
        return "min"
    elif payer_name == "De-identified max contracted rate":
        return "max"

    return "payer"


def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid = df_mid.rename(
        columns={
            "Code": "code",
            "Description": "description",
            "Type": "line_type",
        }
    )

    money_columns = df_mid.columns[3:]
    remaining_columns = df_mid.columns[:3]
    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name",
        value_name="standard_charge",
    )

    df_mid.loc[df_mid["line_type"] == "Outpatient", "setting"] = "outpatient"
    df_mid.loc[df_mid["line_type"] == "Inpatient", "setting"] = "outpatient"

    df_mid.loc[df_mid["line_type"] == "Inpatient", "line_type"] = None
    df_mid.loc[df_mid["line_type"] == "Outpatient", "line_type"] = None

    df_mid["hcpcs_cpt"] = None
    df_mid["ms_drg"] = None

    df_mid = df_mid.apply(recognise_codes, axis=1)

    df_mid.loc[df_mid["hcpcs_cpt"] == "CASH", "hcpcs_cpt"] = None
    df_mid.loc[df_mid["hcpcs_cpt"] == "TRACK", "hcpcs_cpt"] = None
    df_mid.loc[df_mid["hcpcs_cpt"] == "COMM", "hcpcs_cpt"] = None

    df_mid["standard_charge"] = df_mid["standard_charge"].apply(
        lambda rate: str(rate).replace(",", ".").strip()
    )
    df_mid = df_mid[df_mid["standard_charge"] != "N/A"]
    df_mid = df_mid[df_mid["standard_charge"] != "nan"]
    df_mid = df_mid[df_mid["standard_charge"].notnull()]

    df_mid["payer_category"] = df_mid["payer_name"].apply(
        payer_category_from_payer_name
    )

    if not "local_code" in df_mid.columns:
        df_mid["local_code"] = None

    df_mid["hospital_id"] = ccn
    df_mid["rev_code"] = None
    df_mid["apr_drg"] = None
    df_mid["eapg"] = None
    df_mid["modifiers"] = None
    df_mid["alt_hcpcs_cpt"] = None
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


def main():
    out_f = open("hospitals.sql", "w")

    for ccn in TASKS.keys():
        urls = TASKS[ccn]

        if type(urls) == str:
            urls = [urls]

        for url in urls:
            filename = derive_filename_from_url(url)
            ein = derive_ein_from_filename(filename)

            df_in, last_updated, filename, ein = get_input_dataframe(url)
            print(df_in)

            query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
                ein, last_updated, filename, "|".join(urls), TRANSPARENCY_PAGE, ccn
            )

            out_f.write(query)
            out_f.write("\n")

            df_out = convert_dataframe(df_in, ccn)
            print(query)
            print(df_out)

            df_out.to_csv("rate_" + filename + ".csv", index=False)

    out_f.close()


if __name__ == "__main__":
    main()
