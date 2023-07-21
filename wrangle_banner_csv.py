#!/usr/bin/python3

import base64
from io import StringIO
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
    "030002": "https://www.cdmpricing.com/f3e6b4138e9802fa49983b8b277b62f9/standard-charges",
    "030016": "https://www.cdmpricing.com/3cfb3cf906698fc70da08c5ac2b6692b/standard-charges",
    "030064": "https://www.cdmpricing.com/be222bd54fd847aed65ee681547f6476/standard-charges",
    "030088": "https://www.cdmpricing.com/585924912a2cd5e02db64c2aebdd44b5/standard-charges",
    "030089": "https://www.cdmpricing.com/0559f2257dbba2fa82528ab3a6b07d69/standard-charges",
    "030093": "https://www.cdmpricing.com/1bac539ab924151e457b3e9d47e805e6/standard-charges",
    "030115": "https://www.cdmpricing.com/88f9218893a7ed69a61a28c4d4ad57ae/standard-charges",
    "030122": "https://www.cdmpricing.com/d04809cfbe49a3e36c86a89963b11e9e/standard-charges",
    "030130": "https://www.cdmpricing.com/5411243ade91ad4636c25a1a7893dff6/standard-charges",
    "030147": "https://www.cdmpricing.com/ae012ff8ccbd5cb389c2cf8d21a61d19/standard-charges",
    "031304": "https://www.cdmpricing.com/d95d13128d2fdfa76c86ee952e439d95/standard-charges",
    "031318": "https://www.cdmpricing.com/35a5674bdfc0ca1fbda7a02ee32e62c0/standard-charges",
    "034004": "https://www.cdmpricing.com/3d341025cdd3be852c03575365bd3382/standard-charges",
    "051320": "https://www.cdmpricing.com/58b9711892823fdfbe9216587e7b2d09/standard-charges",
    "060001": "https://www.cdmpricing.com/b7843bd1aed8398665277f1dd1f74be4/standard-charges",
    "060076": "https://www.cdmpricing.com/fa47ddb150af8e935c7b21e78547f531/standard-charges",
    "281355": "https://www.cdmpricing.com/a6bb005af0e387343425bab5557a078f/standard-charges",
    "291313": "https://www.cdmpricing.com/d310a4fd4e69ba0d31f4c10d95f7d7e7/standard-charges",
    "531305": "https://www.cdmpricing.com/8ad6766fce7c182be1c85848ce4b21c9/standard-charges",
    "531306": "https://www.cdmpricing.com/4f223e503a44d435c96685a9a38a9864/standard-charges",
    "531307": "https://www.cdmpricing.com/4b03a29374010f22d3d1e0cf8d2e26ea/standard-charges",
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

    csv_str = base64.b64decode(base64_str).decode("utf-8-sig")
    out_f = open(filename, "w")
    out_f.write(csv_str)
    out_f.close()

    starts_at = csv_str.index("\n") + 1
    first_line = csv_str[:starts_at].strip()

    date_str = (
        first_line.split('"')[1].replace("Updated on ", "").replace("Updated on: ", "")
    )
    last_updated = parse_datetime(date_str).isoformat().split("T")[0]

    s_f = StringIO(csv_str[starts_at:])
    df_in = pd.read_csv(s_f)

    return df_in, last_updated, filename, ein


def recognise_codes(row):
    line_type = row["line_type"]
    code = row["code"]

    if line_type == "Charge Code":
        row["local_code"] = code
    elif line_type == "HCPCS/CPT":
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

    df_mid["standard_charge"] = df_mid["standard_charge"].apply(cleanup_dollar_value)
    df_mid = df_mid[df_mid["standard_charge"] != "N/A"]
    df_mid = df_mid[df_mid["standard_charge"].notnull()]

    df_mid["payer_category"] = df_mid["payer_name"].apply(
        payer_category_from_payer_name
    )

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
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        df_in, last_updated, filename, ein = get_input_dataframe(url)
        print(df_in)

        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
            ein, last_updated, filename, url, TRANSPARENCY_PAGE, ccn
        )

        out_f.write(query)
        out_f.write("\n")

        df_out = convert_dataframe(df_in, ccn)
        print(query)
        print(df_out)

        df_out.to_csv("rate_" + ccn + ".csv", index=False)

    out_f.close()


if __name__ == "__main__":
    main()
