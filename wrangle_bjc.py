import os
import json

import requests
import pandas as pd

from helpers import *

PROXY_URL = "http://brd-customer-hl_cecd546c-zone-zone_unlocker_test2:i2jv2kwowy6r@brd.superproxy.io:22225"
TRANSPARENCY_PAGE = "https://www.bjc.org/For-Patients-Visitors/Financial-Assistance-Billing-Resources/BJC-Hospital-Rates"

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


def get_input_dataframe(url):
    filename = derive_filename_from_url(url)

    if not os.path.isfile(filename):
        proxies = {"http": PROXY_URL, "https": PROXY_URL}

        resp = requests.get(url, proxies=proxies, verify=False)
        print(resp.url)
        print(resp)

        out_f = open(filename, "w", encoding="utf-8")
        out_f.write(resp.text)
        out_f.close()

    in_f = open(filename, "r")
    json_dict = json.load(in_f)
    in_f.close()

    in_rows = json_dict.get("data")
    in_rows = in_rows[1:]
    in_rows = list(map(lambda row: row[0], in_rows))

    df_in = pd.DataFrame(in_rows)

    return df_in


def payer_category_from_payer_name(payer_name):
    if payer_name == "gross charge":
        return "gross"
    elif payer_name == "discounted cash price":
        return "cash"
    elif payer_name == "de-identified minimum negotiated charge":
        return "min"
    elif payer_name == "de-identified maximum negotiated charge":
        return "max"
    elif payer_name == "payer-specific negotiated charge":
        return "payer"

    return "payer"


def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    del df_mid["file_create_date"]
    del df_mid["run_id"]
    del df_mid["name"]
    del df_mid["tax_id"]

    df_mid = df_mid.rename(
        columns={
            "code type": "line_type",
            "code description": "description",
            "payer": "payer_name",
            "patient_class": "setting",
        }
    )

    df_mid.loc[df_mid["setting"] == "O", "setting"] = "outpatient"
    df_mid.loc[df_mid["setting"] == "I, O", "setting"] = "both"
    df_mid.loc[df_mid["setting"] == "I,O", "setting"] = "both"
    df_mid.loc[df_mid["setting"] == "I", "setting"] = "inpatient"
    df_mid.loc[df_mid["setting"] == "E", "setting"] = None

    money_columns = df_mid.columns[5:].to_list()
    remaining_columns = df_mid.columns[:5].to_list()
    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name2",
        value_name="standard_charge",
    )

    df_mid["payer_category"] = df_mid["payer_name2"].apply(
        payer_category_from_payer_name
    )

    df_mid.loc[df_mid["payer_category"] != "payer", "payer_name"] = df_mid[
        df_mid["payer_category"] != "payer"
    ]["payer_name2"]
    del df_mid["payer_name2"]

    df_mid.loc[df_mid["line_type"] == "cpt", "hcpcs_cpt"] = df_mid[
        df_mid["line_type"] == "cpt"
    ]["code"]
    df_mid.loc[df_mid["line_type"] == "ms-drg", "ms_drg"] = df_mid[
        df_mid["line_type"] == "ms-drg"
    ]["code"]
    df_mid.loc[df_mid["line_type"] == "revCode", "rev_code"] = df_mid[
        df_mid["line_type"] == "revCode"
    ]["code"]
    df_mid.loc[df_mid["line_type"] == "ndc", "ndc"] = df_mid[
        df_mid["line_type"] == "ndc"
    ]["code"]

    df_mid["rev_code"] = df_mid["rev_code"].apply(pad_rev_code_if_needed)

    df_mid["hospital_id"] = ccn
    df_mid["local_code"] = None
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

    df_mid = df_mid[df_mid["standard_charge"].notnull()]
    df_mid = df_mid[df_mid["standard_charge"] != "N/A"]

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out


TASKS = {
    "140002": "https://www.bjc.org/Portals/0/PDF/rates/370661172_AltonMemorialHospital_standardcharges.json",
    "260032": "https://www.bjc.org/Portals/0/PDF/rates/237309937_Barnes-JewishHospital_standardcharges.json",
    "260191": "https://www.bjc.org/Portals/0/PDF/rates/431452426_Barnes-JewishStPetersHospital_standardcharges.json",
    "260162": "https://www.bjc.org/Portals/0/PDF/rates/431527130_Barnes-JewishWestCountyHospital_standardcharges.json",
    "260180": "https://www.bjc.org/Portals/0/PDF/rates/436057893_ChristianHospital_standardcharges.json",
    "140307": "https://www.bjc.org/Portals/0/PDF/rates/370635502_MemorialHospital_standardcharges.json",
    "140185": "https://www.bjc.org/Portals/0/PDF/rates/370635502_MemorialHospital_standardcharges.json",
    "260108": "https://www.bjc.org/Portals/0/PDF/rates/430652656_MissouriBaptistMedicalCenter_standardcharges.json",
    "261337": "https://www.bjc.org/Portals/0/PDF/rates/431459495_MissouriBaptistSullivanHospital_standardcharges.json",
    "260163": "https://www.bjc.org/Portals/0/PDF/rates/431332368_ParklandHealthCenter_standardcharges.json",
    "260219": "https://www.bjc.org/Portals/0/PDF/rates/412140764_ProgressWestHospital_standardcharges.json",
    "263301": "https://www.bjc.org/Portals/0/PDF/rates/430654870_StLouisChildrensHospital_standardcharges.json",
}


def main():
    out_f = open("hospital.sql", "w")

    for ccn in TASKS.keys():
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        df_in = get_input_dataframe(url)

        last_updated = df_in["file_create_date"].to_list()[0]

        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
            ein, last_updated, filename, url, TRANSPARENCY_PAGE, ccn
        )

        out_f.write(query)
        out_f.write("\n")

        print(df_in)
        df_out = convert_dataframe(df_in, ccn)
        print(df_out)

        df_out.to_csv("rate_" + ccn + ".csv", index=False)

    out_f.close()


if __name__ == "__main__":
    main()
