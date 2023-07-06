#!/usr/bin/python3

import subprocess
from datetime import date

import pandas as pd

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

TRANSPARENCY_PAGE = "https://www.aurorahealthcare.org/patients-visitors/billing-payment/health-care-costs"

TASKS = {
    "520034": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391211629_aurora-medical-center-manitowoc-county_standardcharges.xml",
    "520035": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390930748_aurora-sheboygan-memorial-medical-center_standardcharges.xml",
    "520038": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391150165_aurora-medical-center-washington-county_standardcharges.xml",
    "520059": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-burlington_standardcharges.xml",
    "520102": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-lakeland-medical-center_standardcharges.xml",
    "520113": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391528430_aurora-medical-center-bay-area_standardcharges.xml",
    "520189": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-kenosha_standardcharges.xml",
    "520193": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391947472_aurora-baycare-medical-center_standardcharges.xml",
    "520198": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391027676_aurora-medical-center-oshkosh_standardcharges.xml",
    "520206": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-summit_standardcharges.xml",
    "520207": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/272953799_aurora-medical-center-grafton_standardcharges.xml",
    "520064": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806181_aurora-sinai-medical-center_standardcharges.xml",
    "520138": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806181_aurora-st-lukes-medical-center_standardcharges.xml",
    "520139": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391022464_aurora-west-allis-medical-center_standardcharges.xml",
}


def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])

    df_in = pd.read_xml(filename, dtype=str)

    return df_in


def get_payer_category_from_payer_name(payer_name):
    if payer_name == "Min":
        return "min"
    elif payer_name == "Max":
        return "max"
    elif payer_name == "Self_Pay":
        return "cash"
    elif payer_name.endswith("_Fee"):
        return "gross"

    return "payer"


def set_code_fields(row):
    code = row["code"]

    if type(row["hcpcs_cpt"]) != str:
        if code_is_cpt(code) or code_is_hcpcs(code):
            row["hcpcs_cpt"] = code

    if code_is_ms_drg(code):
        row["ms_drg"] = code

    return row


def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    del df_mid["Facility"]

    df_mid = df_mid.rename(
        columns={
            "Type": "line_type",
            "Chargecode_DRG_CPT": "code",
            "Description": "description",
            "Rev": "rev_code",
            "CPT": "hcpcs_cpt",
            "NDC": "ndc",
        }
    )

    money_columns = df_mid.columns[6:]
    remaining_columns = df_mid.columns[:6]
    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name",
        value_name="standard_charge",
    )
    df_mid["standard_charge"] = df_mid["standard_charge"].apply(cleanup_dollar_value)

    df_mid["rev_code"] = df_mid["rev_code"].apply(
        lambda rev_code: rev_code.zfill(4) if type(rev_code) == str else rev_code
    )

    df_mid["local_code"] = None
    df_mid["local_code"] = df_mid["hcpcs_cpt"].apply(
        lambda hcpcs_cpt: hcpcs_cpt
        if type(hcpcs_cpt) == str
        and (not code_is_cpt(hcpcs_cpt) and not code_is_hcpcs(hcpcs_cpt))
        else None
    )
    df_mid.loc[df_mid["local_code"].notnull(), "hcpcs_cpt"] = None
    df_mid.loc[df_mid["hcpcs_cpt"].notnull(), "hcpcs_cpt"] = df_mid[
        df_mid["hcpcs_cpt"].notnull()
    ]["hcpcs_cpt"].str.upper()

    df_mid["payer_category"] = df_mid["payer_name"].apply(
        get_payer_category_from_payer_name
    )

    df_mid.loc[df_mid["line_type"] == "IP DRG*", "setting"] = "inpatient"
    df_mid.loc[df_mid["line_type"] == "OP PROC*", "setting"] = "outpatient"

    df_mid["ms_drg"] = None

    df_mid = df_mid.apply(set_code_fields, axis=1)

    df_mid["hospital_id"] = ccn
    df_mid["apr_drg"] = None
    df_mid["eapg"] = None
    df_mid["modifiers"] = None
    df_mid["alt_hcpcs_cpt"] = None
    df_mid["thru"] = None
    df_mid["apc"] = None
    df_mid["icd"] = None
    df_mid["drug_hcpcs_multiplier"] = None
    df_mid["drug_quantity"] = None
    df_mid["drug_unit_of_measurement"] = None
    df_mid["drug_type_of_measurement"] = None
    df_mid["plan_name"] = None
    df_mid["standard_charge_percent"] = None
    df_mid["contracting_method"] = None
    df_mid["additional_generic_notes"] = None
    df_mid["additional_payer_specific_notes"] = None
    df_mid["billing_class"] = None

    df_mid = df_mid[df_mid["standard_charge"].notnull()]

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out


def extract_last_updated(df_in):
    date_str = df_in.columns[7]
    date_str = date_str[1:-4]
    day, month, year = date_str.split("_")
    day = int(day)
    month = int(month)
    year = int(year) + 2000

    last_updated = date(year=year, month=month, day=day).isoformat()

    return last_updated


def main():
    out_f = open("hospitals.sql", "w")

    for ccn in TASKS.keys():
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        df_in = get_input_dataframe(url)
        print(df_in)

        try:
            last_updated = extract_last_updated(df_in)
        except:
            last_updated = "2021-01-01"

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
