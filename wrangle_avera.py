#!/usr/bin/python3

import csv
import subprocess
from urllib.parse import urlparse
import os

from dateutil.parser import parse as parse_datetime
import httpx
import openpyxl
import pandas as pd

from helpers import cleanup_dollar_value

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
    "rate_category",
    "payer_name",
    "plan_name",
    "standard_charge",
    "standard_charge_percent",
    "contracting_method",
    "additional_generic_notes",
    "additional_payer_specific_notes",
]


def create_session():
    session = httpx.Client(http2=True)

    session.headers = {
        "authority": "www.avera.org",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    }

    resp = session.get("https://www.avera.org/patients-visitors/price-transparency/")
    print(resp.url)

    return session


def download_file(session, url, filename):
    if os.path.isfile(filename):
        print("{} already downloaded - skipping".format(filename))
        return

    resp = session.get(url)
    print(resp.url)

    out_f = open(filename, "wb")
    out_f.write(resp.content)
    out_f.close()


def derive_ein_from_filename(filename):
    ein = filename.split("_")[0]
    ein = ein[:2] + "-" + ein[2:]
    return ein


def derive_filename_from_url(url):
    o = urlparse(url)
    filename = o.path.split("/")[-1]
    return filename


def rate_category_from_payer(payer):
    if payer == "Gross Charge":
        return "gross"
    elif payer == "Discounted Cash Price":
        return "cash"
    elif payer.startswith("Min"):
        return "min"
    elif payer.startswith("Max"):
        return "max"

    return "negotiated"


def convert_cdm_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid = df_mid.rename(
        columns={
            "CPT": "cpt",
            "Charge Code": "local_code",
            "Description": "description",
            "Hcpcs": "hcpcs",
            "Revcode": "rev_code",
        }
    )

    columns = df_mid.columns.to_list()
    columns.pop(columns.index("Gross Charge"))
    df_mid = df_mid[columns + ["Gross Charge"]]

    del df_mid["Facility Name"]

    money_columns = df_mid.columns[5:]
    remaining_columns = df_mid.columns.to_list()[:5]
    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name",
        value_name="standard_charge",
    )

    def join_hcpcs_cpt_fields(row):
        if row["cpt"] is not None:
            return row["cpt"]

        if row["hcpcs"] is not None:
            return row["hcpcs"]

        return None

    df_mid.loc[df_mid["cpt"].isnull(), "cpt"] = None
    df_mid.loc[df_mid["hcpcs"].isnull(), "hcpcs"] = None

    df_mid["hcpcs_cpt"] = df_mid.apply(join_hcpcs_cpt_fields, axis=1)
    df_mid["alt_hcpcs_cpt"] = df_mid.apply(
        lambda row: row["hcpcs"]
        if row["hcpcs"] is not None and row["cpt"] is not None
        else None,
        axis=1,
    )

    del df_mid["cpt"]
    del df_mid["hcpcs"]

    df_mid["rate_category"] = df_mid["payer_name"].apply(rate_category_from_payer)

    df_mid["hospital_id"] = ccn
    df_mid["line_type"] = None
    df_mid["rev_code"] = df_mid["rev_code"].fillna("")
    df_mid["rev_code"] = df_mid["rev_code"].astype(str)
    df_mid["rev_code"] = df_mid["rev_code"].str[:4]  # 0964an
    df_mid["rev_code"] = df_mid["rev_code"].apply(
        lambda rev_code: rev_code.zfill(4)
        if len(rev_code) > 0 and len(rev_code) < 4
        else rev_code
    )
    df_mid.loc[df_mid["rev_code"] == "", "rev_code"] = None
    df_mid["code"] = None
    df_mid["ms_drg"] = None
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
    df_mid["setting"] = None
    df_mid["plan_name"] = None
    df_mid["standard_charge_percent"] = None
    df_mid["contracting_method"] = None
    df_mid["additional_generic_notes"] = None
    df_mid["additional_payer_specific_notes"] = None

    df_mid = df_mid.dropna(subset=["standard_charge"])

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    print(df_out)

    return df_out


def convert_bundle_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid.columns = list(map(lambda c: c.strip(), df_mid.columns.to_list()))

    df_mid = df_mid.rename(
        columns={
            "Item / Service Description": "description",
            "Item /Service Description": "description",
            "Patient Type": "setting",
        }
    )

    del df_mid["Facility Name"]

    if "setting" in df_mid.columns.to_list():
        df_mid.loc[df_mid["setting"] == "O", "setting"] = "outpatient"
        df_mid.loc[df_mid["setting"] == "I", "setting"] = "inpatient"

    money_columns = df_mid.columns[2:]
    remaining_columns = df_mid.columns[:2]

    df_mid = pd.melt(
        df_mid,
        id_vars=remaining_columns,
        var_name="payer_name",
        value_name="standard_charge",
    )

    df_mid["rate_category"] = df_mid["payer_name"].apply(rate_category_from_payer)

    df_mid["hospital_id"] = ccn
    df_mid["line_type"] = None
    df_mid["rev_code"] = None
    df_mid["local_code"] = None
    df_mid["code"] = None
    df_mid["ms_drg"] = None
    df_mid["apr_drg"] = None
    df_mid["eapg"] = None
    df_mid["hcpcs_cpt"] = None
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
    df_mid["contracting_method"] = "other"
    df_mid["additional_generic_notes"] = "Service Bundle"
    df_mid["additional_payer_specific_notes"] = None

    if not "setting" in df_mid.columns.to_list():
        df_mid["setting"] = None

    df_mid = df_mid[df_mid["standard_charge"].notnull()]

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out


def get_input_dataframe(session, url, sheet_name):
    filename = derive_filename_from_url(url)
    download_file(session, url, filename)

    try:
        df_in = pd.read_excel(
            filename, skiprows=1, sheet_name=sheet_name, engine="openpyxl"
        )
        print(df_in)
    except:
        # Some files don't have the Service Bundle sheet.
        return None

    return df_in


TASKS = {
    "160124": "https://www.avera.org/app/files/public/e920ec8c-5f0b-480e-b0dd-8644ed571c8d/426037582_lakes-regional-hospital_standardcharges.xlsx",
    "161336": "https://www.avera.org/app/files/public/ecaf1876-8445-4bac-a539-746aa4a50654/420932564_hegg-health-center_standardcharges.xlsx",
    "161345": "https://www.avera.org/app/files/public/42b8b0dd-2ecb-46d2-9db1-7a01f0e75026/420890973_osceola-regional-realth-center_standardcharges.xlsx",
    "161368": "https://www.avera.org/app/files/public/93140804-fd16-463c-abaa-43505e2a6f31/420928451_floyd-valley-healthcare_standardcharges.xlsx",
    "241374": "https://www.avera.org/app/files/public/a8a06bca-7cff-40aa-a6c4-70f04224d16a/411392082_pipestone-county-medical-ctr_standardcharges.xlsx",
    "431306": "https://www.avera.org/app/files/public/84e67da3-faca-4710-9f74-865cf7e5a9ed/460239781_platte-healthcare_standardcharges.xlsx",
    "431326": "https://www.avera.org/app/files/public/4ac8cf50-4896-4620-b659-a2813c7e36fa/460224743_avera-milbank-hospital_standardcharges.xlsx",
    "431327": "https://www.avera.org/app/files/public/55aa22ce-08b4-47ff-b943-f9feb5199b85/460225414_st.-michaels-hospital_standardcharges.xlsx",
    "161346": "https://www.avera.org/app/files/public/6621903d-6293-41f9-a9fe-104587a689bf/420796764_sioux-center-health_standardcharges.xlsx",
    "161321": "https://www.avera.org/app/files/public/887b57a5-04fc-4a68-9636-d6f0d9b2dfab/460224743_avera-merrill-pioneer_standardcharges.xlsx",
    "161351": "https://www.avera.org/app/files/public/188febb6-f608-4630-877b-eeab98316fa3/420680370_avera-holy-family-hospital_standardcharges.xlsx",
    "241343": "https://www.avera.org/app/files/public/84e6c5b3-a05c-41ab-bc99-9e7cdecc6698/843156881_avera-granite-falls-health-center_standardcharges.xlsx",
    "241348": "https://www.avera.org/app/files/public/c6bee50e-7514-4ee4-b280-65fa14a68c3b/410853163_avera-tyler-hospital_standardcharges.xlsx",
    "241359": "https://www.avera.org/app/files/public/b391414f-acc2-417e-a79f-28a1b5f26467/460380552_avera-marshall-hospital_standardcharges.xlsx",
    "281329": "https://www.avera.org/app/files/public/fd10ed16-edb3-4376-b5fb-597facee8de1/470463911_avera-st.-anthonys-hospital_standardcharges.xlsx",
    "281331": "https://www.avera.org/app/files/public/b98961ca-b15f-4795-ad88-54f6d3b5bfab/460225483_avera-creighton-hospital_standardcharges.xlsx",
    "430012": "https://www.avera.org/app/files/public/acc8f842-8881-42f7-a5d3-3f51053a2d6f/460225483_avera-sacred-heart-hospital_standardcharges.xlsx",
    "430013": "https://www.avera.org/app/files/public/bc8a4b94-6096-41ff-87df-897ba9e95a2a/460224604_avera-queen-of-peace_standardcharges.xlsx",
    "430014": "https://www.avera.org/app/files/public/2b16bcfc-5575-4bc0-8f9b-2adcbc497f74/460224598_avera-st.-lukes_standardcharges.xlsx",
    "430015": "https://www.avera.org/app/files/public/660d6fed-090c-4637-9d3d-fda1e5f4b339/460230199_avera-st.-marys-hospital_standardcharges.xlsx",
    "430016": "https://www.avera.org/app/files/public/07f4da97-92e8-4e47-afe6-45df7189c896/460024743_avera-mcKennan-hospital_standardcharges.xlsx",
    "430095": "https://www.avera.org/app/files/public/1e15639c-7810-4899-ad0f-9368f892df0c/562143771_avera-heart-hospital_standardcharges.xlsx",
    "431302": "https://www.avera.org/app/files/public/08b43fe0-f7e6-4955-86e4-a110149167c6/460234354_avera-missouri-river-health-center_standardcharges.xlsx",
    "431310": "https://www.avera.org/app/files/public/652b2e59-7031-4d9c-abec-649dcf20f2ec/460224743_avera-flandreau-hospital_standardcharges.xlsx",
    "431324": "https://www.avera.org/app/files/public/7e400597-6472-47c0-a80b-9c649c997f69/460224604_avera-weskota-memorial-med-ctr_standardcharges.xlsx",
    "431330": "https://www.avera.org/app/files/public/b0514906-867f-4f82-b455-bdb5acc0a0ee/460226738_avera-st.-benedicts_standardcharges.xlsx",
    "431331": "https://www.avera.org/app/files/public/beefa3f3-2582-45c6-9acd-eb0bc724917b/460224743_avera-dell-rapids-hospital_standardcharges.xlsx",
    "431332": "https://www.avera.org/app/files/public/657353e3-2de5-40f2-b265-f0460302fa29/460224604_avera-desmet_standardcharges.xlsx",
    "431337": "https://www.avera.org/app/files/public/862efce2-7f11-4c2b-939e-5608a8356a89/460224743_avera-hand-county-hospital_standardcharges.xlsx",
    "431338": "https://www.avera.org/app/files/public/f6cb31b1-0cf3-4316-be33-ea0b2da7bea5/460224743_avera-gregory-hospital_standardcharges.xlsx",
}

TRANSPARENCY_PAGE_URL = "https://www.avera.org/patients-visitors/price-transparency/"


def main():
    session = create_session()

    for ccn in TASKS.keys():
        url = TASKS[ccn]
        print(ccn, url)

        filename = derive_filename_from_url(url)

        df_in1 = get_input_dataframe(session, url, "CDM")
        df_in2 = get_input_dataframe(session, url, "Service Bundle")

        df_out1 = convert_cdm_dataframe(df_in1, ccn)

        if df_in2 is not None:
            df_out2 = convert_bundle_dataframe(df_in2, ccn)
            df_out = pd.concat([df_out1, df_out2])
        else:
            df_out = df_out1

        df_out.to_csv("rate_" + ccn + ".csv", index=False, quoting=csv.QUOTE_MINIMAL)

    out_f = open("hospital.sql", "w")

    for ccn in TASKS.keys():
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        wb = openpyxl.load_workbook(filename)
        ws = wb["CDM"]
        first_cell_value = ws.cell(row=1, column=1).value
        date_str = first_cell_value.replace("Prices Effective ", "")
        last_updated_at = parse_datetime(date_str).isoformat().split("T")[0]

        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
            ein, last_updated_at, filename, url, TRANSPARENCY_PAGE_URL, ccn
        )

        out_f.write(query)
        out_f.write("\n")

    out_f.close()


if __name__ == "__main__":
    main()
