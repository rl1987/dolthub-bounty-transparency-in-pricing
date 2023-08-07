#!/usr/bin/python3

import subprocess
import json

from dateutil.parser import parse as parse_datetime
import pandas as pd
import numpy as np

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
    "rate_category",
    "payer_name",
    "plan_name",
    "standard_charge",
    "standard_charge_percent",
    "contracting_method",
    "additional_generic_notes",
    "additional_payer_specific_notes",
]

TRANSPARENCY_PAGE = "https://www.mskcc.org/insurance-assistance/understanding-cost-your-care-msk/download"
CCN = "330154"
EIN = "13-1624082"

def rearrange_columns(df):
    columns = df.columns.to_list()

    new_columns = list(filter(lambda c: "CONTRACTING METHOD" in c, columns))
    new_columns += list(filter(lambda c: not "CONTRACTING METHOD" in c, columns)) 

    df = df.reindex(columns=new_columns)

    return df

def fix_modifiers(modifiers):
    if type(modifiers) == int:
        modifiers = str(modifiers)
        
    modifiers = modifiers.strip()
    if modifiers == '':
        return None

    modifiers = modifiers.replace(" ", "|")
    modifiers = modifiers.replace(",", "|")

    return modifiers

def payer_name_to_rate_category(payer_name):
    if payer_name.startswith("MAX"):
        return 'max'
    elif payer_name.startswith("MIN"):
        return 'min'
    elif payer_name == "PRICE":
        return 'gross'
    elif payer_name == "SELF PAY":
        return 'cash'

    return 'negotiated'

def set_contracting_method(row):
    if row.get('rate_category') != "negotiated":
        return row
    
    method_by_payer = dict()
    
    for col_name in dict(row).keys():
        value = row[col_name]
        if type(value) != str:
            continue
        
        if 'CONTRACTING METHOD' in col_name:
            payer = col_name.replace("CONTRACTING METHOD", "")
            method_by_payer[payer] = value
    
    payer_name = row.get('payer_name')

    for payer in method_by_payer.keys():
        if not payer in payer_name:
            continue

        method = method_by_payer[payer]
        if method == 'FEE SCHEDULE':
            row['contracting_method'] = 'fee schedule'
        elif method == 'PERCENT OF CHARGE':
            row['contracting_method'] = 'percent of total billed charge'
        elif method == 'CASE RATE':
            row['contracting_method'] = 'case rate'
        elif method == 'PER DIEM':
            row['contracting_method'] = 'per diem'
        else:
            row['contracting_method'] = 'other'
            row['additional_payer_specific_notes'] = method

    return row

def split_code(row):
    code = row.get('code')

    if code_is_cpt(code) or code_is_hcpcs(code):
        row['hcpcs_cpt'] = code
    elif code_is_ms_drg(code):
        row['ms_drg'] = code

    return row

def fix_rev_code(rev_code):
    if rev_code is None or type(rev_code) == float and np.isnan(rev_code):
        return None

    # 260, 940
    if "," in rev_code:
        rev_code = rev_code.split(",")[0]

    # 450-459
    if "-" in rev_code:
        rev_code = rev_code.split("-")[0]

    rev_code = pad_rev_code_if_needed(rev_code)

    return rev_code

# 96360-5, C8957
def fix_hcpcs_cpt(row):
    hcpcs_cpt = row.get('hcpcs_cpt')
    if not type(hcpcs_cpt) == str:
        return row

    if "," in hcpcs_cpt:
        codes = hcpcs_cpt.split(",")
        row['hcpcs_cpt'] = codes[0].strip()
        row['alt_hcpcs_cpt'] = codes[1].strip()

    hcpcs_cpt = row.get('hcpcs_cpt')
    if "-" in hcpcs_cpt:
        hcpcs_cpt, mod = hcpcs_cpt.split("-")
        mod = mod.strip()
        row['hcpcs_cpt'] = hcpcs_cpt
        if len(mod) == 2:
            if row.get('modifiers') is None:
                row['modifiers'] = mod
            else:
                row['modifiers'] = row['modifiers'] + "|" + mod

    return row

def convert_dataframe(df_in, setting, billing_class):
    df_mid = pd.DataFrame(df_in)

    df_mid = rearrange_columns(df_mid)
    df_mid = df_mid.rename(columns={
        'CHG CD': 'local_code',
        'CHG CD DESC': 'description',
        'MCD CPT': 'hcpcs_cpt',
        'MOD': 'modifiers',
        'REV CD': 'rev_code',
        'MCD CPT/MS DRG': 'code',
        'Description': 'description',
        'CPT Code': 'hcpcs_cpt',
        'Mod': 'modifiers'
    })

    money_idx = df_mid.columns.to_list().index("PRICE")
    money_columns = df_mid.columns.to_list()[money_idx:]

    remaining_columns = df_mid.columns.to_list()[:money_idx]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    df_mid['standard_charge_percent'] = None

    df_mid.loc[df_mid['payer_name'].str.endswith('PERCENT'), 'standard_charge_percent'] = df_mid[df_mid['payer_name'].str.endswith('PERCENT')]['standard_charge']
    df_mid.loc[df_mid['payer_name'].str.endswith('PERCENT'), 'standard_charge'] = None

    df_mid.loc[df_mid['modifiers'].notnull(), 'modifiers'] = df_mid[df_mid['modifiers'].notnull()]['modifiers'].apply(fix_modifiers)

    df_mid['rate_category'] = df_mid['payer_name'].apply(payer_name_to_rate_category)

    df_mid['contracting_method'] = None
    df_mid['additional_payer_specific_notes'] = None

    df_mid = df_mid.apply(set_contracting_method, axis=1)

    df_mid = df_mid[df_mid['standard_charge'].notnull() | df_mid['standard_charge_percent'].notnull()]
    df_mid = df_mid[df_mid['standard_charge'] != 'MEDICAID RATE']

    df_mid.loc[df_mid['standard_charge_percent'].notnull(), 'standard_charge_percent'] = df_mid[df_mid['standard_charge_percent'].notnull()]['standard_charge_percent'].apply(
        lambda percent: percent.replace("%", "").strip()
    )

    df_mid['standard_charge'] = df_mid['standard_charge'].apply(cleanup_dollar_value)
    
    if 'code' in df_mid.columns:
        df_mid.loc[df_mid['code'].isnull(), 'code'] = ''
        df_mid['code'] = df_mid['code'].astype(str)
        df_mid = df_mid.apply(split_code, axis=1)
        df_mid.loc[df_mid['code'] == '', 'code'] = None

    if not 'rev_code' in df_mid.columns:
        df_mid['rev_code'] = None

    df_mid['rev_code'] = df_mid['rev_code'].apply(fix_rev_code)
    df_mid.loc[df_mid['rev_code'] == 'Other IP', 'rev_code'] = None

    df_mid['alt_hcpcs_cpt'] = None

    df_mid = df_mid.apply(fix_hcpcs_cpt, axis=1)
    
    df_mid['hospital_id'] = CCN
    df_mid['line_type'] = None
    if not 'code' in df_mid.columns:
        df_mid['code'] = None
    if not 'hcpcs_cpt' in df_mid.columns:
        df_mid['hcpcs_cpt'] = None
    if not 'ms_drg' in df_mid.columns:
        df_mid['ms_drg'] = None
    df_mid['apr_drg'] = None
    df_mid['eapg'] = None
    if not 'alt_hcpcs_cpt' in df_mid.columns:
        df_mid['alt_hcpcs_cpt'] = None
    df_mid['thru'] = None
    df_mid['apc'] = None
    df_mid['icd'] = None
    df_mid['ndc'] = None
    df_mid['drug_hcpcs_multiplier'] = None
    df_mid['drug_quantity'] = None
    df_mid['drug_unit_of_measurement'] = None
    df_mid['drug_type_of_measurement'] = None
    df_mid['billing_class'] = billing_class
    df_mid['setting'] = setting
    df_mid['plan_name'] = None
    df_mid['additional_generic_notes'] = None
    if not 'additional_payer_specific_notes' in df_mid.columns:
        df_mid['additional_payer_specific_notes'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out

def main():
    urls = ["https://www.mskcc.org/teaser/standard-charges-nyc.json", 
        "https://www.mskcc.org/teaser/standard-charges-ny-suburbs.json",
        "https://www.mskcc.org/teaser/standard-charges-sourthern-nj.json",
        "https://www.mskcc.org/teaser/standard-charges-northern-nj.json"]

    output_dfs = []

    last_updated = None
    
    for url in urls:
        print(url)
        filename = derive_filename_from_url(url)
        subprocess.run(["wget", "--no-clobber", url, "-O", filename])

        in_f = open(filename, "r")
        json_dict = json.load(in_f)
        in_f.close()
        
        for key in json_dict.keys():
            setting = None
            billing_class = None
            
            if key.startswith("IP"):
                setting = 'inpatient'
                billing_class = "facility"
            elif key.startswith("OP"):
                setting = 'outpatient'
                billing_class = "facility"
            elif key.startswith("PHY"):
                billing_class = "professional"
            elif key == 'File Notes':
                if last_updated is not None:
                    continue
                    
                date_str = json_dict['File Notes'][-1]['Memorial Hospital for Cancer and Allied Diseases']
                date_str = date_str.split(" ")[-1]
                last_updated = parse_datetime(date_str).isoformat().split("T")[0]
                continue
            else:
                print(key)
                print(json_dict[key])
                continue

            df_in = pd.DataFrame(json_dict[key])
            print(key)
            print(df_in)

            df_out = convert_dataframe(df_in, setting, billing_class)

            print(df_out)

            output_dfs.append(df_out)

    df_out = pd.concat(output_dfs)

    df_out.to_csv('rate_' + CCN + '.csv', index=False)

    out_f = open("hospital.sql", "w")

    filenames = list(map(lambda url: derive_filename_from_url(url), urls))
    filenames = "|".join(filenames)

    urls = "|".join(urls)

    query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
        EIN, last_updated, filenames, url, TRANSPARENCY_PAGE, CCN
    )

    out_f.write(query)
    out_f.write("\n")

if __name__ == "__main__":
    main()
    


