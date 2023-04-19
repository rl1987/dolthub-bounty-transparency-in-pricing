#!/usr/bin/python3

import subprocess
import sys
import os

from dateutil.parser import parse as parse_datetime
import doltcli as dolt
import pandas as pd

TARGET_COLUMNS = [ 
    'hospital_ccn',
    'hospital_ein',
    'code_type',
    'internal_code',
    'description',
    'code_orig',
    'code_prefix',
    'code',
    'modifier',
    'ndc',
    'ndc_num_hcpcs_doses',
    'ndc_quantity_desc',
    'apc',
    'rev_code',
    'rev_desc',
    'billing_class',
    'patient_class',
    'payer_category',
    'payer_orig',
    'payer_name',
    'plan_orig',
    'plan_name',
    'plan_id',
    'plan_type',
    'rate',
    'rate_method',
    'rate_desc',
    'filename',
    'file_last_updated',
    'url',
    'permalink'
]

def pad_rev_code_if_needed(rev_code):
    if type(rev_code) == str and rev_code != 'na':
        if len(rev_code) == 3:
            return '0' + rev_code
        elif len(rev_code) == 2:
            return '00' + rev_code
        elif len(rev_code) == 1:
            return '000' + rev_code

    return rev_code

def code_orig_to_code_prefix(code_orig):
    if code_orig.startswith('APR-DRG'):
        return 'apr-drg'
    elif code_orig.startswith('MS-DRG'):
        return 'ms-drg'

    return 'none'

def plan_type_from_payer_orig(payer_orig):
    if "PPO" in payer_orig:
        return "PPO"
    elif "HMO" in payer_orig:
        return "HMO"

    return None

def payer_category_from_payer_orig(payer_orig):
    if payer_orig == "Gross Charges" or payer_orig == "GROSS CHARGES":
        return 'gross'
    elif payer_orig == "Discounted Cash Price" or payer_orig == "SELF PAY CASH PRICE":
        return 'cash'
    elif payer_orig == "De-Identified Minimum" or payer_orig == "MIN NEGOTIATED RATE":
        return 'min'
    elif payer_orig == "De-Identified Maximum" or payer_orig == "MAX NEGOTIATED RATE":
        return 'max'

    return 'payer'


def patient_class_from_payer_orig(payer_orig):
    if "OP Rate" in payer_orig:
        return "outpatient"
    elif "IP Rate" in payer_orig:
        return "inpatient"

    return 'na'

def process_df_mid(df_mid, ccn, ein, url):
    df_mid['rev_code'] = df_mid['rev_code'].fillna('na')
    df_mid['rev_code'] = df_mid['rev_code'].apply(pad_rev_code_if_needed)

    df_mid['code_type'] = 'none'
    df_mid.loc[df_mid['cpt'].notnull(), 'code_type'] = 'cpt'
    df_mid.loc[df_mid['drg'].notnull(), 'code_type'] = 'drg'
    
    df_mid_cpt = pd.DataFrame(df_mid.loc[df_mid['code_type'] == 'cpt'])
    df_mid_cpt['code'] = df_mid['cpt']
    df_mid_cpt['code_prefix'] = 'hcpcs_cpt'

    df_mid_drg = pd.DataFrame(df_mid.loc[df_mid['code_type'] == 'drg'])
    df_mid_drg['code'] = df_mid_drg['drg']
    df_mid_drg['code_type'] = 'drg'

    def code_orig_to_code_prefix(code_orig):
        if code_orig.startswith('APR-DRG'):
            return 'apr-drg'
        elif code_orig.startswith('MS-DRG'):
            return 'ms-drg'

        return 'none'

    def pad_drg_if_needed(drg):
        if len(drg) == 1:
            return "00" + drg
        elif len(drg) == 2:
            return "0" + drg

        return drg

    df_mid_drg['code_type'] = 'drg'
    df_mid_drg['code_prefix'] = df_mid['code_orig'].apply(code_orig_to_code_prefix)
    df_mid_drg['code'] = df_mid_drg['drg'].apply(pad_drg_if_needed)

    df_mid_cdm = pd.DataFrame(df_mid.loc[df_mid['code_type'] == 'none'])
    df_mid_cdm['code_prefix'] = 'cdm'
    df_mid_cdm['code'] = df_mid_cdm['code_orig']
    df_mid_cdm['code_prefix'] = 'none'
    df_mid_cdm['code'] = 'na'

    df_mid = pd.concat([df_mid_cpt, df_mid_drg, df_mid_cdm])
    
    del df_mid['drg']
    if 'cpt' in df_mid.columns.to_list():
        del df_mid['cpt']
    df_mid['modifier'] = df_mid['modifier'].fillna('na')
    df_mid['modifier'] = df_mid['modifier'].apply(lambda modifier: 'na' if len(modifier.strip()) == 0 else modifier)
    df_mid['rev_code'] = df_mid['rev_code'].fillna('na')
    if 'ndc' in df_mid.columns:
        df_mid['ndc'] = df_mid['ndc'].fillna('na')
    else:
        df_mid['ndc'] = 'na'

    df_mid['payer_category'] = df_mid['payer_orig'].apply(payer_category_from_payer_orig)

    df_mid['patient_class'] = df_mid['payer_orig'].apply(patient_class_from_payer_orig)

    df_mid['billing_class'] = 'na'

    df_mid['plan_type'] = df_mid['payer_orig'].apply(plan_type_from_payer_orig)
    
    if 'file_last_updated' in df_mid.columns:
        df_mid['file_last_updated'] = df_mid['file_last_updated'].apply(
            lambda file_last_updated: parse_datetime(file_last_updated).isoformat().split("T")[0])
    df_mid['plan_orig'] = 'na'
    df_mid['hospital_ccn'] = ccn
    df_mid['hospital_ein'] = ein
    df_mid['filename'] = url.split("/")[-1]
    df_mid['url'] = url
    
    if 'internal_code' in df_mid.columns:
        df_mid['internal_code'] = df_mid['internal_code'].fillna('na')
    else:
        df_mid['internal_code'] = 'na'

    df_mid['rate'] = df_mid['rate'].apply(lambda rate: None if type(rate) == str and rate.startswith("ERROR") else rate)

    df_out = pd.DataFrame(columns=TARGET_COLUMNS)
    df_out = df_out.append(df_mid)
    df_out = df_out.dropna(subset=['rate'], axis=0)

    return df_out

def convert_df1(df_in, ccn, ein, url):
    df_mid = pd.DataFrame(df_in)
    df_mid = df_mid.rename(columns={
        'LINE TYPE': 'code_type',
        'CHARGE CODE/PACKAGE': 'code_orig',
        'CHARGE CODE/ PACKAGE': 'code_orig',
        'CHARGE DESCRIPTION': 'description',
        'DRG': 'drg',
        'CPT/HCPCS': 'cpt',
        'CPT': 'cpt',
        'MODIFIERS': 'modifier',
        'MODIFIER': 'modifier',
        'REV CODE': 'rev_code',
        'NDC': 'ndc',
    })

    money_columns = df_mid.columns.to_list()[8:]
    remaining_columns = df_mid.columns.to_list()[:8]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_orig', value_name='rate')

    return process_df_mid(df_mid, ccn, ein, url)

def convert_df2(df_in, ccn, ein, url):
    df_mid = pd.DataFrame(df_in)
    df_mid = df_mid.rename(columns={
        'Line Type': 'code_type',
        'As of Date': 'file_last_updated',
        'Line ID': 'internal_code',
        'Charge Code/Package': 'code_orig',
        'Charge Description': 'description',
        'DRG': 'drg',
        'CPT': 'cpt',
        'Patient Type': 'patient_class',
        'Modifiers': 'modifier',
        'Rev Code': 'rev_code',
    })

    money_columns = df_mid.columns.to_list()[10:]
    remaining_columns = df_mid.columns.to_list()[:10]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_orig', value_name='rate')

    return process_df_mid(df_mid, ccn, ein, url)

def convert_df(ccn, ein, url):
    filename = url.split("/")[-1]
    ein = ein[:2] + "-" + ein[2:]

    target_filename = "steward_" + ccn + ".csv"
    if os.path.isfile(target_filename):
        print("Output file already present - skipping")
        return

    subprocess.run(["wget", "--no-clobber", url, "-O", filename])
    
    df_in = pd.read_csv(filename, dtype={'REV CODE': str, 'Rev Code': str, 'DRG': str},
                        encoding='latin-1', low_memory=False)

    df_out = None
    if df_in.columns.to_list()[0] == "LINE TYPE":
        #df_out = convert_df1(df_in, ccn, ein, url)
        return
    elif df_in.columns.to_list()[0] == "As of Date":
        df_out = convert_df2(df_in, ccn, ein, url)
    else:
        print("Don't know how to process: {}".format(url))
        return

    df_out.to_csv(target_filename, index=False)

def main():
    db = dolt.Dolt(sys.argv[1])

    sql = 'SELECT ccn, tin, standard_charge_file_url FROM hospitals WHERE standard_charge_file_url LIKE "%steward.org%";'

    res = db.sql(sql, result_format="json")
    for row in res["rows"]:
        ccn = row.get("ccn")
        url = row.get("standard_charge_file_url")
        ein = row.get("tin")
        print(ccn, url)
        
        convert_df(ccn, ein, url)
        

if __name__ == "__main__":
    main()
