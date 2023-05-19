#!/usr/bin/python3

import csv
import subprocess

import pandas as pd
from dateutil.parser import parse as parse_datetime

from helpers import derive_ein_from_filename, derive_filename_from_url
from helpers import cleanup_dollar_value

TARGET_COLUMNS = [
    'hospital_id',
    #'row_id',
    'line_type',
    'description',
    'rev_code',
    'local_code',
    'code',
    'ms_drg',
    'apr_drg',
    'eapg',
    'hcpcs_cpt',
    'modifiers',
    'alt_hcpcs_cpt',
    'thru',
    'apc',
    'icd',
    'ndc',
    'drug_hcpcs_multiplier',
    'drug_quantity',
    'drug_unit_of_measurement',
    'drug_type_of_measurement',
    'billing_class',
    'setting',
    'payer_category',
    'payer_name',
    'plan_name',
    'standard_charge',
    'standard_charge_percent',
    'contracting_method',
    'additional_generic_notes',
    'additional_payer_specific_notes'
]

def setting_from_payer_name(payer_name):
    if "INPATIENT" in payer_name:
        return 'inpatient'
    elif "OUTPATIENT" in payer_name:
        return 'outpatient'

    return None

def payer_category_from_payer_name(payer_name):
    if payer_name == "Gross Charge":
        return 'gross'
    elif payer_name == "DISCOUNTED CASH PRICE INPATIENT" or payer_name == "DISCOUNTED CASH PRICE OUTPATIENT":
        return 'cash'
    elif payer_name == "DE-IDENTIFIED MIN":
        return 'min'
    elif payer_name == "DE-IDENTIFIED MAX":
        return 'max'

    return 'payer'

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)
    del df_mid['As of Date']

    df_mid = df_mid.rename(columns={
        'SVCCD': 'local_code',
        'Description': 'description',
        'CPT': 'hcpcs_cpt',
    })

    money_columns = df_mid.columns.to_list()[3:]
    remaining_columns = df_mid.columns.to_list()[:3]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    df_mid['standard_charge'] = df_mid['standard_charge'].apply(cleanup_dollar_value)

    df_mid['hospital_id'] = ccn
    df_mid['line_type'] = None
    df_mid['rev_code'] = None
    df_mid['code'] = None

    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].astype(str)

    df_mid.loc[df_mid['local_code'] == 'DRG', 'ms_drg'] = df_mid[df_mid['local_code'] == 'DRG']['hcpcs_cpt'].str.zfill(3)
    df_mid['ms_drg'] = df_mid['ms_drg'].apply(lambda ms_drg: ms_drg.replace(".00", "").zfill(3) if type(ms_drg) == str and ms_drg.endswith(".00") else ms_drg)

    # HACK for Ponca city hospital.
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].apply(lambda cpt: cpt.replace(".00", "") if type(cpt) == str and cpt.endswith(".00") else cpt)
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].apply(lambda cpt: None if type(cpt) == str and len(cpt) < 5 and cpt.isnumeric() else cpt)
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].apply(lambda cpt: cpt.replace(".00", "") if type(cpt) == str and cpt.endswith(".00") else cpt)
    df_mid.loc[df_mid['hcpcs_cpt'] == '0', 'hcpcs_cpt'] = None
    
    df_mid['local_code'] = df_mid['local_code'].fillna('')
    df_mid.loc[~df_mid['local_code'].str.isnumeric(), 'line_type'] = df_mid[~df_mid['local_code'].str.isnumeric()]['local_code']
    df_mid['local_code'] = df_mid['local_code'].apply(
        lambda local_code: local_code.replace(".00", "") if type(local_code) == str and local_code.endswith(".00") else local_code
    )
    df_mid.loc[~df_mid['local_code'].str.isnumeric(), 'local_code'] = None

    df_mid.loc[df_mid['hcpcs_cpt'] == 'SURG', 'hcpcs_cpt'] = None
    df_mid.loc[df_mid['hcpcs_cpt'] == 'MANUL', 'hcpcs_cpt'] = None
    df_mid.loc[df_mid['hcpcs_cpt'] == 'nan', 'hcpcs_cpt'] = None

    df_mid['apr_drg'] = None
    df_mid['eapg'] = None
    df_mid['modifiers'] = None
    df_mid['alt_hcpcs_cpt'] = None
    df_mid['thru'] = None
    df_mid['apc'] = None
    df_mid['icd'] = None
    df_mid['ndc'] = None
    df_mid['drug_hcpcs_multiplier'] = None
    df_mid['drug_quantity'] = None
    df_mid['drug_unit_of_measurement'] = None
    df_mid['drug_type_of_measurement'] = None
    df_mid['billing_class'] = None

    df_mid['setting'] = df_mid['payer_name'].apply(setting_from_payer_name)
    
    df_mid['payer_category'] = df_mid['payer_name'].apply(payer_category_from_payer_name)
    
    df_mid['plan_name'] = None
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    df_mid['additional_generic_notes'] = None
    df_mid['additional_payer_specific_notes'] = None

    df_mid = df_mid.dropna(subset=['standard_charge'])
    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])
    
    return df_out

def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])

    df_in = pd.read_csv(filename, dtype={'CPT': str, 'SVCCD': str})

    return df_in

def main():
    df_tasks = pd.read_csv('chs_tasks.csv', dtype={'ccn': str})
    tasks = df_tasks.to_dict('records')

    out_f = open("hospital.sql", "w")

    for task in tasks:
        ccn = task.get('ccn')
        url = task.get('mrf_urls')
        transparency_page = task.get('transparency_page')

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)
        
        df_in = get_input_dataframe(url)
        print(df_in)

        try:
            last_updated = df_in['As of Date'].to_list()[0]
            last_updated = parse_datetime(last_updated).isoformat().split("T")[0]
        except:
            last_updated = "2021-01-01"
        
        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
                ein, last_updated, filename, url, transparency_page, ccn)

        out_f.write(query)
        out_f.write("\n")

        df_out = convert_dataframe(df_in, ccn)
        print(query)
        print(df_out)

        df_out.to_csv("rate_" + ccn + ".csv", index=False)

    out_f.close()

if __name__ == "__main__":
    main()
