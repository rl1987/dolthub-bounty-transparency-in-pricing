#!/usr/bin/python3

import csv
import subprocess
from urllib.parse import urlparse

import pandas as pd
from dateutil.parser import parse as date_parse

from helpers import *

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
    'rate_category',
    'payer_name',
    'plan_name',
    'standard_charge',
    'standard_charge_percent',
    'contracting_method',
    'additional_generic_notes',
    'additional_payer_specific_notes'
]

TASKS = {
    "370099": "https://coc.ardenthealthservices.com/2023/oklahoma/201024792_Hillcrest-Hospital-Cushing_standardcharges.csv",
    "370228": "https://coc.ardenthealthservices.com/2023/oklahoma/201547519_Bailey-Medical-Center_standardcharges.csv"
}

TRANSPARENCY_PAGES = {
    "370099": "https://hillcrestcushing.com/pricing-transparency",
    "370228": "https://baileymedicalcenter.com/cost-care",
}

def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
    
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])
    df_in = pd.read_csv(filename, dtype=str, skiprows=[0])

    in_f = open(filename, "r")
    first_line = in_f.readline()
    in_f.close()

    date_str = first_line.split(" ")[-1].strip()
    last_updated = date_parse(date_str).isoformat().split("T")[0]
    
    return df_in, last_updated

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)
    df_mid = df_mid.rename(columns={
        'Procedure': 'code',
        'Code Type': 'line_type',
        'Revenue Code': 'rev_code',
        'Procedure Description': 'description',
        'Quantity': 'drug_quantity',
        'Payer': 'payer_name',
        'Plan': 'plan_name',
    })

    df_mid.loc[df_mid['rev_code'].notnull(), 'rev_code'] = df_mid[df_mid['rev_code'].notnull()]['rev_code'].apply(
        lambda rev_code: rev_code.split(' ')[0]
    )

    money_idx = df_mid.columns.to_list().index('Inpatient Gross Charges')
    money_columns = df_mid.columns.to_list()[money_idx:]

    remaining_columns = df_mid.columns.to_list()[:money_idx]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name2', value_name='standard_charge')

    df_mid['setting'] = None

    df_mid.loc[df_mid['payer_name2'].str.contains('Inpatient'), 'setting'] = 'inpatient'
    df_mid.loc[df_mid['payer_name2'].str.contains('Outpatient'), 'setting'] = 'outpatient'

    df_mid.loc[df_mid['payer_name2'].str.contains('Gross'), 'rate_category'] = 'gross'
    df_mid.loc[df_mid['payer_name2'].str.contains('Cash'), 'rate_category'] = 'cash'
    df_mid.loc[df_mid['payer_name2'].str.contains('Min'), 'rate_category'] = 'min'
    df_mid.loc[df_mid['payer_name2'].str.contains('Max'), 'rate_category'] = 'max'
    df_mid.loc[df_mid['payer_name2'].str.contains('Expected Reimbursement'), 'rate_category'] = 'negotiated'

    df_mid.loc[df_mid['rate_category'] != 'negotiated', 'payer_name'] = df_mid[df_mid['rate_category'] != 'negotiated']['payer_name2']
    df_mid.loc[df_mid['rate_category'] != 'negotiated', 'plan_name'] = None
    del df_mid['payer_name2']

    df_mid.loc[df_mid['line_type'] == 'DRG', 'ms_drg'] = df_mid[df_mid['line_type'] == 'DRG']['code'].str.replace('MS', '')

    df_mid['hospital_id'] = ccn
    df_mid['local_code'] = None
    df_mid['apr_drg'] = None
    df_mid['eapg'] = None
    df_mid['hcpcs_cpt'] = None
    df_mid['modifiers'] = None
    df_mid['alt_hcpcs_cpt'] = None
    df_mid['thru'] = None
    df_mid['apc'] = None
    df_mid['icd'] = None
    df_mid['ndc'] = None
    df_mid['drug_hcpcs_multiplier'] = None
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    df_mid['additional_generic_notes'] = None
    df_mid['additional_payer_specific_notes'] = None
    df_mid['billing_class'] = None
    df_mid['drug_unit_of_measurement'] = None
    df_mid['drug_type_of_measurement'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])
    
    return df_out

def main():
    out_f = open("hospitals.sql", "w")
    
    for ccn in TASKS.keys():
        url = TASKS[ccn]
        print(ccn, url)

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        df_in, last_updated = get_input_dataframe(url)

        print(df_in)

        df_out = convert_dataframe(df_in, ccn)

        print(df_out)

        df_out.to_csv('rate_' + ccn + '.csv', quoting=csv.QUOTE_MINIMAL, index=False)

        transparency_page = TRANSPARENCY_PAGES[ccn]
        
        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
            ein, last_updated, filename, url, transparency_page, ccn
        )

        out_f.write(query)
        out_f.write("\n")
        
    out_f.close()


if __name__ == "__main__":
    main()
