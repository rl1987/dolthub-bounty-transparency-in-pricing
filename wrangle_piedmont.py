#!/usr/bin/python3

import subprocess

from dateutil.parser import parse as parse_datetime
import pandas as pd
import numpy as np

from helpers import derive_ein_from_filename, derive_filename_from_url

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

def payer_category_from_payer_name(payer_name):
    if payer_name == "Charges":
        return "gross"
    elif payer_name == "Self-pay":
        return "cash"
    elif payer_name == "Min Fee":
        return "min"
    elif payer_name == "Max Fee":
        return "max"

    return "payer"

def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])

    df_in = pd.read_excel(filename, skiprows=[1,2,3])

    # HACK!!!
    d = df_in.iloc[0].to_dict()
    k = list(d.keys())[0]
    columns = d[k].split(',')

    df_in = df_in.iloc[1:] 
    records = df_in[k].to_list()
    records = list(map(lambda r: r.split(","), records))

    def fix_record_list(r):
        while len(r) > len(columns):
            r[1] = r[1] + "," + r[2]
            del r[2]
    
        return r

    records = list(map(lambda r: fix_record_list(r), records))

    records = list(map(lambda r: dict(zip(columns, r)), records))
    df_in = pd.DataFrame(records)
    df_in = df_in.replace("N/A", np.NaN) # https://stackoverflow.com/a/34794112
    
    return df_in

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid = df_mid.rename(columns={
        'Procedure': 'local_code',
        'Description': 'description',
        'HCPCS Code': 'hcpcs_cpt',
        'MSDRG': 'ms_drg',
        'Rev Code': 'rev_code',
        'NDC': 'ndc',
        'Service': 'setting',
    })

    money_idx = df_mid.columns.to_list().index('Charges')
    money_columns = df_mid.columns[money_idx:]
    remaining_columns = df_mid.columns[:money_idx]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    if 'setting' in df_mid.columns:
        df_mid['additional_generic_notes'] = df_mid['setting']
        df_mid['setting'] = None
        df_mid.loc[df_mid['additional_generic_notes'].str.startswith('Inpatient'), 'setting'] = 'inpatient'
        df_mid.loc[df_mid['additional_generic_notes'].str.startswith('Outpatient'), 'setting'] = 'outpatient'
    
    df_mid.loc[df_mid['ms_drg'].notnull(), 'ms_drg'] = df_mid[df_mid['ms_drg'].notnull()]['ms_drg'].str.replace('MS', '')
    df_mid.loc[df_mid['rev_code'].notnull(), 'rev_code'] = df_mid[df_mid['rev_code'].notnull()]['rev_code'].str.zfill(4)
    df_mid.loc[df_mid['hcpcs_cpt'].notnull(), 'hcpcs_cpt'] = df_mid[df_mid['hcpcs_cpt'].notnull()]['hcpcs_cpt'].str.upper()

    df_mid['payer_category'] = df_mid['payer_name'].apply(payer_category_from_payer_name)

    df_mid.dropna(subset=['standard_charge'], inplace=True)

    df_mid['hospital_id'] = ccn
    df_mid['line_type'] = None
    df_mid['code'] = None
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
    if not 'setting' in df_mid.columns:
        df_mid['setting'] = None
    df_mid['plan_name'] = None
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    if not 'additional_generic_notes' in df_mid.columns:
        df_mid['additional_generic_notes'] = None
    df_mid['additional_payer_specific_notes'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out

TRANSPARENCY_PAGE = "https://www.piedmont.org/patient-tools/ContentPage.aspx?nd=15973"

TASKS = {
    "110083": "https://www.piedmont.org/media/file/580566213_piedmont-atlanta-hospital_standardcharges.xls",
    "110074": "https://www.piedmont.org/media/file/582179986_piedmont-athens-hospital_standardcharges.xls",
    "110064": "https://www.piedmont.org/media/file/581685139_piedmont-columbus-regional-midtown-hospital_standardcharges.xls",
    "110200": "https://www.piedmont.org/media/file/331216751_piedmont-columbus-regional-northside-hospital_standardcharges.xls",
    "110215": "https://www.piedmont.org/media/file/582322328_piedmont-fayette-hospital_standardcharges.xls",
    "110191": "https://www.piedmont.org/media/file/582200195_piedmont-henry-hospital_standardcharges.xls",
    "110225": "https://www.piedmont.org/media/file/352228583_piedmont-mountainside-hospital_standardcharges.xls",
    "110229": "https://www.piedmont.org/media/file/205077249_piedmont-newnan-hospital_standardcharges.xls",
    "110018": "https://www.piedmont.org/media/file/582155150_piedmont-newton-hospital_standardcharges.xls",
    "110091": "https://www.piedmont.org/media/file/300999841_piedmont-rockdale-hospital_standardcharges.xls",
    "110046": "https://www.piedmont.org/media/file/824194264_piedmont-walton-hospital_standardcharges.xls"
}

def main():
    out_f = open("hospital.sql", "w")

    for ccn in TASKS.keys():
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)
        
        df_in = get_input_dataframe(url)
        print(df_in)

        try:
            df_tmp = pd.read_excel(filename)
            last_updated = df_tmp.iloc[1][0]
            last_updated = last_updated.split(' ')[-1]
            last_updated = parse_datetime(last_updated).isoformat().split("T")[0]
        except:
            last_updated = "2021-01-01"
        
        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
                ein, last_updated, filename, url, TRANSPARENCY_PAGE, ccn)

        out_f.write(query)
        out_f.write("\n")

        df_out = convert_dataframe(df_in, ccn)
        print(query)
        print(df_out)

        df_out.to_csv("rate_" + ccn + ".csv", index=False)

    out_f.close()

if __name__ == "__main__":
    main()

