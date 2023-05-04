#!/usr/bin/python3

import csv
import subprocess
from urllib.parse import urlparse

import pandas as pd

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
    'additional_payer_notes',
    'additional_generic_notes',
    'additional_payer_specific_notes'
]

def derive_ein_from_filename(filename):
    ein = filename.split("_")[0]
    ein = ein[:2] + "-" + ein[2:]
    return ein

def derive_filename_from_url(url):
    o = urlparse(url)
    filename = o.path.split("/")[-1]
    return filename

def payer_category_from_payer(payer):
    if payer == 'GROSS CHARGE' or payer == 'RECONSTRUCTED CHARGE':
        return 'gross'
    elif payer == 'CASH PRICE':
        return 'cash'
    elif payer == 'DEIDENTIFIED MIN' or payer == 'Min Price':
        return 'min'
    elif payer == 'DEIDENTIFIED MAX' or payer == 'Max Price':
        return 'max'
    
    return 'payer'

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid.columns = list(map(lambda c: c.strip(), df_mid.columns.to_list()))
    df_mid = df_mid.rename(columns={
        'Procedure': 'local_code',
        'Code Type': 'line_type',
        'Code': 'code',
        'NDC': 'ndc',
        'CPT/HCPCS Code': 'hcpcs_cpt',
        'Beaumont Health Charge Code Description': 'description',
        'Rev Code': 'rev_code',
        'REV CODE': 'rev_code',
        'Procedure Description': 'description',
        'FS ID': 'local_code'
    })

    if 'GROSS CHARGE' in df_mid.columns.to_list():
        money_idx = df_mid.columns.to_list().index('GROSS CHARGE')
    elif 'RECONSTRUCTED CHARGE' in df_mid.columns.to_list():
        money_idx = df_mid.columns.to_list().index('RECONSTRUCTED CHARGE')
    
    money_columns = df_mid.columns.to_list()[money_idx:]
    remaining_columns = df_mid.columns.to_list()[:money_idx]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    df_mid['payer_name'] = df_mid['payer_name'].str.strip()
    
    df_mid['standard_charge'] = df_mid['standard_charge'].astype(str)
    df_mid['standard_charge'] = df_mid['standard_charge'].apply(cleanup_dollar_value)
    df_mid = df_mid[df_mid['standard_charge'] != 'nan']
    df_mid = df_mid[df_mid['standard_charge'] != '#VALUE!']
    df_mid = df_mid[df_mid['standard_charge'] != '#N/A']
    df_mid = df_mid[df_mid['standard_charge'] != '-']
    df_mid = df_mid[df_mid['standard_charge'] != '!']
    df_mid = df_mid.dropna(subset=['standard_charge'], axis=0)
    
    df_mid['additional_payer_specific_notes'] = None
    df_mid['contracting_method'] = None
    df_mid.loc[df_mid['standard_charge'].str.isalpha(), 'contracting_method'] = 'other'
    df_mid.loc[df_mid['standard_charge'].str.isalpha(), 'additional_payer_specific_notes'] = df_mid[df_mid['standard_charge'].str.isalpha()]['standard_charge']
    df_mid.loc[df_mid['standard_charge'].str.isalpha(), 'standard_charge'] = None
    
    if not 'line_type' in df_mid.columns:
        df_mid['line_type'] = None
    
    df_mid['eapg'] = None
    df_mid['ms_drg'] = None
    
    if 'code' in df_mid.columns:
        df_mid.loc[df_mid['line_type'] == 'DRG', 'ms_drg'] = df_mid[df_mid['line_type'] == 'DRG']['code'].apply(
            lambda code: code.split(" ")[-1]
        )
        df_mid.loc[df_mid['line_type'] == 'EAP', 'eapg'] = df_mid[df_mid['line_type'] == 'EAP']['code'].astype(str)
    
        df_mid['code'] = df_mid['code'].fillna('')
        df_mid['code'] = df_mid['code'].apply(lambda code: str(int(code)) if type(code) == int or type(code) == float else code)
    
        if not 'hcpcs_cpt' in df_mid.columns:
            df_mid['hcpcs_cpt'] = df_mid['code'].apply(lambda code: code if len(code) == 5 else None)
        
        df_mid.loc[df_mid['code'] == '', 'code'] = None
    else:
        df_mid['code'] = None
        
    if not 'hcpcs_cpt' in df_mid.columns:
        df_mid['hcpcs_cpt'] = None
    
    df_mid = df_mid[df_mid['hcpcs_cpt'] != 'deleted']
        
    df_mid['payer_category'] = df_mid['payer_name'].apply(payer_category_from_payer)

    df_mid['plan_name'] = None

    df_mid['hospital_id'] = ccn
    df_mid['apr_drg'] = None
    df_mid['modifiers'] = None
    df_mid['thru'] = None
    df_mid['apc'] = None
    df_mid['icd'] = None
    df_mid['drug_hcpcs_multiplier'] = None
    df_mid['drug_type_of_measurement'] = None
    df_mid['drug_quantity'] = None
    df_mid['drug_unit_of_measurement'] = None
    df_mid['billing_class'] = None
    
    if not 'setting' in df_mid.columns:
        df_mid['setting'] = None
        
    df_mid.loc[df_mid['setting'] == 'Office Clinic Provider Fee', 'setting'] = 'inpatient'
    df_mid.loc[df_mid['setting'] == 'Hospital Outpatient Department Provider Fee', 'setting'] = 'outpatient'
        
    df_mid['additional_payer_notes'] = None
    df_mid['additional_generic_notes'] = None
    df_mid['standard_charge_percent'] = None
    
    df_mid['additional_generic_notes'] = df_mid['eapg'].apply(
        lambda eapg: 'Custom code marked as EAP' if type(eapg) == str and eapg.startswith('Custom') else None
    )
    
    df_mid.loc[df_mid['additional_generic_notes'] == 'Custom code marked as EAP', 'eapg'] = None
    
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].apply(lambda cpt: cpt.replace(" ", "") if type(cpt) == str else cpt)
    
    df_mid['additional_generic_notes'] = df_mid.apply(
        lambda row: 'Truncated hcpcs_cpt' if row['hcpcs_cpt'] is not None and len(row['hcpcs_cpt']) > 5 else row['additional_generic_notes'], axis=1
    )
    
    df_mid.loc[df_mid['additional_generic_notes'] == 'Truncated hcpcs_cpt', 'hcpcs_cpt'] = df_mid[df_mid['additional_generic_notes'] == 'Truncated hcpcs_cpt']['hcpcs_cpt'].str[:5]
    
    df_mid['additional_generic_notes'] = df_mid.apply(
        lambda row: 'Truncated eapg' if type(row['eapg']) == str and len(row['eapg']) > 5 else row['additional_generic_notes'], 
        axis=1
    )
    
    df_mid.loc[df_mid['additional_generic_notes'] == 'Truncated eapg', 'eapg'] = df_mid[df_mid['additional_generic_notes'] == 'Truncated eapg']['eapg'].str[:5]
    
    df_mid.loc[df_mid['hcpcs_cpt'] == 'RN001' ,'additional_generic_notes'] = 'Invalid hcpcs_cpt: RN001'
    df_mid.loc[df_mid['hcpcs_cpt'] == 'RN001' ,'code'] = 'RN001'

    df_mid.loc[df_mid['hcpcs_cpt'] == 'RN001' ,'hcpcs_cpt'] = None
    
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'additional_generic_notes'] = 'Invalid hcpcs_cpt: 1992'
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'code'] = '1992'
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'hcpcs_cpt'] = None
    
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'additional_generic_notes'] = 'Invalid hcpcs_cpt: 1992'
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'code'] = '1992'
    df_mid.loc[df_mid['hcpcs_cpt'] == '1992' ,'hcpcs_cpt'] = None
    
    
    if 'local_code' in df_mid.columns:
        df_mid['local_code'] = df_mid['local_code'].str[:40]
    else:
        df_mid['local_code'] = None

    if not 'rev_code' in df_mid.columns:
        df_mid['rev_code'] = None
    
    df_mid['ndc'] = df_mid['ndc'].fillna('')
    df_mid.loc[df_mid['ndc'].str.startswith("WBH"), 'ndc'] = None
    df_mid.loc[df_mid['ndc'] == '', 'ndc'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out

def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])

    skiprows=None
    if 'royal-oak' in url and not 'profession' in url:
        skiprows = 1

    df_in = pd.read_csv(filename, 
            encoding='latin-1', 
            skiprows=skiprows, 
            dtype={'Rev Code': str, 
                'REV CODE': str, 
                'Rev Code ': str,
                'CPT/HCPCS Code ': str,
                'Code': str}, 
            low_memory=False)

    return df_in

TASKS = {
    "230020": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-dearborn-hospital_standardcharges.csv?sfvrsn=bffa40fb_3&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-dearborn-professional_standardcharges.csv?sfvrsn=f8fb40fb_3&download=true"
    ],
    "230089": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-grosse-pointe-hospital_standardcharges.csv?sfvrsn=62fa40fb_5&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-grosse-pointe-professional_standardcharges.csv?sfvrsn=dfa40fb_3&download=true"
    ],
    "230130": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-royal-oak-hospital_standardcharges.csv?sfvrsn=5cfa40fb_7&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-royal-oak-professional_standardcharges.csv?sfvrsn=a1fb40fb_5&download=true",
    ],
    "230142": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-wayne-hospital_standardcharges.csv?sfvrsn=c6fa40fb_1&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-wayne-professional_standardcharges.csv?sfvrsn=efa40fb_3&download=true"
    ],
    "230151": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381426929_beaumont-hospital-farmington-hills-hospital_standardcharges.csv?sfvrsn=fdfa40fb_3&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381426929_beaumont-hospital-farmington-hills-professional_standardcharges.csv?sfvrsn=aafb40fb_3&download=true"
    ],
    "230176": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-trenton-hospital_standardcharges.csv?sfvrsn=affa40fb_5&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-trenton-professional_standardcharges.csv?sfvrsn=c1fb40fb_5&download=true"
    ],
    "230269": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-troy-hospital_standardcharges.csv?sfvrsn=4bfa40fb_3&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381459362_beaumont-hospital-troy-professional_standardcharges.csv?sfvrsn=b7fb40fb_5&download=true"
    ],
    "230270": [
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-taylor-hospital_standardcharges.csv?sfvrsn=99fa40fb_5&download=true",
        "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-taylor-professional_standardcharges.csv?sfvrsn=25fa40fb_5&download=true"
    ]
}

TRANSPARENCY_PAGE_URL = "https://www.beaumont.org/patients-families/billing/pricing/beaumont-health-price-transparency-information"

LAST_UPDATED_AT = "2023-01-01"

def main():
    out_f = open("hospital.sql", "w")
    
    for ccn in TASKS.keys():
        urls = list(TASKS[ccn])
        filenames = list(map(derive_filename_from_url, urls))
        ein = derive_ein_from_filename(filenames[0])

        urls = "|".join(urls)
        filenames = "|".join(filenames)
                         
        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
                ein, LAST_UPDATED_AT, filenames, urls, TRANSPARENCY_PAGE_URL, ccn)

        out_f.write(query)
        out_f.write("\n")

    out_f.close()
    
    for ccn in TASKS.keys():
        urls = TASKS[ccn]
        
        for url in TASKS[ccn]:
            print(ccn, urls)
            filename = derive_filename_from_url(url)
        
            df_in = get_input_dataframe(url)
            print(df_in)
        
            df_out = convert_dataframe(df_in, ccn)
    
            billing_class = 'facility'
            if "professional" in filename:
                billing_class = 'professional'

            df_out['billing_class'] = billing_class
            print(df_out)
            
            df_out.to_csv("rate_" + ccn + billing_class + ".csv", 
                          index=False, quoting=csv.QUOTE_MINIMAL)
    print('UPDATE rate SET line_type = SUBSTRING(local_code, 1, 25), local_code = NULL WHERE local_code LIKE "Office%" OR local_code LIKE "Hospital%";')

if __name__ == "__main__":
    main()
