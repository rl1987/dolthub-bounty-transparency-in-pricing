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
    'hospital_id',
    'line_type',
    'description',
    'rev_code',
    'local_code',
    'code',
    'ms_drg',
    'apr_drg',
    'hcpcs_cpt',
    'modifiers',
    'thru',
    'apc',
    'icd',
    'ndc',
    'drug_hcpcs_multiplier',
    'drug_quantity',
    'drug_units',
    'billing_class',
    'setting',
    'payer_category',
    'payer',
    'plan',
    'standard_charge',
    'standard_charge_percent',
    'contracting_method',
    'additional_payer_notes'
]

def create_session():
    session = httpx.Client(http2=True)
    
    session.headers = {
        'authority': 'www.avera.org',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
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

def payer_category_from_payer(payer):
    if payer == 'Gross Charge':
        return 'gross'
    elif payer == 'Discounted Cash Price':
        return 'cash'
    elif payer == 'Minimum Negotiated Rate':
        return 'min'
    elif payer == 'Maximum Negotiated Rate':
        return 'max'
    
    return 'payer'

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)

    df_mid = df_mid.rename(columns={
        'CPT': 'cpt',
        'Charge Code': 'local_code',
        'Description': 'description',
        'Hcpcs': 'hcpcs',
        'Revcode': 'rev_code'
    })

    columns = df_mid.columns.to_list()
    columns.pop(columns.index('Gross Charge'))
    df_mid = df_mid[columns + ['Gross Charge']]

    del df_mid['Facility Name']

    money_columns = df_mid.columns[5:]
    remaining_columns = df_mid.columns.to_list()[:5]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer', value_name='standard_charge')
    
    df_mid['hcpcs_cpt'] = df_mid.apply(lambda row: row['cpt'] if row['cpt'] is not None else row['hcpcs'], axis=1)
    del df_mid['cpt']
    del df_mid['hcpcs']
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].fillna('')

    df_mid['payer_category'] = df_mid['payer'].apply(payer_category_from_payer)

    df_mid['hospital_id'] = ccn
    df_mid['line_type'] = None
    df_mid['rev_code'] = df_mid['rev_code'].fillna('')
    df_mid['rev_code'] = df_mid['rev_code'][:4] # 0964an
    df_mid['local_code'] = df_mid['local_code'].fillna('')
    df_mid['code'] = ''
    df_mid['ms_drg'] = ''
    df_mid['apr_drg'] = ''
    df_mid['apr_drg'] = ''
    df_mid['modifiers'] = ''
    df_mid['thru'] = None
    df_mid['apc'] = None
    df_mid['icd'] = None
    df_mid['ndc'] = ''
    df_mid['drug_hcpcs_multiplier'] = None
    df_mid['drug_quantity'] = None
    df_mid['drug_units'] = None
    df_mid['billing_class'] = 1
    df_mid['setting'] = 1
    df_mid['plan'] = ''
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    df_mid['additional_payer_notes'] = None
    
    df_mid = df_mid.dropna(subset=['standard_charge'])

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])
    del df_out['apc']

    print(df_out)

    return df_out

def get_input_dataframe(session, url):
    filename = derive_filename_from_url(url)
    download_file(session, url, filename)

    df_in = pd.read_excel(filename, skiprows=1, sheet_name='CDM', engine='openpyxl')
    print(df_in)

    return df_in

TASKS = {
    '161321': 'https://www.avera.org/app/files/public/887b57a5-04fc-4a68-9636-d6f0d9b2dfab/460224743_avera-merrill-pioneer_standardcharges.xlsx',
    '161351': 'https://www.avera.org/app/files/public/188febb6-f608-4630-877b-eeab98316fa3/420680370_avera-holy-family-hospital_standardcharges.xlsx',
    '241343': 'https://www.avera.org/app/files/public/84e6c5b3-a05c-41ab-bc99-9e7cdecc6698/843156881_avera-granite-falls-health-center_standardcharges.xlsx',
    '241348': 'https://www.avera.org/app/files/public/c6bee50e-7514-4ee4-b280-65fa14a68c3b/410853163_avera-tyler-hospital_standardcharges.xlsx',
    '241359': 'https://www.avera.org/app/files/public/b391414f-acc2-417e-a79f-28a1b5f26467/460380552_avera-marshall-hospital_standardcharges.xlsx',
    '281329': 'https://www.avera.org/app/files/public/fd10ed16-edb3-4376-b5fb-597facee8de1/470463911_avera-st.-anthonys-hospital_standardcharges.xlsx',
    '281331': 'https://www.avera.org/app/files/public/b98961ca-b15f-4795-ad88-54f6d3b5bfab/460225483_avera-creighton-hospital_standardcharges.xlsx',
    '430012': 'https://www.avera.org/app/files/public/acc8f842-8881-42f7-a5d3-3f51053a2d6f/460225483_avera-sacred-heart-hospital_standardcharges.xlsx',
    '430013': 'https://www.avera.org/app/files/public/bc8a4b94-6096-41ff-87df-897ba9e95a2a/460224604_avera-queen-of-peace_standardcharges.xlsx',
    '430014': 'https://www.avera.org/app/files/public/2b16bcfc-5575-4bc0-8f9b-2adcbc497f74/460224598_avera-st.-lukes_standardcharges.xlsx',
    '430015': 'https://www.avera.org/app/files/public/660d6fed-090c-4637-9d3d-fda1e5f4b339/460230199_avera-st.-marys-hospital_standardcharges.xlsx',
    '430016': 'https://www.avera.org/app/files/public/07f4da97-92e8-4e47-afe6-45df7189c896/460024743_avera-mcKennan-hospital_standardcharges.xlsx',
    '430095': 'https://www.avera.org/app/files/public/1e15639c-7810-4899-ad0f-9368f892df0c/562143771_avera-heart-hospital_standardcharges.xlsx',
    '431302': 'https://www.avera.org/app/files/public/08b43fe0-f7e6-4955-86e4-a110149167c6/460234354_avera-missouri-river-health-center_standardcharges.xlsx',
    '431310': 'https://www.avera.org/app/files/public/652b2e59-7031-4d9c-abec-649dcf20f2ec/460224743_avera-flandreau-hospital_standardcharges.xlsx',
    '431324': 'https://www.avera.org/app/files/public/7e400597-6472-47c0-a80b-9c649c997f69/460224604_avera-weskota-memorial-med-ctr_standardcharges.xlsx',
    '431330': 'https://www.avera.org/app/files/public/b0514906-867f-4f82-b455-bdb5acc0a0ee/460226738_avera-st.-benedicts_standardcharges.xlsx',
    '431331': 'https://www.avera.org/app/files/public/beefa3f3-2582-45c6-9acd-eb0bc724917b/460224743_avera-dell-rapids-hospital_standardcharges.xlsx',
    '431332': 'https://www.avera.org/app/files/public/657353e3-2de5-40f2-b265-f0460302fa29/460224604_avera-desmet_standardcharges.xlsx',
    '431337': 'https://www.avera.org/app/files/public/862efce2-7f11-4c2b-939e-5608a8356a89/460224743_avera-hand-county-hospital_standardcharges.xlsx',
    '431338': 'https://www.avera.org/app/files/public/f6cb31b1-0cf3-4316-be33-ea0b2da7bea5/460224743_avera-gregory-hospital_standardcharges.xlsx'
}

TRANSPARENCY_PAGE_URL = "https://www.avera.org/patients-visitors/price-transparency/"

def main():
    session = create_session()

    for ccn in TASKS.keys():
        url = TASKS[ccn]
        print(ccn, url)

        filename = derive_filename_from_url(url)
        
        df_in = get_input_dataframe(session, url)
        
        df_out = convert_dataframe(df_in, ccn)

        df_out.to_csv("rate_" + ccn + ".csv", index=False, quoting=csv.QUOTE_ALL)

    out_f = open("hospital.sql", "w")
    
    for ccn in TASKS.keys():
        url = TASKS[ccn]

        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)
            
        wb = openpyxl.load_workbook(filename)
        ws = wb['CDM']
        first_cell_value = ws.cell(row=1, column=1).value
        date_str = first_cell_value.replace("Prices Effective ", "")
        last_updated_at = parse_datetime(date_str).isoformat().split("T")[0]

        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", stdchg_file_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
                ein, last_updated_at, filename, url, TRANSPARENCY_PAGE_URL, ccn)

        out_f.write(query)
        out_f.write("\n")

    out_f.close()

if __name__ == "__main__":
    main()

