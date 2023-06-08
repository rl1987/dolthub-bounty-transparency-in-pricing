#!/usr/bin/python3

import csv
from urllib.parse import urljoin
from io import StringIO
import subprocess
import sys

from dateutil.parser import parse as parse_datetime
import pandas as pd
import requests
from lxml import html
import js2xml

from helpers import derive_ein_from_filename

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

def unify_modifiers(m1, m2):
    if m1 is None:
        return None

    if m1 is not None:
        if m2 is not None:
            return m1 + "|" + m2

        return m1

def payer_name_to_payer_category(payer_name):
    if payer_name == 'Discounted Cash Price':
        return 'cash'
    elif payer_name == 'Gross Charge':
        return 'gross'
    elif payer_name == 'De-identified maximum negotiated charge':
        return 'max'
    elif payer_name == 'De-identified minimum negotiated charge':
        return 'min'

    return 'payer'

def convert_chunk(chunk, ccn):
    csv_buf = StringIO(chunk)

    df_in = pd.read_csv(csv_buf, dtype=str)

    df_mid = pd.DataFrame(df_in)
    df_mid = df_mid.rename(columns={
        'Procedure Code': 'local_code',
        'Procedure Description': 'description',
        'Price Tier': 'setting',
        'Revenue Code': 'rev_code',
        'CPT HCPCS Code': 'hcpcs_cpt',
        'NDC Code': 'ndc',
        'Rx Unit Multiplier': 'drug_hcpcs_multiplier',
        'Modifier1': 'modifiers',
        'Diagnosis Related Group Code': 'ms_drg',
        'Diagnosis Related Group Description': 'description',
        'CPT HCPCS DRG Code': 'code',
        'Shoppable Services Code': 'local_code',
        'Shoppable Services Description': 'description'
    })

    df_mid.loc[df_mid['modifiers'].isnull(), 'modifiers'] = None
    df_mid.loc[df_mid['Modifier2'].isnull(), 'Modifier2'] = None

    df_mid['modifiers'] = df_mid[['modifiers', 'Modifier2']].apply(lambda row: unify_modifiers(row['modifiers'], row['Modifier2']), axis=1)
    del df_mid['Modifier2']

    remaining_columns = df_mid.columns.to_list()[:8]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    df_mid['additional_generic_notes'] = df_mid['setting']
    df_mid['billing_class'] = None
    df_mid.loc[df_mid['setting'] == 'ProFee', 'billing_class'] = 'professional'
    df_mid['setting'] = None
    df_mid.loc[df_mid['additional_generic_notes'] == 'Inpatient', 'setting'] = 'inpatient'
    df_mid.loc[df_mid['additional_generic_notes'] == 'Outpatient', 'setting'] = 'outpatient'

    df_mid['payer_category'] = df_mid['payer_name'].apply(payer_name_to_payer_category)

    df_mid = df_mid[df_mid['standard_charge'].notnull()]

    df_mid = pd.DataFrame(df_mid) # XXX

    if not 'hcpcs_cpt' in df_mid.columns:
        df_mid['hcpcs_cpt'] = None

    df_mid.loc[df_mid['hcpcs_cpt'].isnull(), 'hcpcs_cpt'] = ''
    df_mid.loc[df_mid['hcpcs_cpt'].str.isalpha(), 'code'] = df_mid[df_mid['hcpcs_cpt'].str.isalpha()]['hcpcs_cpt']
    df_mid['hcpcs_cpt'] = df_mid['hcpcs_cpt'].apply(lambda cpt: '' if len(cpt) != 5 else cpt)
    df_mid.loc[df_mid['hcpcs_cpt'].str.isalpha(), 'hcpcs_cpt'] = None
    df_mid.loc[df_mid['hcpcs_cpt'] == '', 'hcpcs_cpt'] = None

    df_mid['hospital_id'] = ccn
    df_mid['line_type'] = None
    if not 'local_code' in df_mid.columns:
        df_mid['local_code'] = None
    if not 'code' in df_mid.columns:
        df_mid['code'] = None
    if not 'ms_drg' in df_mid.columns:
        df_mid['ms_drg'] = None
    df_mid['apr_drg'] = None
    df_mid['eapg'] = None
    df_mid['alt_hcpcs_cpt'] = None
    df_mid['thru'] = None
    df_mid['apc'] = None
    if not 'icd' in df_mid.columns:
        df_mid['icd'] = None
    df_mid['drug_quantity'] = None
    df_mid['drug_unit_of_measurement'] = None
    df_mid['drug_type_of_measurement'] = None
    df_mid['plan_name'] = None
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    df_mid['additional_payer_specific_notes'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])
    return df_out

def perform_task(h_f, ccn, app_url, transparency_page):
    resp = requests.get(app_url)
    print(resp.url)

    tree = html.fromstring(resp.text)
    js_link = tree.xpath('//script[contains(@src, "/PTT/extnet/extnet-init-js")]/@src')[0]
    js_url = urljoin(resp.url, js_link)
    resp1 = requests.get(js_url)
    parsed = js2xml.parse(resp1.text)
    db_name = parsed.xpath('//object[./property/string[text()="App.hdnDB_Container"]]/property[@name="value"]/string/text()')[0]

    params = { 'dbName': db_name, 'type': 'CDMWithoutLabel' }

    data = '------WebKitFormBoundarysVqstz3xq11k5yBT\r\nContent-Disposition: form-data; name="__EVENTTARGET"\r\n\r\nResourceManager\r\n------WebKitFormBoundarysVqstz3xq11k5yBT\r\nContent-Disposition: form-data; name="__EVENTARGUMENT"\r\n\r\n-|public|DownloadReport\r\n------WebKitFormBoundarysVqstz3xq11k5yBT\r\nContent-Disposition: form-data; name="__ExtNetDirectEventMarker"\r\n\r\ndelta=true\r\n------WebKitFormBoundarysVqstz3xq11k5yBT--\r\n'

    resp2 = requests.post('https://apps.para-hcfs.com/PTT/FinalLinks/Reports.aspx', params=params, data=data)
    print(resp2.url)

    dfs = []
    
    chunks = resp2.text.split("\r\n\r\n")

    for chunk in chunks[1:]:
        df_tmp = convert_chunk(chunk, ccn)
        dfs.append(df_tmp)

    df_out = pd.concat(dfs)

    df_out.to_csv('rate_' + ccn + '.csv', index=False)
    
    filename = resp2.headers['content-disposition'].split('"')[1]
    ein = derive_ein_from_filename(filename)
    
    date_str = chunks[0].split(" ")[-1]

    last_updated_at = parse_datetime(date_str).isoformat().split("T")[0]
    query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
        ein, last_updated_at, filename, app_url, transparency_page, ccn)

    h_f.write(query)
    h_f.write("\n")

def main():
    if len(sys.argv) != 2:
        print("Usage:")
        print("{} <tasks_csv>".format(sys.argv[0]))
        return

    tasks_csv = sys.argv[1]

    in_f = open(tasks_csv)

    csv_reader = csv.DictReader(in_f)

    h_f = open("hospitals.sql", "w")
    
    for row in csv_reader:
        transparency_page = row.get('transparency_page')
        ccn = row.get('ccn')
        app_url = row.get('app_url')

        perform_task(h_f, ccn, app_url, transparency_page)
    
    h_f.close()
    in_f.close()

if __name__ == "__main__":
    main()
    

