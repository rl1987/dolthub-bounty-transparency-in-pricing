#!/usr/bin/python3

import csv
import subprocess
from urllib.parse import urlparse

from dateutil.parser import parse as parse_datetime
from sqlalchemy import create_engine
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

ccn = "230142"
url = "https://www.beaumont.org/docs/default-source/default-document-library/cdm-documents/2023/381405141_beaumont-hospital-wayne-hospital_standardcharges.csv"
filename = url.split("/")[-1].replace("%20", " ")
ein = filename.split("_")[0]
ein = ein[:2] + "-" + ein[2:]

import subprocess
subprocess.run(["wget", "--no-clobber", url, "-O", filename])

df_in = pd.read_csv(filename, encoding='latin-1', dtype={'Rev Code': str, 'Code': str}, low_memory=False)

df_mid = pd.DataFrame(df_in)
df_mid.columns = list(map(lambda c: c.strip(), df_mid.columns.to_list()))
df_mid = df_mid.rename(columns={
    'Procedure': 'local_code',
    'Code Type': 'line_type',
    'Code': 'code',
    'NDC': 'ndc',
    'Rev Code': 'rev_code',
    'Procedure Description': 'description',
})

money_columns = df_mid.columns[6:]
remaining_columns = df_mid.columns.to_list()[:6]
df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer', value_name='standard_charge')

df_mid['standard_charge'] = df_mid['standard_charge'].astype(str)
df_mid = df_mid[df_mid['standard_charge'] != 'nan']
df_mid = df_mid[df_mid['standard_charge'] != '#VALUE!']
df_mid = df_mid[df_mid['standard_charge'] != '#N/A']
df_mid = df_mid[df_mid['standard_charge'] != '-']
df_mid = df_mid[df_mid['standard_charge'] != '!']
df_mid['standard_charge'] = df_mid['standard_charge'].apply(cleanup_dollar_value)

df_mid.loc[df_mid['line_type'] == 'DRG', 'ms_drg'] = df_mid[df_mid['line_type'] == 'DRG']['code'].apply(
    lambda code: code.split(" ")[-1]
)
df_mid['ms_drg'] = df_mid['ms_drg'].fillna('')
df_mid.loc[df_mid['line_type'] == 'EAP', 'eapg'] = df_mid[df_mid['line_type'] == 'EAP']['code']
df_mid['eapg'] = df_mid['eapg'].fillna('')
df_mid['hcpcs_cpt'] = df_mid['code'].apply(lambda code: str(code) if len(str(code).replace(".0", "")) == 5 else '')

def payer_category_from_payer(payer):
    if payer == 'GROSS CHARGE':
        return 'gross'
    elif payer == 'CASH PRICE':
        return 'cash'
    elif payer == 'DEIDENTIFIED MIN':
        return 'min'
    elif payer == 'DEIDENTIFIED MAX':
        return 'max'
    
    return 'payer'

df_mid['payer_category'] = df_mid['payer'].apply(payer_category_from_payer)

df_mid.loc[df_mid['payer_category'] == 'payer', 'plan'] = df_mid[df_mid['payer_category'] == 'payer']['payer'].apply(
    lambda payer: payer.split("[")[-1].replace("]", "")
)
df_mid['plan'] = df_mid['plan'].fillna('')

df_mid['hospital_id'] = ccn
df_mid['apr_drg'] = ''
df_mid['modifiers'] = ''
df_mid['thru'] = None
df_mid['apc'] = None
df_mid['icd'] = None
df_mid['drug_hcpcs_multiplier'] = None
df_mid['drug_quantity'] = None
df_mid['drug_units'] = None
df_mid['billing_class'] = ''
df_mid['setting'] = ''
df_mid['contracting_method'] = None
df_mid['additional_payer_notes'] = None
df_mid['rev_code'] = df_mid['rev_code'].fillna('')
df_mid['standard_charge_percent'] = None

df_mid['code'] = df_mid['code'].fillna('')
df_mid['ndc'] = df_mid['ndc'].fillna('')
df_mid.loc[df_mid['ndc'].str.startswith("WBH"), 'ndc'] = ''

df_mid = df_mid.dropna(subset=['standard_charge'], axis=0)
df_mid = df_mid.drop_duplicates(subset=[
    'rev_code', 'local_code', 'code', 'ms_drg', 'apr_drg', 'modifiers',
    'hcpcs_cpt', 'ndc', 'billing_class', 'setting', 'payer', 'plan'
])

db_connection_str = 'mysql+mysqlconnector://rl:trustno1@localhost/transparency_in_pricing'
db_connection = create_engine(db_connection_str, pool_size=1)

df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

df_out.to_csv(ccn + ".csv", index=False, quoting=csv.QUOTE_ALL)
df_out.to_sql('rate', db_connection, if_exists='append', index=False, method=None, chunksize=1)