import subprocess

import pandas as pd

from helpers import cleanup_dollar_value, derive_ein_from_filename, derive_filename_from_url

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

TASKS = {
    "310074": "https://www.rwjbh.org/documents/billing/mrf/222783298_jersey-city-medical-center_standardcharges.csv",
    "310009": "https://www.rwjbh.org/documents/billing/MRF/221500556_clara-maass-medical-center_standardcharges.csv",
    "310041": "https://www.rwjbh.org/documents/billing/mrf/223452306_community-medical-center_standardcharges.csv",
    "310076": "https://www.rwjbh.org/documents/billing/mrf/222977312_cooperman-barnabas-medical-center_standardcharges.csv",
    "310075": "https://www.rwjbh.org/documents/billing/mrf/223452412_monmouth-medical-center_standardcharges.csv",
    "310084": "https://www.rwjbh.org/documents/billing/mrf/223452412_monmouth-medical-center-southern-campus_standardcharges.csv",
    "310002": "https://www.rwjbh.org/documents/billing/mrf/223452311_newark-beth-israel-medical-center_standardcharges.csv",
    "310110": "https://www.rwjbh.org/documents/billing/mrf/210634572_rwj-university-hospital-hamilton_standardcharges.csv",
    "310038": "https://www.rwjbh.org/documents/billing/mrf/221487243_rwj-university-hospital-new-brunswick_standardcharges.csv",
    "310024": "https://www.rwjbh.org/documents/billing/mrf/221487305_rwj-university-hospital-rahway_standardcharges.csv",
    "310048": "https://www.rwjbh.org/documents/billing/mrf/221487243_rwj-university-hospital-somerset_standardcharges.csv",
}

TRANSPARENCY_PAGES = {
    "310074": "https://www.rwjbh.org/jersey-city-medical-center/billing/charges/",
    "310009": "https://www.rwjbh.org/clara-maass-medical-center/billing/charges/",
    "310041": "https://www.rwjbh.org/community-medical-center/billing/charges/",
    "310076": "https://www.rwjbh.org/cooperman-barnabas-medical-center/billing/charges/",
    "310075": "https://www.rwjbh.org/monmouth-medical-center/billing/charges/",
    "310084": "https://www.rwjbh.org/monmouth-medical-center-southern-campus/billing/charges/",
    "310002": "https://www.rwjbh.org/newark-beth-israel-medical-center/billing/charges/",
    "310110": "https://www.rwjbh.org/rwj-university-hospital-hamilton/billing/charges/",
    "310038": "https://www.rwjbh.org/rwj-university-hospital-new-brunswick/billing/charges/",
    "310024": "https://www.rwjbh.org/rwj-university-hospital-rahway/billing/charges/",
    "310048": "https://www.rwjbh.org/rwj-university-hospital-somerset/billing/charges/",
}

def payer_category_from_payer_name(payer_name):
    payer_name = payer_name.strip()
    
    if payer_name == "Gross Charge":
        return 'gross'
    elif payer_name.startswith('Discounted Cash Price'):
        return 'cash'
    elif payer_name.startswith('De-identified Minimum Negotiated Charge'):
        return 'min'
    elif payer_name.startswith('De-identified Maximum Negotiated Charge'):
        return 'max'

    return 'payer'

def convert_dataframe(df_in, ccn):
    df_mid = pd.DataFrame(df_in)
    df_mid = df_mid.rename(columns={
        'Service Type': 'setting',
        'Billing Code': 'code',
        'Service Description': 'description',
    })

    del df_mid['Hospital Name']
    del df_mid['Hospital Code']

    money_columns = df_mid.columns.to_list()[3:]
    remaining_columns = df_mid.columns.to_list()[:3]
    df_mid = pd.melt(df_mid, id_vars=remaining_columns, var_name='payer_name', value_name='standard_charge')

    df_mid['ms_drg'] = df_mid['code'].apply(lambda code: str(code) if code is not None and len(str(code)) == 3 else None)
    df_mid['hcpcs_cpt'] = df_mid['code'].apply(lambda code: str(code) if code is not None and len(str(code)) == 5 else None)

    df_mid = df_mid[df_mid['standard_charge'].notnull()]
    df_mid['standard_charge'] = df_mid['standard_charge'].apply(cleanup_dollar_value)
    df_mid = df_mid[df_mid['standard_charge'] != "-"]
    df_mid = df_mid[df_mid['standard_charge'] != "n/a"]
    df_mid = df_mid[df_mid['standard_charge'] != "#REF!"]

    df_mid['payer_category'] = df_mid['payer_name'].apply(payer_category_from_payer_name)

    df_mid.loc[df_mid['setting'] == 'Inpatient', 'setting'] = 'inpatient'
    df_mid.loc[df_mid['setting'] == 'Inpatient / Outpatient', 'setting'] = 'both'
    df_mid.loc[df_mid['setting'] == 'Outpatient', 'setting'] = 'outpatient'

    df_mid['hospital_id'] = ccn
    df_mid['line_type'] = None
    df_mid['rev_code'] = None
    df_mid['local_code'] = None
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
    df_mid['plan_name'] = None
    df_mid['standard_charge_percent'] = None
    df_mid['contracting_method'] = None
    df_mid['additional_generic_notes'] = None
    df_mid['additional_payer_specific_notes'] = None

    df_out = pd.DataFrame(df_mid[TARGET_COLUMNS])

    return df_out

def get_input_dataframe(url):
    filename = derive_filename_from_url(url)
                                        
    subprocess.run(["wget", "--no-clobber", url, "-O", filename])

    df_in = pd.read_csv(filename, skiprows=[1,2,3], encoding='latin-1')
    df_in.columns = df_in.iloc[0]
    df_in = df_in.iloc[1:]

    return df_in

def main():
    out_f = open("hospital.sql", "w")

    last_updated = "2023-04-28" # XXX: maybe extract this from files
    
    for ccn in TASKS.keys():
        url = TASKS[ccn]
        filename = derive_filename_from_url(url)
        ein = derive_ein_from_filename(filename)

        df_in = get_input_dataframe(url)
        print(df_in)

        df_out = convert_dataframe(df_in, ccn)
        print(df_out)

        df_out.to_csv('rate_' + ccn + '.csv', index=False)

        transparency_page = TRANSPARENCY_PAGES[ccn]
        
        query = 'UPDATE hospital SET ein = "{}", last_updated = "{}", file_name = "{}", mrf_url = "{}", transparency_page = "{}" WHERE id = "{}";'.format(
            ein, last_updated, filename, url, transparency_page, ccn
        )

        out_f.write(query)
        out_f.write("\n")

    out_f.close()

if __name__ == "__main__":
    main()
    