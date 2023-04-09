#!/usr/bin/python3

import pandas as pd

from helpers import *

TARGET_COLUMNS = ['hospital_tin', 'hospital_ccn', 'code_meta', 'description', 'procedure_code', 
                  'code_type', 'code', 'modifier', 'ndc', 'rev_code', 'rev_desc', 'apc', 
                  'billing_class', 'patient_class', 'billed_quantity', 'quantity_desc', 
                  'payer_desc', 'payer_category', 'payer_name', 'plan_desc', 'plan_name',
                  'plan_id', 'plan_type', 'is_medicare_adv', 'rate', 'rate_method', 'rate_desc',
                  'filename', 'file_last_updated', 'url', 'permalink']

class AbstractStandardChargesConverter(object):
    def convert(self, url, file_path, ccn):
        pass

class AuroraXMLConverter(AbstractStandardChargesConverter):
    def __init__(self):
        super().__init__()

    def convert(self, url, file_path, ccn):
        df_out = pd.DataFrame(columns=TARGET_COLUMNS)

        df_in = pd.read_xml(file_path)
        # HACK: https://stackoverflow.com/a/50132405
        df_in['NDC'] = df_in['NDC'].fillna('na')
        df_in['NDC'] = df_in['NDC'].astype(str)
        df_in['Rev'] = df_in['Rev'].fillna('na')
        df_in['Rev'] = df_in['Rev'].astype(str)
        df_in['Chargecode_DRG_CPT'] = df_in['Chargecode_DRG_CPT'].astype(str)

        columns = df_in.columns.to_list()
        money_columns = list(filter(lambda c: c.startswith('_'), columns)) + ['Self_Pay', 'Min', 'Max']

        df_in[money_columns] = df_in[money_columns].apply(lambda values: cleanup_values(values))

        remaining_cols = list(set(columns) - set(money_columns))

        df_intermediate = pd.DataFrame(df_in)
        df_intermediate = pd.melt(df_intermediate, id_vars=remaining_cols)
        df_intermediate = df_intermediate.rename(columns={
            'variable': 'payer_desc',
            'value': 'rate',
            'Description': 'procedure_desc',
            'Rev': 'rev_code',
            'NDC': 'ndc',
            'Chargecode_DRG_CPT': 'code'
        })
        del df_intermediate['Facility']
        del df_intermediate['CPT']

        df_intermediate['rev_code'] = df_intermediate['rev_code'].apply(lambda rev_code: rev_code.split('.')[0])
        df_intermediate['rev_code'] = df_intermediate['rev_code'].apply(pad_rev_code_if_needed)
        df_intermediate['patient_class'] = df_intermediate['Type'].replace(
            'CHARGE', 'na').replace(
            'IP DRG*', 'inpatient').replace(
            'OP PROC*', 'outpatient')
        df_intermediate['code_type'] = df_intermediate['Type'].replace(
            'IP DRG*', 'ms-drg').replace(
            'OP PROC*', 'hcpcs_cpt').replace(
            'CHARGE', 'cdm')
        df_intermediate['code_meta'] = df_intermediate['Type'].replace(
            'IP DRG*', 'drg').replace(
            'OP PROC*', 'cpt').replace(
            'CHARGE', 'cdm')

        del df_intermediate['Type']

        def get_payer_category_from_payer_desc(payer_desc):
            if payer_desc == "Min":
                return "min"
            elif payer_desc == "Max":
                return "max"
            elif payer_desc == "Self_Pay":
                return "cash"
            elif payer_desc.endswith("_Fee"):
                return "gross"

            return "payer"

        df_intermediate['payer_category'] = df_intermediate['payer_desc'].apply(get_payer_category_from_payer_desc)

        def get_payer_name_from_payer_desc(payer_desc):
            if 'Common_Ground' in payer_desc:
                return 'Common Ground'
            elif 'Health_EOS' in payer_desc:
                return 'Health EOS'
            elif 'Aetna' in payer_desc:
                return 'Aetna'
            elif 'Anthem' in payer_desc:
                return 'Anthem'
            elif 'Aurora' in payer_desc:
                return 'Aurora'
            elif 'Centivo' in payer_desc:
                return 'Centivo'
            elif 'Cigna' in payer_desc:
                return 'Cigna'
            elif 'Common_Ground' in payer_desc:
                return 'Common Ground'
            elif 'Everpointe' in payer_desc:
                return 'Everpointe'
            elif 'HealthPartners' in payer_desc:
                return 'HealthPartners'
            elif 'HPS' in payer_desc:
                return 'HPS'
            elif 'HST' in payer_desc:
                return 'HST'
            elif 'Humana' in payer_desc:
                return 'Humana'
            elif 'Molina' in payer_desc:
                return 'Molina'
            elif 'Quartz_One' in payer_desc:
                return 'Quartz One'
            elif 'Trilogy' in payer_desc:
                return 'Trilogy'
            elif 'UHC' in payer_desc:
                return 'UHC'
            elif 'WPS' in payer_desc:
                return 'WPS'

            return ''

        df_intermediate['payer_name'] = df_intermediate['payer_desc'].apply(get_payer_name_from_payer_desc)

        filename = file_path.split("/")[0]
        hospital_tin = filename.split("_")[0]
        hospital_tin = hospital_tin[:2] + "-" + hospital_tin[2:]

        df_intermediate['filename'] = filename
        df_intermediate['hospital_tin'] = hospital_tin
        df_intermediate['hospital_ccn'] = ccn
        df_intermediate['url'] = url
        df_intermediate['file_last_updated'] = '2023-01-01' # FIXME: refrain from hardcoding this; determine this field from _Fee column name
        df_intermediate['unique_procedure_id'] = 'na'
        df_intermediate['internal_code'] = 'na'
        df_intermediate['billing_class'] = 'na'
        df_intermediate['procedure_code'] = 'na'
        df_intermediate['modifier'] = 'na'
        df_intermediate['apc'] = 'na'
        df_intermediate['billed_quantity'] = -1

        def get_plan_type_from_payer_desc(payer_desc):
            components = payer_desc.split('_')
            last_component = components[-1]
            if last_component in ["HMO", "PPO", "HPN", "EPO"]:
                return last_component

            return ''

        df_intermediate['plan_type'] = df_intermediate['payer_desc'].apply(get_plan_type_from_payer_desc)
        df_intermediate['plan_desc'] = 'na'

        df_out = pd.DataFrame(columns=TARGET_COLUMNS)

        df_out = df_out.append(df_intermediate)

        return df_out

