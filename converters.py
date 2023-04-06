#!/usr/bin/python3

import pandas as pd

from helpers import *

TARGET_COLUMNS = ['filename', 'hospital_ccn', 'hospital_ein', 'code_meta', 'unique_procedure_id',
                      'internal_code', 'billing_class', 'patient_class', 'rev_code',
                      'rev_desc', 'code_type', 'code', 'code_modifier', 'procedure_desc',
                      'cdm', 'hcpcs_cpt', 'ndc', 'ms_drg', 'icd_10',
                      'eapg', 'apc', 'cmg', 'quantity_desc', 'quantity_number',
                      'quantity_type', 'payer_category', 'payer_desc', 'payer_name',
                      'plan_name', 'plan_id', 'plan_type', 'is_medicare_adv', 'rate', 'rate_method',
                      'rate_desc', 'file_last_updated', 'url', 'permalink']

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
        df_in['NDC'] = df_in['NDC'].fillna('')
        df_in['NDC'] = df_in['NDC'].astype(str)
        df_in['Rev'] = df_in['Rev'].fillna('nan')
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
            'CPT': 'hcpcs_cpt',
            'Rev': 'rev_code',
            'NDC': 'ndc',
            'Chargecode_DRG_CPT': 'code'
        })
        del df_intermediate['Facility']

        df_intermediate['rev_code'] = df_intermediate['rev_code'].apply(lambda rev_code: rev_code.split('.')[0])
        df_intermediate['rev_code'] = df_intermediate['rev_code'].apply(pad_rev_code_if_needed)
        df_intermediate['hcpcs_cpt'] = df_intermediate['hcpcs_cpt'].fillna('-1')
        df_intermediate['hcpcs_cpt'] = df_intermediate['hcpcs_cpt'].astype(str)
        df_intermediate['hcpcs_cpt'] = df_intermediate['hcpcs_cpt'].replace('-1', '')
        df_intermediate['hcpcs_cpt'] = df_intermediate['hcpcs_cpt'].apply(lambda cpt: cpt[:5])
        df_intermediate['quantity_desc'] = 'nan'
        df_intermediate['patient_class'] = df_intermediate['Type'].replace(
            'CHARGE', 'nan').replace(
            'IP DRG*', 'inpatient').replace(
            'OP PROC*', 'outpatient')
        df_intermediate['code_type'] = df_intermediate['Type'].replace(
            'IP DRG*', 'ms-drg').replace(
            'OP PROC*', 'hcpcs_cpt').replace(
            'CHARGE', '')
        df_intermediate['code_meta'] = df_intermediate['Type'].replace(
            'IP DRG*', 'drg').replace(
            'OP PROC*', 'cpt').replace(
            'CHARGE', 'cdm')

        del df_intermediate['Type']

        # https://stackoverflow.com/a/60264415
        df_intermediate['hcpcs_cpt'] = df_intermediate.apply(lambda row: row['code'] if row['code_meta'] == 'cpt' else None, axis=1)
        df_intermediate['ms_drg'] = df_intermediate.apply(lambda row: row['code'] if row['code_meta'] == 'drg' else None, axis=1)
        df_intermediate['cdm'] = df_intermediate.apply(lambda row: row['code'] if row['code_meta'] == 'cdm' else None, axis=1)

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

            if 'Health_EOS' in payer_desc:
                return 'Health EOS'

            components = payer_desc.split('_')
            if len(components) < 3 or components[-1] == 'Fee':
                return ''

            components = components[2:]
            return components[0]

        df_intermediate['payer_name'] = df_intermediate['payer_desc'].apply(get_payer_name_from_payer_desc)

        filename = file_path.split("/")[0]
        hospital_ein = filename.split("_")[0]

        df_intermediate['filename'] = filename
        df_intermediate['hospital_ein'] = hospital_ein
        df_intermediate['hospital_ccn'] = ccn
        df_intermediate['url'] = url
        df_intermediate['unique_procedure_id'] = 'nan'
        df_intermediate['internal_code'] = 'nan'
        df_intermediate['billing_class'] = 'nan'

        def get_plan_type_from_payer_desc(payer_desc):
            components = payer_desc.split('_')
            last_component = components[-1]
            if last_component in ["HMO", "PPO", "HPN", "GPPO", "EPO"]:
                return last_component

            return ''

        def get_plan_name_from_payer_desc(payer_desc):
            if payer_desc.endswith("Fee"):
                return ''

            payer_name = get_payer_name_from_payer_desc(payer_desc)
            components = payer_desc.split('_')
            components = components[2:]
            plan_type = get_plan_type_from_payer_desc(payer_desc)
            return ' '.join(components).replace(plan_type, '').replace(payer_name, '').strip()

        df_intermediate['plan_type'] = df_intermediate['payer_desc'].apply(get_plan_type_from_payer_desc)
        df_intermediate['plan_name'] = df_intermediate['payer_desc'].apply(get_plan_name_from_payer_desc)

        df_out = pd.DataFrame(columns=TARGET_COLUMNS)

        df_out = df_out.append(df_intermediate)

        return df_out
