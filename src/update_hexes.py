#: To create hexes:
#: Summarize within- StateHex_5sqmi, wfh_eins, out to wfh_eins_date_5mihex

from pathlib import Path

import numpy as np
import pandas as pd


def get_wfh_eins(survey_path, monthly_employee_data_path, output_csv_path):

    print('Reading WFH survey...')
    survey_df = pd.read_excel(survey_path, engine='openpyxl')

    #: Drop double header row
    survey_df.drop(survey_df.index[0])

    #: Drop rows that aren't new or that are UTNG (they don't use EINs)
    non_update_df = survey_df[(survey_df['New'] == 'Yes') & ~(survey_df['Q5'] == 'UTNG')]

    #: Non-na EINs as a series
    eins = non_update_df[non_update_df['Q1_4'].notna()].loc[:, 'Q1_4']

    #: Drop malformed EINs (not 6 digits) and duplicate EINs
    wfh_eins_int = eins[eins.str.len() == 6].astype(int)
    wfh_eins_int = wfh_eins_int.drop_duplicates()

    #: Format the monthly data from DHRM
    print('Reading DHRM employee data...')
    monthly_df = pd.read_excel(monthly_employee_data_path)
    #: Use different row for header
    monthly_df.columns = monthly_df.iloc[0]
    #: Drop old header row
    monthly_df = monthly_df.drop(monthly_df.index[0])
    #: Drop "Page" row at bottom
    monthly_df = monthly_df.drop(monthly_df.index[-1])

    #: Use physical addr if available, otherwise stick with mailing (improves geocode by 1% :shrug:)
    monthly_df['real_addr'] = np.where(
        monthly_df['physical_address_line1'].isnull(), monthly_df['mailing_address_line1'],
        monthly_df['physical_address_line1']
    )
    monthly_df['real_zip'] = np.where(
        monthly_df['Empl Physical ZIP'].isnull(), monthly_df['Empl Mail ZIP'].str.strip().str.slice(stop=5),
        monthly_df['Empl Physical ZIP'].str.strip().str.slice(stop=5)
    )

    #: Convert EINs to ints
    monthly_df['EINint'] = monthly_df['EIN'].astype(int, errors='ignore')

    #: "join" by only including rows where EINint is in series of EINs from WFH survey
    wfh_records = monthly_df[monthly_df['EINint'].isin(wfh_eins_int)]
    print(
        f'Employee records with matching EIN in WFH survey: {wfh_records.shape[0]} (out of {wfh_eins_int.count()} EINs from survey)'
    )

    print(f'Saving output data to {output_csv_path}...')
    wfh_records.to_csv(output_csv_path)


if __name__ == '__main__':
    survey_path = Path(r'A:\telework_survey\Teleworking Onboarding Survey_December 14, 2020_08.45.xlsx')
    employee_data_path = Path(r'A:\monthly_data\2020_12_01.xls')
    out_path = Path(r'A:\telework_survey\wfh_records_test.csv')

    get_wfh_eins(survey_path, employee_data_path, out_path)
