#: To create hexes:
#: Summarize within- StateHex_5sqmi, wfh_eins, out to wfh_eins_date_5mihex

from pathlib import Path

import numpy as np
import pandas as pd

import arcpy


def get_wfh_eins(survey_path, monthly_employee_data_path, output_csv_path):
    '''Create a csv of the employee data that have matching records in the WFH survey

    Args:
        survey_path (Path): xls/x file of the WFH survey data
        monthly_employee_data_path (Path): xls/x of employee data
        output_csv_path (Path): output csv file
    '''

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

    # operators_path = r"A:\telework_survey\Operators.xlsx"
    # operators_df = pd.read_excel(operators_path, engine='openpyxl')
    # cleaned_operators = operators_df[(operators_df['EIN']>=100000) & (operators_df['EIN'] <= 999999)]
    # op_merged = pd.merge(monthly_df, cleaned_operators, how='inner', left_on='EINint', right_on='EIN')
    # op_merged.to_csv(r'A:\telework_survey\operator_records.csv')

    print(f'Saving output data to {output_csv_path}...')
    wfh_records.to_csv(output_csv_path)


def geocode_points(points_csv, out_fc, locator, addr_field, zip_field):
    '''Geocode the points_csv, only saving the points that have a valid match to out_fc

    Args:
        points_csv (str): The csv holding the points
        out_fc (str): The feature class to save the geocoded points
        locator (str): The full path to the geolocator to use
        addr_field (str): The address field in points_csv
        zip_field (str): The zip code field in points_csv
    '''

    #: using 'memory' seems to limit the geocode to 1000
    # geocode_fc = r'A:\telework_survey\wfh.gdb\geocoded_test_intermediate'
    geocode_fc = r'memory\temp_geocode_fc'

    fields_str = f"'Street or Intersection' {addr_field} VISIBLE NONE;'City or Placename' <None> VISIBLE NONE;'ZIP Code' {zip_field} VISIBLE NONE"
    print('Geocoding (this could take a while)...')
    geocode_results = arcpy.geocoding.GeocodeAddresses(points_csv, locator, fields_str, geocode_fc)

    print(geocode_results.getMessages())

    print('Copying out only the matched points...')
    query = "Status = 'M'"
    arcpy.management.MakeFeatureLayer(geocode_fc, 'geocode_layer', query)
    arcpy.management.CopyFeatures('geocode_layer', out_fc)

    #: Testing environment stuff
    environments = arcpy.ListEnvironments()

    # Sort the environment names
    environments.sort()

    for environment in environments:
        # Format and print each environment and its current setting.
        # (The environments are accessed by key from arcpy.env.)
        print("{0:<30}: {1}".format(environment, arcpy.env[environment]))


if __name__ == '__main__':
    survey_path = Path(r'A:\telework_survey\Teleworking Onboarding Survey_December 14, 2020_08.45.xlsx')
    employee_data_path = Path(r'A:\monthly_data\2020_12_01.xls')
    out_path = Path(r'A:\telework_survey\wfh_records_test.csv')
    geocoded_points_path = Path(r'A:\telework_survey\wfh.gdb\geocoded_test')
    locator_path = Path(r'C:\temp\locators\AGRC_CompositeLocator.loc')

    # get_wfh_eins(survey_path, employee_data_path, out_path)
    geocode_points(str(out_path), str(geocoded_points_path), str(locator_path), 'real_addr', 'real_zip')
