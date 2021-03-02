#: To create hexes:
#: Summarize within- StateHex_5sqmi, wfh_eins, out to wfh_eins_date_5mihex

from pathlib import Path

import numpy as np
import pandas as pd

import arcpy


def get_wfh_eins(survey_path, monthly_dhrm_data, output_csv_path):
    '''Create a csv of the employee data that have matching records in the WFH survey

    Args:
        survey_path (Path): xlsx file of the WFH survey data
        monthly_dhrm_data (DataFrame): monthly employee data
        output_csv_path (Path): output csv file
    '''

    print(f'\nReading WFH survey {survey_path}...')
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

    #: "join" by only including rows where EINint is in series of EINs from WFH survey
    wfh_records = monthly_dhrm_data[monthly_dhrm_data['EINint'].isin(wfh_eins_int)]
    print(
        f'Employee records with matching EIN in WFH survey: {wfh_records.shape[0]} (out of {wfh_eins_int.count()} EINs from survey)'
    )

    print(f'Saving output data to {output_csv_path}...')
    wfh_records.to_csv(output_csv_path)


def get_dhrm_dataframe(monthly_employee_data_path):
    '''Read and process monthly DHRM employee data dump.

    Creates real_addr/real_zip field with mailing address/zip if provided, physical address/zip otherwise.
    Creates EINint field by converting EIN to int, ignoring any errors

    Args:
        monthly_employee_data_path (Path): xls file from DHRM

    Returns:
        DataFrame: DHRM data with real_addr, real_zip, and EINint fields added.
    '''
    print(f'\nReading DHRM employee data {monthly_employee_data_path}...')
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

    return monthly_df


def geocode_points(points_csv, out_fc, locator, addr_field, zip_field):
    '''Geocode the points_csv, only saving the points that have a valid match to out_fc

    Args:
        points_csv (str): The csv holding the points
        out_fc (str): The feature class to save the geocoded points
        locator (str): The full path to the geolocator to use
        addr_field (str): The address field in points_csv
        zip_field (str): The zip code field in points_csv
    '''

    #: using 'memory' seems to limit the geocode to 1000, use 'in_memory' instead.
    geocode_fc = r'in_memory\temp_geocode_fc'

    if arcpy.Exists(geocode_fc):
        arcpy.management.Delete(geocode_fc)

    fields_str = f"'Street or Intersection' {addr_field} VISIBLE NONE;'City or Placename' <None> VISIBLE NONE;'ZIP Code' {zip_field} VISIBLE NONE"
    print(f'\nGeocoding {points_csv} (this could take a while)...')
    geocode_results = arcpy.geocoding.GeocodeAddresses(points_csv, locator, fields_str, geocode_fc)

    print(geocode_results.getMessages())

    print('Copying out only the matched points...')
    query = "Status = 'M'"

    if arcpy.Exists(geocode_fc):
        arcpy.management.Delete('geocode_layer')

    arcpy.management.MakeFeatureLayer(geocode_fc, 'geocode_layer', query)
    arcpy.management.CopyFeatures('geocode_layer', out_fc)


def get_operator_eins(operators_path, monthly_dhrm_data, output_csv_path):
    '''Read and process approved operator data

    Args:
        operators_path (Path): xlsx of approved operators from Fleet
        monthly_dhrm_data (DataFrame): Monthly DHRM data
        out_path (Path): output csv file
    '''
    print(f'\nReading approved operator data {operators_path}...')
    operators_df = pd.read_excel(operators_path, engine='openpyxl')
    cleaned_operators = operators_df[(operators_df['EIN'] >= 100000) & (operators_df['EIN'] <= 999999)]
    op_merged = pd.merge(monthly_dhrm_data, cleaned_operators, how='inner', left_on='EINint', right_on='EIN')
    print(f'Saving output data to {output_csv_path}...')
    op_merged.to_csv(output_csv_path)


def hex_bin(points_fc, hex_fc, output_fc, simple_count=True):
    '''Bin points_fc into hexes from hex_fc, adding total category counts if needed

    Args:
        points_fc (str): Path to points feature class
        hex_fc (str): Path to hexes to use for binning
        output_fc (str): Location of final output
        simple_count (bool, optional): Just bin (default) or both bin and add category counts. Defaults to True.
    '''

    pass


def update_feature_service(source_feature_class, feature_service_item_id, org, username, password=None):
    '''Overwrite feature_service_url with data from source_feature_class

    Args:
        source_feature_class (str): Path to the source data for overwritting
        feature_service_item_id (str): Item ID for the feature service to be overwritten
        org (str): URL for the target AGOL org/portal
        username (str): Portal username
        password (str, optional): Portal password. If not provided (default), script will prompt for password. Defaults to None.
    '''

    pass


if __name__ == '__main__':
    employee_data_path = Path(r'A:\monthly_data\2021_02_01.xls')

    wfh_survey_data_path = Path(r'A:\telework_survey\Teleworking Onboarding Survey_December 14, 2020_08.45.xlsx')
    wfh_csv_out_path = Path(r'A:\telework_survey\wfh_records_test.csv')
    wfh_geocoded_points_path = Path(r'A:\telework_survey\wfh.gdb\wfh_geocoded')

    operator_data_path = Path(r"A:\telework_survey\Operators.xlsx")
    operator_csv_out_path = Path(r'A:\telework_survey\operator_records_test.csv')
    operator_geocoded_points_path = Path(r'A:\telework_survey\wfh.gdb\operator_geocoded')

    locator_path = Path(r'C:\temp\locators\AGRC_CompositeLocator.loc')

    dhrm_data = get_dhrm_dataframe(employee_data_path)

    get_wfh_eins(wfh_survey_data_path, dhrm_data, wfh_csv_out_path)
    get_operator_eins(operator_data_path, dhrm_data, operator_csv_out_path)
    geocode_points(
        str(wfh_csv_out_path),
        str(wfh_geocoded_points_path),
        str(locator_path),
        'real_addr',
        'real_zip',
    )
    geocode_points(
        str(operator_csv_out_path),
        str(operator_geocoded_points_path),
        str(locator_path),
        'real_addr',
        'real_zip',
    )
