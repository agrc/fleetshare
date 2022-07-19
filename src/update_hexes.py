'''Ok, it's not pretty, but it gets the job done.
    1. Set all the relevant variables in the specific_info and common_info classes
    2. Call from the command line twice, once for WFH and again for approved operators:
        python update_hexes.py w
        python update_hexes.py o
        (arcpy.SummarizeWithin _really_ does not like to be called twice in the same script)
'''

import datetime
from dataclasses import dataclass, field
from getpass import getpass
import logging
# from os.path import join
from pathlib import Path
from sys import argv
import sys

import numpy as np
import pandas as pd

import arcpy
import arcgis
import palletjack

import hex_secrets as secrets


@dataclass
class SpecificInfo:
    method: str
    data_source: Path
    sd_itemid: str
    fs_itemid: str
    fs_name: str
    description: str
    simple_summary: bool = field(init=False)

    def __post_init__(self):
        if self.method == 'wfh':
            self.simple_summary = True
        elif self.method == 'operator':
            self.simple_summary = False
        else:
            raise NotImplementedError(f'Method {self.method} not recognized...')


@dataclass
class CommonInfo:
    employee_data_path: Path
    locator_path: Path
    hex_fc_path: Path
    project_path: Path
    map_name: str
    portal: str
    username: str
    scratch_gdb: Path
    working_dir_path: Path
    csv_path: Path = field(init=False)
    geocoded_points_path: Path = field(init=False)
    hexes_fc_path: Path = field(init=False)
    within_table_path: Path = field(init=False)
    trimmed_hex_fc_path: Path = field(init=False)

    def __post_init__(self):
        self.csv_path = self.working_dir_path / 'ein_records.csv'
        self.geocoded_points_path = self.scratch_gdb / 'geocoded_points'
        self.hexes_fc_path = self.scratch_gdb / 'hexes'
        self.within_table_path = self.scratch_gdb / 'within_table'
        self.trimmed_hex_fc_path = self.scratch_gdb / 'trimmed_hexes'


def clean_field_name(name):
    bad_chars = [' ', '&', '`', '-']
    for char in bad_chars:
        name = name.replace(char, '_')
    return name


def get_wfh_eins(report_dir_path, monthly_dhrm_data):
    '''Create a csv of the employee data that have matching records in the WFH survey

    Args:
        report_dir_path (Path): directory of non-cumulative csvs of the WFH survey data
        monthly_dhrm_data (DataFrame): monthly employee data
    '''

    print(f'\nReading WFH surveys from {report_dir_path}...')

    wfh_reports = report_dir_path.glob('*.csv')
    report_dfs = []

    #: Read in each report, trim out weird row, and concat into a single data frame
    for report in wfh_reports:
        report_df = pd.read_csv(report, header=0)
        report_df.drop([0, 1], inplace=True)
        report_dfs.append(report_df)
    survey_df = pd.concat(report_dfs)

    #: Drop rows that aren't new or that are UTNG (they don't use EINs)
    non_update_df = survey_df[(survey_df['New'] == 'Yes') & ~(survey_df['Q5'] == 'UTNG')]

    #: Non-na EINs as a series
    eins = non_update_df[non_update_df['Q1_4'].notna()].loc[:, 'Q1_4']

    #: Drop malformed EINs (not 6 digits) and duplicate EINs
    wfh_eins_int = eins[eins.str.len() == 6].astype(int)
    wfh_eins_int = wfh_eins_int.drop_duplicates()

    #: "join" by only including rows where EINint is in series of EINs from WFH survey
    wfh_records_dataframe = monthly_dhrm_data[monthly_dhrm_data['EINint'].isin(wfh_eins_int)]
    print(
        f'Employee records with matching EIN in WFH survey: {len(wfh_records_dataframe.index)} (out of {wfh_eins_int.count()} EINs from survey)'
    )

    return wfh_records_dataframe


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

    fields_str = f"'Address or Place' {addr_field} VISIBLE NONE;Address2 <None> VISIBLE NONE;Address3 <None> VISIBLE NONE;Neighborhood <None> VISIBLE NONE;City <None> VISIBLE NONE;County <None> VISIBLE NONE;State <None> VISIBLE NONE;ZIP {zip_field} VISIBLE NONE;ZIP4 <None> VISIBLE NONE;Country <None> VISIBLE NONE"
    print(f'\nGeocoding {points_csv} (this could take a while)...')
    geocode_results = arcpy.geocoding.GeocodeAddresses(points_csv, locator, fields_str, geocode_fc)

    # arcpy.geocoding.GeocodeAddresses(r"A:\telework_survey\auto\ein_records.csv", r"C:\temp\locators\new\v101\ugrc_composite_locator.loc", "'Address or Place' real_addr VISIBLE NONE;Address2 <None> VISIBLE NONE;Address3 <None> VISIBLE NONE;Neighborhood <None> VISIBLE NONE;City <None> VISIBLE NONE;County <None> VISIBLE NONE;State <None> VISIBLE NONE;ZIP real_zip VISIBLE NONE;ZIP4 <None> VISIBLE NONE;Country <None> VISIBLE NONE", r"C:\gis\Projects\Maps2020\Maps2020.gdb\locator_test_GeocodeAddresses", "STATIC", None, '', None, "ALL")

    print(geocode_results.getMessages())

    print('Copying out only the matched points...')
    query = "Status = 'M'"

    if arcpy.Exists(geocode_fc):
        arcpy.management.Delete('geocode_layer')

    arcpy.management.MakeFeatureLayer(geocode_fc, 'geocode_layer', query)
    arcpy.management.CopyFeatures('geocode_layer', out_fc)


def _geocode_points_with_palletjack(points_df, out_fc, addr_field, zone_field):
    # points_df = pd.read_csv(points_csv)
    geocoder = palletjack.transform.APIGeocoder(secrets.API_KEY)

    print('\nGeocoding addresses (this could take a while)...')
    geocoded_df = geocoder.geocode_dataframe(points_df, addr_field, zone_field, 26912)

    print('Copying out only the matched points...')
    valid_geocoded_df = geocoded_df[geocoded_df['score'] > 70].copy()
    valid_geocoded_df.spatial.to_featureclass(out_fc, sanitize_columns=True)


def get_operator_eins(operators_path, monthly_dhrm_data):
    '''Read and process approved operator data

    Performs an inner join between the operators data and the DHRM data based on the EIN

    Args:
        operators_path (Path): xlsx of approved operators from Fleet
        monthly_dhrm_data (DataFrame): Monthly DHRM data

    Returns:
        pd.DataFrame: Operators dataframe with DHRM info added
    '''
    print(f'\nReading approved operator data {operators_path}...')
    operators_df = pd.read_excel(operators_path, engine='openpyxl')
    cleaned_operators = operators_df[(operators_df['EIN'] >= 100000) & (operators_df['EIN'] <= 999999)]
    operators_merged_dataframe = pd.merge(
        monthly_dhrm_data, cleaned_operators, how='inner', left_on='EINint', right_on='EIN'
    )

    return operators_merged_dataframe


def hex_bin(points_fc, hex_fc, output_fc, simple_count=True, within_table=None):
    '''Bin points_fc into hexes from hex_fc, adding total category counts if needed

    Args:
        points_fc (str): Path to points feature class
        hex_fc (str): Path to hexes to use for binning
        output_fc (str): Location of final output
        simple_count (bool, optional): Just bin (default) or both bin and add category counts. Defaults to True.
        within_table (str, optional): Output table for bin grouping if simple_count=False. Defaults to None.
    '''

    print('Summarizing...')

    #: Print counts as a sanity check
    print(arcpy.management.GetCount(points_fc))
    print(arcpy.management.GetCount(hex_fc))

    #: Run a simple summarize and return if groupings aren't needed
    if simple_count:
        arcpy.analysis.SummarizeWithin(hex_fc, points_fc, output_fc, keep_all_polygons='ONLY_INTERSECTING')
        return

    #: Otherwise, summarize with groupings and add group info to output_fc
    arcpy.analysis.SummarizeWithin(
        hex_fc,
        points_fc,
        output_fc,
        keep_all_polygons='ONLY_INTERSECTING',
        group_field='DEPT_NAME',  #: I think the esri geocode or something added the 'USER_' prefix
        out_group_table=within_table
    )

    print('Joining...')
    #: Get our table into a dataframe we can play with
    grouped_table_dict = {}
    with arcpy.da.SearchCursor(within_table, '*') as table_cursor:
        grouped_table_dict = {row[0]: row[1:] for row in table_cursor}

    groups_df = pd.DataFrame.from_dict(grouped_table_dict, orient='index', columns=['JoinID', 'Dept', 'Count'])
    all_departments = groups_df['Dept'].unique()

    #: pivot so each row is now a unique join id and the columns are the counts of each department's count value
    joins = pd.pivot(data=groups_df, values='Count', index='JoinID', columns=['Dept'])
    joins.fillna(0, inplace=True)

    #: dump our dataframe to a dict we can use in an insert cursor
    #: {joinid: {dept1:count, dept2:count}}
    joins_dict = joins.to_dict('index')

    #: prep output feature class
    new_fields = [[name, 'LONG'] for name in all_departments]
    arcpy.management.AddFields(output_fc, new_fields)

    print('Writing output data...')
    #: Write our new data to the output feature class
    insert_fields = ['Join_ID']
    insert_fields.extend([clean_field_name(name) for name in all_departments])
    with arcpy.da.UpdateCursor(output_fc, insert_fields) as updater:
        for row in updater:
            join_id = row[0]
            new_list = [join_id]
            for department in all_departments:
                new_list.append(joins_dict[join_id][department])
            row = new_list
            updater.updateRow(row)


def remove_single_count_hexes(input_hex_fc, output_hex_fc):
    '''Create a new feature class with hexes that only have 2 or more points

    Args:
        input_hex_fc (str): The hexagons with point counts
        output_hex_fc (str): Output path for trimmed data
    '''

    query = "Point_Count > 1"
    arcpy.management.MakeFeatureLayer(input_hex_fc, 'hex_layer', query)
    arcpy.management.CopyFeatures('hex_layer', output_hex_fc)


def get_item(portal, username, item_id, password=None):
    '''Log into an arcgis portal and get the item referenced by item_id.

    Args:
        portal (str): URL to the Portal to log in to
        username (str): Portal username
        item_id (str): AGOL item id for the desired item
        password (str, optional): Portal password. Will prompt for if not provided (Default). Defaults to None.

    Returns:
        arcgis.Item: The desired item object.
    '''

    print(f'Logging into {portal} as {username}...')
    gis = arcgis.gis.GIS(portal, username, password)
    item = gis.content.get(item_id)
    return item


def _update_feature_service_with_palletjack(feature_class_path, gis, fs_itemid, scratch_dir):
    new_dataframe = pd.DataFrame.spatial.from_featureclass(feature_class_path)
    overwriter = palletjack.FeatureServiceOverwriter(gis)
    print(f'Truncating and loading hosted feature service {fs_itemid}...')
    new_record_count = overwriter.truncate_and_load_feature_service(fs_itemid, new_dataframe, scratch_dir)
    return new_record_count


def one_function_to_rule_them_all(common_info: CommonInfo, specific_info: SpecificInfo):
    '''Calls all the previous functions in appropriate order

    Args:
        common_info (CommonInfo): Info common to all layers (wfh and operator)
        specific_info (SpecificInfo): Info specific to a particular layer (wfh or operator)

    Raises:
        NotImplementedError: If a method other than 'wfh' or 'operator' is provided
    '''

    #: Set up logging
    logger = logging.getLogger('hexes')
    logger.setLevel(logging.DEBUG)
    palletjack_logger = logging.getLogger('palletjack')
    palletjack_logger.setLevel(logging.DEBUG)

    cli_handler = logging.StreamHandler(sys.stdout)
    cli_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )
    cli_handler.setFormatter(formatter)
    logger.addHandler(cli_handler)
    palletjack_logger.addHandler(cli_handler)

    #: Log any warnings at logging.WARNING
    #: Put after everything else to prevent creating a duplicate, default formatter
    #: (all log messages were duplicated if put at beginning)
    logging.captureWarnings(True)

    print('Getting AGOL references...')
    password = getpass('Enter Password: ')
    gis = arcgis.gis.GIS(common_info.portal, common_info.username, password)
    # sd_item = gis.content.get(specific_info.sd_itemid)
    # fs_item = gis.content.get(specific_info.fs_itemid)

    #: Because Pro signs itself out randomly...
    arcpy.SignInToPortal(arcpy.GetActivePortalURL(), common_info.username, password)

    print('Cleaning up scratch areas...')
    if arcpy.Exists(str(common_info.scratch_gdb)):
        print(f'Deleting existing {common_info.scratch_gdb}...')
        arcpy.management.Delete(str(common_info.scratch_gdb))
    print(f'Creating {common_info.scratch_gdb}...')
    arcpy.management.CreateFileGDB(str(common_info.scratch_gdb.parent), str(common_info.scratch_gdb.name))

    dhrm_data = get_dhrm_dataframe(common_info.employee_data_path)

    if specific_info.method == 'wfh':
        eins_dataframe = get_wfh_eins(specific_info.data_source, dhrm_data)
    elif specific_info.method == 'operator':
        eins_dataframe = get_operator_eins(specific_info.data_source, dhrm_data)
    else:
        raise NotImplementedError(f'Method {specific_info.method} not recognized...')

    # geocode_points(
    #     str(common_info.csv_path),
    #     str(common_info.geocoded_points_path),
    #     str(common_info.locator_path),
    #     'real_addr',
    #     'real_zip',
    # )

    _geocode_points_with_palletjack(eins_dataframe, str(common_info.geocoded_points_path), 'real_addr', 'real_zip')

    hex_bin(
        str(common_info.geocoded_points_path),
        str(common_info.hex_fc_path),
        str(common_info.hexes_fc_path),
        simple_count=specific_info.simple_summary,
        within_table=str(common_info.within_table_path)
    )

    remove_single_count_hexes(str(common_info.hexes_fc_path), str(common_info.trimmed_hex_fc_path))

    new_record_count = _update_feature_service_with_palletjack(
        common_info.trimmed_hex_fc_path, gis, specific_info.fs_itemid, common_info.working_dir_path
    )
    print(f'{new_record_count} features after upload')

    # sharing_layer, sharing_map = add_layer_to_map(
    #     str(common_info.project_path), common_info.map_name, str(common_info.trimmed_hex_fc_path)
    # )

    # update_agol_feature_service(sharing_map, sharing_layer, sd_item, fs_item, specific_info)


if __name__ == '__main__':

    common_info = CommonInfo(
        employee_data_path=secrets.EMPLOYEE_DATA_PATH,
        locator_path=secrets.LOCATOR_PATH,
        hex_fc_path=secrets.HEX_FC_PATH,
        project_path=secrets.PROJECT_PATH,
        scratch_gdb=secrets.SCRATCH_GDB,
        working_dir_path=secrets.WORKING_DIR_PATH,
        map_name=secrets.MAP_NAME,
        portal=secrets.AGOL_PORTAL,
        username=secrets.AGOL_USERNAME,
    )

    wfh_info = SpecificInfo(
        method='wfh',
        data_source=secrets.WFH_DATA_SOURCE_PATH,
        sd_itemid=secrets.WFH_SD_ITEMID,
        fs_itemid=secrets.WFH_FS_ITEMID,
        fs_name=secrets.WFH_FS_NAME,
        description=(
            f'Last Updated: {datetime.datetime.today().strftime("%d %b %Y")}<br />'
            'WFH locations per 5 sq mile hex (two locations or more).'
        )
    )

    operator_info = SpecificInfo(
        method='operator',
        data_source=secrets.OPERATOR_DATA_SOURCE_PATH,
        sd_itemid=secrets.OPERATOR_SD_ITEMID,
        fs_itemid=secrets.OPERATOR_FS_ITEMID,
        fs_name=secrets.OPERATOR_FS_NAME,
        description=(
            f'Last Updated: {datetime.datetime.today().strftime("%d %b %Y")}<br />'
            'Approved operator locations per 5 sq mile hex (two locations or more). Data from Fleet.'
        )
    )

    if len(argv) != 2:
        print(
            'Syntax: `python update_hexes.py <method>`, where method is either "w" for WFH or "o" for Approved Operators'
        )
    elif argv[1] == 'w':
        one_function_to_rule_them_all(common_info, wfh_info)
    elif argv[1] == 'o':
        one_function_to_rule_them_all(common_info, operator_info)
    else:
        print(f'Method "{argv[1]}" not available.')
