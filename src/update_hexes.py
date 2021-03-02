#: To create hexes:
#: Summarize within- StateHex_5sqmi, wfh_eins, out to wfh_eins_date_5mihex

from os.path import join
from pathlib import Path

import numpy as np
import pandas as pd

import arcpy
import arcgis


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


def hex_bin(points_fc, hex_fc, output_fc, simple_count=True, within_table=None):
    '''Bin points_fc into hexes from hex_fc, adding total category counts if needed

    Args:
        points_fc (str): Path to points feature class
        hex_fc (str): Path to hexes to use for binning
        output_fc (str): Location of final output
        simple_count (bool, optional): Just bin (default) or both bin and add category counts. Defaults to True.
        within_table (bool, optional): Output table for bin grouping if simple_count=False. Defaults to None.
    '''

    #: Works in the built-in interpreter, but not in conda??? WTF???
    print('Summarizing...')
    #: First, create a layer and dq to remove null geometries
    query = "Status = 'M'"
    arcpy.management.MakeFeatureLayer(points_fc, 'points_layer', query)
    print(arcpy.management.GetCount('points_layer'))
    print(arcpy.management.GetCount(hex_fc))

    #: Run a simple summarize and return if groupings aren't needed
    if simple_count:
        arcpy.analysis.SummarizeWithin(hex_fc, 'points_layer', output_fc, keep_all_polygons='ONLY_INTERSECTING')
        return

    #: Otherwise, summarize with groupings and add group info to output_fc
    arcpy.analysis.SummarizeWithin(
        hex_fc,
        'points_layer',
        output_fc,
        keep_all_polygons='ONLY_INTERSECTING',
        group_field='USER_DEPT_NAME',
        out_group_table=within_table
    )

    #: Loop through all values in DEPT_NAME, creating a field for each and then adding counts from out_grouped_table to these fields, then create a custom popup with Arcade and a host of filters from that?

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
    insert_fields.extend([name.replace(' ', '_') for name in all_departments])
    with arcpy.da.UpdateCursor(output_fc, insert_fields) as updater:
        for row in updater:
            join_id = row[0]
            new_list = [join_id]
            for department in all_departments:
                new_list.append(joins_dict[join_id][department])
            row = new_list
            updater.updateRow(row)


def symbolize_new_layer(new_data, template_layer, output_layer_file):
    '''Create a .lyrx file from new_data symbolized according to template_layer

    Args:
        new_data (str): Path to new data
        template_layer (str): Path to layer file symbolized according to desired output
        output_layer_file (str): Output path for new .lyrx file.
    '''

    new_layer = arcpy.management.ApplySymbologyFromLayer(new_data, template_layer)
    arcpy.management.SaveToLayerFile(new_layer, output_layer_file)


def add_layer_to_map(project_path, map_name, lyrx_file):
    '''Add lyrx_file to map_name in project_path, return the map and layer objects for future sharing

    Args:
        project_path (str): Path to the .aprx project file
        map_name (str): Name of the desired map within the project
        lyrx_file (str): Path to the .lyrx file to add to the map

    Returns:
        (arcpy.mp.Layer, arcpy.mp.Map): Tuple of layer and map objects.
    '''

    print(f'Getting {map_name} from {project_path}...')
    project = arcpy.mp.ArcGISProject(project_path)
    sharing_map = project.listMaps(map_name)[0]

    print(f'Adding {lyrx_file} as layer to {sharing_map.name}...')
    #: Do we need to first create the layer object, or can we pass directly using addDataFromPath?
    # layer_file = arcpy.mp.LayerFile(lyrx_file)
    layer = sharing_map.addDataFromPath(lyrx_file)
    project.save()

    return layer, sharing_map


def update_agol_feature_service(
    sharing_map, layer, feature_service_name, sddraft_path, sd_path, sd_item, feature_layer_item
):
    '''Helper method for updating an AGOL hosted feature service from an ArcGIS Pro arcpy.mp.Map and .Layer objects.

    Args:
        sharing_map (arcpy.mp.Map): Map object containing the layer to be shared.
        layer (arcpy.mp.Layer): Layer object created from the feature class that holds your new data.
        feature_service_name (str): Name of the existing Hosted Feature Service. Must match exactly, otherwise the publish step will fail.
        sddraft_path (str): Path to save the service definition draft
        sd_path (str): Path to save the service definition.
        sd_item (arcgis.Item): Service definition item on AGOL originally used to publish the hosted feature service.
        feature_layer_item (arcgis.Item): Target feature service AGOL item
    '''

    #: TODO: make these two files internal temp files, not parameters
    sddraft_path = join(arcpy.env.scratchFolder, f'{feature_service_name}.sddraft')
    sd_path = sddraft_path[:-5]
    for item in [sddraft_path, sd_path]:
        if arcpy.Exists(item):
            print(f'Deleting {item} prior to use...')
            arcpy.Delete_management(item)

    #: Get item info that can get overwritten
    item_information = {
        'title': feature_layer_item.title,
        'tags': feature_layer_item.tags,
        'snippet': feature_layer_item.snippet,
        'description': feature_layer_item.description,
        'accessInformation': feature_layer_item.accessInformation
    }
    thumbnail = feature_layer_item.download_thumbnail()

    sharing_draft = sharing_map.getWebLayerSharingDraft('HOSTING_SERVER', 'FEATURE', feature_service_name, [layer])
    sharing_draft.exportToSDDraft(sddraft_path)
    arcpy.server.StageService(sddraft_path, sd_path)
    sd_item.update(data=sd_path)
    sd_item.publish(overwrite=True)

    #: Reapply item info
    print('Resetting all of the stuff that publishing breaks...')
    #: get new copy of feature layer item after republishing-- Do we need this? (please say no)
    # feature_layer_item = retry(lambda: arcgis.gis.Item(self.gis, feature_layer_id), self.log)
    feature_layer_item.update(item_information, thumbnail=thumbnail)


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


if __name__ == '__main__':
    employee_data_path = Path(r'A:\monthly_data\2021_02_01.xls')

    wfh_survey_data_path = Path(r'A:\telework_survey\Teleworking Onboarding Survey_December 14, 2020_08.45.xlsx')
    wfh_csv_out_path = Path(r'A:\telework_survey\wfh_records_test.csv')
    wfh_geocoded_points_path = Path(r'A:\telework_survey\wfh.gdb\wfh_geocoded')
    wfh_hexes_fc_path = Path(r'A:\telework_survey\wfh.gdb\wfh_hexes')
    wfh_lyrx_path = Path(r'A:\telework_survey\wfh_hexes_layer.lyrx')
    wfh_sd_itemid = ''
    wfh_fs_itemid = ''
    wfh_fs_name = ''

    operator_data_path = Path(r"A:\telework_survey\Operators.xlsx")
    operator_csv_out_path = Path(r'A:\telework_survey\operator_records_test.csv')
    operator_geocoded_points_path = Path(r'A:\telework_survey\wfh.gdb\operator_geocoded')

    locator_path = Path(r'C:\temp\locators\AGRC_CompositeLocator.loc')
    hex_fc_path = Path(r'C:\gis\Projects\Maps2020\Maps2020.gdb\StateHex_5sqmi_planar')
    hex_template_lyrx_path = Path(r'')
    project_path = Path(r'')
    map_name = ''

    portal = 'https://utah.maps.arcgis.com'
    username = 'Jake.Adams@UtahAGRC'

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

    hex_bin(str(wfh_geocoded_points_path), str(hex_fc_path), str(wfh_hexes_fc_path), simple_count=True)

    symbolize_new_layer(str(wfh_hexes_fc_path), str(hex_template_lyrx_path), str(wfh_lyrx_path))

    sharing_map, sharing_layer = add_layer_to_map(str(project_path), map_name, str(wfh_lyrx_path))

    wfh_sd_item = get_item(portal, username, wfh_sd_itemid)
    wfh_fs_item = get_item(portal, username, wfh_fs_itemid)

    update_agol_feature_service(
        sharing_map, sharing_layer, wfh_fs_name, str(sddraft_path), str(sd_path), wfh_sd_item, wfh_fs_item
    )
