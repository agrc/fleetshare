import datetime
import os
import sys

from pathlib import Path

import arcgis
import arcpy
import pysftp

import fleetshare_secrets as secrets


def get_latest_csv(temp_csv_dir):
    #: get list of csvs
    temp_dir_path = Path(temp_csv_dir)
    csvs = sorted(temp_dir_path.glob('vehicle_data_*.csv'))

    #: The last of the sorted list of csvs should be the latest
    latest_csv = csvs[-1]

    #: Pull the date out of vehicle_data_yyyymmdd.csv
    date_string = str(latest_csv).rsplit('_')[-1].split('.')[0]
    try:
        csv_datetime = datetime.date(int(date_string[:4]), int(date_string[4:6]), int(date_string[6:]))
    except ValueError as e:
        raise e

    #: Only continue if the latest is within three days
    today = datetime.date.today()
    last_three_days = [today - datetime.timedelta(days=i) for i in range(4)]
    if csv_datetime not in last_three_days:
        print(f'Latest csv "{latest_csv} not within three days of today ({today})')
        sys.exit(f'Latest csv "{latest_csv} not within three days of today ({today})')

    return latest_csv


def get_map_layer(project_path, fc_to_add):
    '''
    Get a reference to map and layer objects that can be used to create a service
    definition.
    project_path:       A path to a ArcGIS Pro project with only one map and no
                        other layers (all layers will be removed)
    fc_to_add:          A path to the feature class containing the features to
                        be uploaded to ArcGIS Online. Will be added as a layer
                        to the project.

    returns: arcpy.mp.Layer and arcpy.mp.Map object references
    '''

    print(f'Getting map from {project_path}...')
    project = arcpy.mp.ArcGISProject(project_path)
    sharing_map = project.listMaps()[0]
    for layer in sharing_map.listLayers():
        print(f'Removing {layer} from {sharing_map.name}...')
        sharing_map.removeLayer(layer)

    print(f'Adding {fc_to_add} as layer to {sharing_map.name}...')
    layer = sharing_map.addDataFromPath(fc_to_add)
    project.save()
    
    return layer, sharing_map


def process():
    feature_service_name = secrets.FEATURE_SERVICE_NAME

    temp_csv_dir = os.path.join(arcpy.env.scratchFolder, 'fleet')
    temp_fc_path = os.path.join(arcpy.env.scratchGDB, feature_service_name)
    sddraft_path = os.path.join(arcpy.env.scratchFolder, f'{feature_service_name}.sddraft')
    sd_path = sddraft_path[:-5]

    paths = [temp_csv_dir, temp_fc_path, sddraft_path, sd_path]
    for item in paths:
        if arcpy.Exists(item):
            print(f'Deleting {item} prior to use...')
            arcpy.Delete_management(item)
    os.mkdir(temp_csv_dir)

    #: Download all the files in the upload folder on sftp to temp_csv_dir
    connection_opts = pysftp.CnOpts(knownhosts=secrets.KNOWNHOSTS)
    with pysftp.Connection(secrets.SFTP_HOST, 
                           username=secrets.SFTP_USERNAME, 
                           password=secrets.SFTP_PASSWORD, 
                           cnopts=connection_opts) as sftp:
        sftp.get_d('upload', temp_csv_dir, preserve_mtime=True)

    source_path = str(get_latest_csv(temp_csv_dir))

    print(f'Converting {source_path} to feature class {temp_fc_path}...')
    result = arcpy.management.XYTableToPoint(source_path, temp_fc_path, 'LONGITUDE', 'LATITUDE', coordinate_system=arcpy.SpatialReference(4326))

    #: Overwrite existing AGOL service
    print(f'Connecting to AGOL as {secrets.AGOL_USERNAME}...')
    gis = arcgis.gis.GIS('https://www.arcgis.com', secrets.AGOL_USERNAME, secrets.AGOL_PASSWORD)
    sd_item = gis.content.get(secrets.SD_ITEM_ID)

    layer, fleet_map = get_map_layer(secrets.PROJECT_PATH, temp_fc_path)

    #: draft, stage, update, publish
    print(f'Staging and updating...')
    sharing_draft = fleet_map.getWebLayerSharingDraft('HOSTING_SERVER', 'FEATURE', feature_service_name, [layer])
    sharing_draft.exportToSDDraft(sddraft_path)
    arcpy.server.StageService(sddraft_path, sd_path)
    sd_item.update(data=sd_path)
    sd_item.publish(overwrite=True)

    #: Update item description
    print('Updating item description...')
    feature_item = gis.content.get(secrets.FEATURES_ITEM_ID)
    description = f'Vehicle location data obtained from Fleet; updated on {datetime.date.today()}'
    feature_item.update(item_properties={'description': description})

if __name__ == '__main__':
    process()