import datetime
import os
import sys

from pathlib import Path
from urllib.error import HTTPError

import arcgis
import arcpy
import pysftp

from forklift.models import Pallet

import fleetshare_secrets as secrets


def get_latest_csv(temp_csv_dir, log, limit_three_days=False):
    '''
    Returns the Path object and date of the latest 'vehicle_data_*.csv' file in
    temp_csv_dir. Will fail if limit_three_days is True and the date on the
    latest csv does not fall within the three preceding days.
    '''

    #: get list of csvs
    temp_dir_path = Path(temp_csv_dir)
    csvs = sorted(temp_dir_path.glob('vehicle_data_*.csv'))

    #: The last of the sorted list of csvs should be the latest
    latest_csv = csvs[-1]

    #: Pull the date out of vehicle_data_yyyymmdd.csv to check recency
    date_string = str(latest_csv).rsplit('_')[-1].split('.')[0]
    try:
        csv_datetime = datetime.date(int(date_string[:4]),
                                     int(date_string[4:6]),
                                     int(date_string[6:]))
    except ValueError as e:
        raise e

    #: Only continue if the latest is within three days
    today = datetime.date.today()
    last_three_days = [today - datetime.timedelta(days=i) for i in range(4)]
    if limit_three_days and csv_datetime not in last_three_days:
        message = (f'Latest csv "{latest_csv}" not within three days of'
                   f' today ({today})')
        log.info(message)
        sys.exit(message)

    return latest_csv, date_string


def get_map_layer(project_path, fc_to_add, log):
    '''
    Get a reference to map and layer objects that can be used to create a
    service definition.
    project_path:       A path to a ArcGIS Pro project with only one map and no
                        other layers (all layers will be removed)
    fc_to_add:          A path to the feature class containing the features to
                        be uploaded to ArcGIS Online. Will be added as a layer
                        to the project.

    returns: arcpy.mp.Layer and arcpy.mp.Map object references
    '''

    log.info(f'Getting map from {project_path}...')
    project = arcpy.mp.ArcGISProject(project_path)
    sharing_map = project.listMaps()[0]
    for layer in sharing_map.listLayers():
        log.info(f'Removing {layer} from {sharing_map.name}...')
        sharing_map.removeLayer(layer)

    log.info(f'Adding {fc_to_add} as layer to {sharing_map.name}...')
    layer = sharing_map.addDataFromPath(fc_to_add)
    project.save()
    
    return layer, sharing_map


def update_agol_feature_service(sharing_map, layer, feature_service_name,
                                sddraft_path, sd_path, sd_item):
    '''
    Helper method for updating an AGOL hosted feature service from an ArcGIS
    Pro arcpy.mp.Map and .Layer ojbects.

    sharing_map:            An arcpy.mp.Map object containing the layer to be
                            shared.
    layer:                  The arcpy.mp.Layer object created from the feature
                            class that holds your new data.
    feature_service_name:   The name of the existing Hosted Feature Service.
                            Must match exactly, otherwise the publish step will
                            fail.
    sddraft_path, sd_path:  Strings of the paths to save the service definition
                            draft and final files.
    sd_item:                The URL of the service definition item on AGOL
                            originally used to publish the hosted feature
                            service.
    '''

    sharing_draft = sharing_map.getWebLayerSharingDraft(
        'HOSTING_SERVER', 'FEATURE', feature_service_name, [layer])
    sharing_draft.exportToSDDraft(sddraft_path)
    arcpy.server.StageService(sddraft_path, sd_path)
    sd_item.update(data=sd_path)
    sd_item.publish(overwrite=True)

class AGOLVehiclesPallet(Pallet):
    def requires_processing(self):
        #: No crates, run process every time
        return True

    def process(self):
        
        #: Set up paths and directories
        feature_service_name = secrets.FEATURE_SERVICE_NAME

        temp_csv_dir = os.path.join(arcpy.env.scratchFolder, 'fleet')
        temp_fc_path = os.path.join(arcpy.env.scratchGDB, feature_service_name)
        sddraft_path = os.path.join(arcpy.env.scratchFolder, 
                                    f'{feature_service_name}.sddraft')
        sd_path = sddraft_path[:-5]

        paths = [temp_csv_dir, temp_fc_path, sddraft_path, sd_path]
        for item in paths:
            if arcpy.Exists(item):
                self.log.info(f'Deleting {item} prior to use...')
                arcpy.Delete_management(item)
        os.mkdir(temp_csv_dir)

        if not secrets.KNOWNHOSTS or not os.path.isfile(secrets.KNOWNHOSTS):
            raise FileNotFoundError(
                f'known_hosts file {secrets.KNOWNHOSTS} not found. Please '
                'create with ssh-keyscan.')

        #: Download all the files in the upload folder on sftp to temp_csv_dir
        self.log.info(
            f'Downloading all files from {secrets.KNOWNHOSTS}/upload...')
        connection_opts = pysftp.CnOpts(knownhosts=secrets.KNOWNHOSTS)
        with pysftp.Connection(
                secrets.SFTP_HOST, username=secrets.SFTP_USERNAME, 
                password=secrets.SFTP_PASSWORD, cnopts=connection_opts) as sftp:
            sftp.get_d('upload', temp_csv_dir, preserve_mtime=True)

        source_path_object, source_date = get_latest_csv(
            temp_csv_dir, self.log, limit_three_days=True)
        source_path = str(source_path_object)

        self.log.info(
            f'Converting {source_path} to feature class {temp_fc_path}...')
        wgs84 = arcpy.SpatialReference(4326)
        result = arcpy.management.XYTableToPoint(
            source_path, temp_fc_path, 'LONGITUDE', 'LATITUDE', 
            coordinate_system=wgs84)

        try:
            #: Overwrite existing AGOL service
            self.log.info(f'Connecting to AGOL as {secrets.AGOL_USERNAME}...')
            gis = arcgis.gis.GIS(
                'https://www.arcgis.com', secrets.AGOL_USERNAME,
                secrets.AGOL_PASSWORD)
            sd_item = gis.content.get(secrets.SD_ITEM_ID)

            self.log.info('Getting map and layer...')
            layer, fleet_map = get_map_layer(
                secrets.PROJECT_PATH, temp_fc_path, self.log)

            #: draft, stage, update, publish
            self.log.info(f'Staging and updating...')
            update_agol_feature_service(
                fleet_map, layer, feature_service_name, sddraft_path,
                sd_path, sd_item)

            #: Update item description
            self.log.info('Updating item description...')
            feature_item = gis.content.get(secrets.FEATURES_ITEM_ID)
            description = ('Vehicle location data obtained from Fleet; '
                        f'updated on {source_date}')
            feature_item.update(item_properties={'description': description})
        except HTTPError as e:
            self.log.info(f'Connection error with {e.url}, probably related to '
                'connection with AGOL.')
            raise e

if __name__ == '__main__':
    pallet = AGOLVehiclesPallet()
    pallet.configure_standalone_logging()
    pallet.process()