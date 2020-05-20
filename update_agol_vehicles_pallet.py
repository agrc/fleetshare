import datetime
import os

import arcgis
import arcpy

import fleetshare_secrets as secrets

source_path = secrets.CSV_PATH

feature_service_name = secrets.FEATURE_SERVICE_NAME

temp_fc_path = os.path.join(arcpy.env.scratchGDB, feature_service_name)
sddraft_path = os.path.join(arcpy.env.scratchFolder, f'{feature_service_name}.sddraft')
sd_path = sddraft_path[:-5]

paths = [temp_fc_path, sddraft_path, sd_path]
for item in paths:
    if arcpy.Exists(item):
        print(f'Deleting {item} prior to use...')
        arcpy.Delete_management(item)

print(f'Converting {source_path} to feature class {temp_fc_path}...')
result = arcpy.management.XYTableToPoint(source_path, temp_fc_path, 'LONGITUDE', 'LATITUDE', coordinate_system=arcpy.SpatialReference(4326))

#: Overwrite existing AGOL service
print(f'Connecting to AGOL as {secrets.USERNAME}...')
gis = arcgis.gis.GIS('https://www.arcgis.com', secrets.USERNAME, secrets.PASSWORD)
sd_item = gis.content.get(secrets.SD_ITEM_ID)

#: Get project references
#: Assume there's only one map in the project, remove all layers for clean map
print(f'Getting map from {secrets.PROJECT_PATH}...')
project = arcpy.mp.ArcGISProject(secrets.PROJECT_PATH)
fleet_map = project.listMaps()[0]
for layer in fleet_map.listLayers():
    print(f'Removing {layer} from {fleet_map.name}...')
    fleet_map.removeLayer(layer)

print(f'Adding {temp_fc_path} as layer to {fleet_map.name}...')
layer = fleet_map.addDataFromPath(temp_fc_path)
project.save()

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