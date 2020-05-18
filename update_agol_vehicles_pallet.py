from pathlib import Path

import arcgis
import arcpy

import fleetshare_secrets as secrets

source_path = secrets.CSV_PATH

scratch_path = Path(arcpy.env.scratchGDB)
temp_fc_path = scratch_path / 'vehicle_locations'

arcpy.management.XYTableToPoint(source_path, str(temp_fc_path), 'LONGITUDE', 'LATITUDE')