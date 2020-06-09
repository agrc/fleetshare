'''
fleetshare_secrets_template.py:
Template secrets file for the update_agol_vehicles_pallet script. Copy this
file to 'fleetshare_secrets.py' and add proper values for each item.

This file IS tracked in version control; do not add sensitive data to this file.
Use the copy named 'fleetshare_secrets.py', which is excluded via the project's
.gitignore file. Verify that your local version control is excluding the copy.
'''

#: Path to csv from fleet
CSV_PATH = ''
#: Name of published hosted feature service on AGOL
FEATURE_SERVICE_NAME = ''
#: SFTP host (fqdn or IP)
SFTP_HOST = ''
#: SFTP username
SFTP_USERNAME = ''
#: SFTP password
SFTP_PASSWORD = ''
#: AGOL username
AGOL_USERNAME = ''
#: AGOL password
AGOL_PASSWORD = ''
#: URL to service definition item in AGOL
SD_ITEM_ID = ''
#: URL to features item in AGOL
FEATURES_ITEM_ID = ''
#: path to ArcGIS Pro project for staging the service
PROJECT_PATH = ''
#: path to knownhosts file for sftp connection
KNOWNHOSTS = ''
