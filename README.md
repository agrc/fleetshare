# fleetshare

Visualizing where vehicles are and how they could be shared

## Installation

1. Install the dependencies
   - `pip install -r requirements.txt`

### update_agol_vehicles

The main part of this project is `update_agol_vehicles_pallet.py`, which automates downloading all the files from an SFTP directory, chooses the latest csv from those files by comparing the date in the filename, and then updates the data in a hosted feature service with the data from the csv.

This is built as a pallet for Forklift, but also works if called as a standalone script. For a standalone script, it still relies on the Forklift environment:

1. Clone the ArcGIS Pro default conda environment and activate the clone
1. Clone the Forklift repo from github
1. `cd` into the Forklift repo
1. `pip install .\ -U` to install Forklift

If instead you want to add fleetshare to a test/production Forklift environment, add the fleetshare repo to the Forklift config:

1. `forklift config repos --add agrc/fleetshare`

After the environment is created, copy `fleetshare_secrets_template.py` to `fleetshare_secrets.py` and add the relevant info for each item. The `.gitignore` file for the project excludes `fleetshare_secrets.py` from being tracked by git. You should verify that it is not being added to version control on your local setup.

### update_hexes.py

A secondary script meant to be run manually at a certain interval (monthly, weekly, something like that) based on when the data comes from DHRM and Fleet. Uses data to update hexes in specified feature classes.

Currently not very pretty or engineered, but it works (kind of).

Usage:

1. Set all the relevant variables in the `specific_info` and `common_info` instantiations
1. Call the script from the command line twice, once for WFH and again for approved operators:
   - `python update_hexes.py w`
   - `python update_hexes.py o`

(arcpy.SummarizeWithin _really_ does not like to be called twice in the same script)

SummarizeWithin() seems to be very sensitive to data in %localappdata%\temp. If it fails with a 999999 error, clear that out. This may also be a hint for running it twice in the same script.

#### known_hosts

The script relies on having a known_hosts file (listed in the secrets file) for verifying the identity of the sftp server prior to connecting to it. By default, this file is not included in version control (and should never be!) and must be generated prior to use. The following commands can be run on the command line to generate the known_hosts file (assuming a version of openSSH is installed):

```shell
ssh-keyscan -t rsa [sftp ip] > [output_directory]\known_hosts
ssh-keyscan -t dsa [sftp ip] > [output_directory]\known_hosts
```

## Development

1. Install the development requirements
   - `pip install -r requirements-dev.txt`
