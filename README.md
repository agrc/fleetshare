# fleetshare

Visualizing where vehicles are and how they could be shared

## update_agol_vehicles

The main part of this project is `update_agol_vehicles_pallet.py`, which automates downloading all the files from an SFTP directory, chooses the latest csv from those files by comparing the date in the filename, and then updates the data in a hosted feature service with the data from the csv.

This is built as a pallet for Forklift, but also works if called as a standalone script. Either way, it needs the Forklift environment:

1. Clone the ArcGIS Pro default conda environment and activate the clone
1. Clone the forklift repo from github
1. `cd` into the forklift repo
1. `pip install .\ -U`

After the environment is created, copy `fleetshare_secrets_template.py` to `fleetshare_secrets.py` and add the relevant info for each item. The `.gitignore` file for the project excludes `fleetshare_secrets.py` from being tracked by git. You should verify that it is not being added to version control on your local setup.

### known_hosts

The script relies on having a known_hosts file (listed in the secrets file) for verifying the identity of the sftp server prior to connecting to it. By default, this file is not included in version control (and should never be!) and must be generated prior to use. The following commands can be run on the command line to generate the known_hosts file (assuming a version of openSSH is installed):

```shell
ssh-keyscan -t rsa [sftp ip] > [output_directory]\known_hosts
ssh-keyscan -t dsa [sftp ip] > [output_directory]\known_hosts
```
