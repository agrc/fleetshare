# fleetshare

Visualizing where vehicles are and how they could be shared

## knownhosts

fleetshare relies on having a knownhosts file for verifying the identity of the sftp server prior to connecting to it. By default, this file is not included in VCS and must be generated prior to use. The following commands can be run on the command line to generate the knownhosts file (assuming a version of openSSH is installed):

```
ssh-keyscan -t rsa <sftp ip> > <output_directory>\knownhosts
ssh-keyscan -t dsa <sftp ip> > <output_directory>\knownhosts
```
