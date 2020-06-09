# fleetshare

Visualizing where vehicles are and how they could be shared

## known_hosts

fleetshare relies on having a known_hosts file for verifying the identity of the sftp server prior to connecting to it. By default, this file is not included in VCS and must be generated prior to use. The following commands can be run on the command line to generate the known_hosts file (assuming a version of openSSH is installed):

```shell
ssh-keyscan -t rsa [sftp ip] > [output_directory]\known_hosts
ssh-keyscan -t dsa [sftp ip] > [output_directory]\known_hosts
```
