# Accessing Previous Versions of the Dataset

Older versions of the data set are stored in Allas and can be accessed by using `restic`. It is possible to:
- list old versions
- mount them locally for browsing
- extract chosen `.zip`s
- extract an entire snapshot

The backups are stored in an [Allas](https://docs.csc.fi/data/Allas/introduction/) bucket called `nlf-harvester-versioning`. The password for the backup repository is `nlf-data-at-csc`.

## Restic basics

The intended purpose for [restic](https://restic.net/) is taking backups, but as it does incremental backups with unique identifiers for each backup and is easily available on Puhti, it was chosen as the tool to use for versioning the data set.

### Getting started in Puhti

The "allas" module on Puhti comes with the restic software. It can be activated with i
```
$ module load allas
```

The output will suggest setting up connection to Allas, but it is not necessary for accessing these backups as they are public and thus don't require authentication.

You can verify that the module has been successfully loaded by checking that
```
$ restic version
```
reports the currently installed version.

### Getting started on another Linux machine

Restic is available for a variety of Linux distributions and on macOS. See [official installation instructions](https://restic.readthedocs.io/en/latest/020_installation.html) for more information. You can verify the installation by checking that
```
$ restic version
```
reports the currently installed version.

### Providing the repository URL and credentials

todo
- basic command structure, repository url
- always --no-lock
- you can set configuration with environment variables (include or not?)

```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots
enter password for repository:
repository c08c9567 opened (version 2, compression level auto)
Save(<lock/b46701cca0>) returned error, retrying after 318.869268ms: client.PutObject: Access Denied.
```


## Listing snapshots

```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots --no-lock
enter password for repository:
repository c08c9567 opened (version 2, compression level auto)
ID        Time                 Host                 Tags        Paths
--------------------------------------------------------------------------------------------------------------
2f8df9e7  2023-11-29 15:24:12  puhti-login12.bullx              /scratch/project_2006633/nlf-harvester/targets
--------------------------------------------------------------------------------------------------------------
1 snapshots
```


## Referencing old versions

Each snapshot of the dataset gets an unique identifier assigned by restic. These IDs are always alphanumeric and 8 characters long, e.g. "2f8df9e7". If it is important to know which version of the dataset was used for computations, the ID of the newest snapshot (i.e. the one whose files are currently available on Puhti) reported by `restic snapshot` should be noted.

Later it will be made possible to use the snapshot IDs as a part of the URN of the dataset to denote a specific version, but that is not yet officially supported.


## Browsing the backups

todo: how to mount (not on puhti)


## Extracting chosen files from the backup

todo


## Extracting the whole backup

> [!WARNING]
> The data set is very large (over 10 TB), so ensure you have enough storage space available
