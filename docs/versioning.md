# Accessing Previous Versions of the Dataset

Older versions of the data set are stored in Allas and can be accessed by using `restic`. It is possible to:
- list old versions
- mount them locally for browsing
- extract chosen `.zip`s
- extract an entire snapshot

The backups are stored in an [Allas](https://docs.csc.fi/data/Allas/introduction/) bucket called `nlf-harvester-versioning`.


## Restic basics

todo

- `module load allas` / install on own computer
- basic command structure, repository url
- always --no-lock
- you can set configuration with environment variables (include or not?)

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
