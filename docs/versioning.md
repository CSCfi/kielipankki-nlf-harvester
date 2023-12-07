# Accessing Previous Versions of the Dataset

Older versions of the data set are stored in Allas and can be accessed by using `restic`. It is possible to:
- list old versions
- mount them locally for browsing
- extract chosen `.zip`s
- extract an entire snapshot

The backups are stored in an [Allas](https://docs.csc.fi/data/Allas/introduction/) bucket called `nlf-harvester-versioning`. The password for the backup repository is `nlf-data-at-csc`.

## Restic basics

The intended purpose for [restic](https://restic.net/) is taking backups, but as it does incremental backups with unique identifiers for each backup and is easily available on Puhti, it was chosen as the tool to use for versioning the data set.

> [!NOTE]
> The Allas bucket is read-only for users, so you always need to use the `--no-lock` flag to prevent restic trying to create a lock file in it.

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

To access data, restic needs to know which repository and credentials to use. The simplest way to do this is to use the `-r` or `--repository` flag and input the password (nlf-data-at-csc) interactively when restic asks for it. This means using commands like `restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots --no-lock`.

If you have scripts that rely on fetching data with restic, you can also provide the URL and password in files or environment variables. Usually it is important to be cautious when handling passwords, making sure to only keep them in places not readable for others, but as this repository only has public data and the password is openly shared, in this case it is fine to store it in a file without encryption or store it as an environment variable.

#### Values via files

To provide information via files, you need to create text files that contain only the desired values, e.g. (you can also use a text editor such as `nano`)
```
$ echo "s3:https://a3s.fi/nlf-harvester-versioning" > example/restic_repo.txt
$ echo "nlf-data-at-csc" > example/restic_password.txt
```
and then provide paths to those files when invoking restic:
```
$ restic --repository-file example/restic_repo.txt --password-file example/restic_password.txt snapshots --no-lock
```

The paths can be relative (e.g. `example/restic_password.txt`) or absolute (e.g. `/users/yourusername/example/restic_password.txt`). The latter is useful if you run commands in different directories.


#### Values via environment variables

To shorten the commands and make them more readable, you can also provide the values via environment variables. [The environment variable section in restic documentation](https://restic.readthedocs.io/en/latest/040_backup.html#environment-variables) has a full list of available environment variables, but in this case we would be using either `RESTIC_REPOSITORY_FILE` and `RESTIC_PASSWORD_FILE` or `RESTIC_REPOSITORY` and `RESTIC_PASSWORD`.

> [!NOTE]
> The environment variables are in effect for one session only, meaning that if you close the terminal window you are using or disconnect from Puhti, you need to set them again.

The former requires the values to be present in the files as described in [values via files](#values-via-files). After that, you can set the environment variables
```
export RESTIC_REPOSITORY_FILE="/users/yourusername/example/restic_repo.txt"
export RESTIC_PASSWORD_FILE="/users/yourusername/example/restic_password.txt"
```
and run restic commands without providing them again, e.g. `restic snapshots --no-lock`.

Another option is to set the repository and password directly, without having them in a file:
```
export RESTIC_REPOSITORY="s3:https://a3s.fi/nlf-harvester-versioning"
export RESTIC_PASSWORD="nlf-data-at-csc"
``

### Common problems

If you get `client.PutObject: Access Denied` when you try to access a repository using the correct password, you are either using a command that alters the data in the repository (e.g. trying to back up new data or remove old snapshots) or have forgotten to use the `--no-lock` flag. The solution is to stick to read-only operations (e.g. `snapshots`, `mount` and `restore`) and to always add the `--no-lock` flag.

The error message in question can look like something like this:
```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots
enter password for repository:
repository c08c9567 opened (version 2, compression level auto)
Save(<lock/b46701cca0>) returned error, retrying after 318.869268ms: client.PutObject: Access Denied.
```
As the output suggests, restic will automatically retry after a while. You can stop the retry loop with ctrl-c.


todo:
- perf on Puhti (does changing tmpdir/cachedir help? interactive session with more cpu?)


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
