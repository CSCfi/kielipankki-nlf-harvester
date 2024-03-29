# Accessing Previous Versions of the Dataset

Older versions of the data set are stored in Allas and can be accessed by using `restic`. It is possible to:
- list old versions
- mount them locally for browsing
- extract chosen `.zip`s
- extract an entire snapshot

The backups are stored in an [Allas](https://docs.csc.fi/data/Allas/introduction/) bucket called `nlf-harvester-versioning`. The password for the backup repository is `nlf-data-at-csc`.

## Cheatsheet

On Puhti you need the `allas` module:
```
$ module load allas
```

Setting the necessary configuration as environment variables:
```
$ export RESTIC_REPOSITORY="s3:https://a3s.fi/nlf-harvester-versioning"
$ export RESTIC_PASSWORD="nlf-data-at-csc"
```

List snapshots:
```
$ restic snapshots --no-lock
```

Restore a single file from a specific snapshot into a [fast temporary disk](https://docs.csc.fi/computing/disk/#temporary-local-disk-areas) on Puhti:
```
$ restic restore 6fd4a7e4 -i col-861_16.zip -t $TMPDIR/example-directory/ --no-lock
```


## Restic basics

The intended purpose for [restic](https://restic.net/) is taking backups, but as it does incremental backups with unique identifiers for each backup and is easily available on Puhti, it was chosen as the tool to use for versioning the data set.

> [!NOTE]
> The Allas bucket is read-only for users, so you always need to use the `--no-lock` flag with this data set to prevent restic trying to create a lock file in the bucket.

This guide will not cover all functionality restic offers. For more information, see [the official documentation](https://restic.readthedocs.io). You can also get information about available commands and flags by running `restic --help` in command line, and more detailed information about a specific command with `restic [command] --help`, e.g. `restic restore --help`.


### Getting started in Puhti

The "allas" module on Puhti comes with the restic software. It can be activated with
```
$ module load allas
```

The output will suggest setting up connection to Allas, but it is not necessary for accessing these backups as they are public and thus don't require authentication.

You can verify that the module has been successfully loaded by checking that `restic version`
reports the currently installed version.

### Getting started on another Linux machine

Restic is available for a variety of Linux distributions and on macOS. See [official installation instructions](https://restic.readthedocs.io/en/latest/020_installation.html) for more information. You can verify the installation by checking that `restic version` reports the currently installed version.

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
$ export RESTIC_REPOSITORY_FILE="/users/yourusername/example/restic_repo.txt"
$ export RESTIC_PASSWORD_FILE="/users/yourusername/example/restic_password.txt"
```
and run restic commands without providing them again, e.g. `restic snapshots --no-lock`.

Another option is to set the repository and password directly, without having them in a file:
```
$ export RESTIC_REPOSITORY="s3:https://a3s.fi/nlf-harvester-versioning"
$ export RESTIC_PASSWORD="nlf-data-at-csc"
```

### Common problems

#### Access denied

If you get `client.PutObject: Access Denied` when you try to access a repository using the correct password, you are either using a command that alters the data in the repository (e.g. trying to back up new data or remove old snapshots) or have forgotten to use the `--no-lock` flag. The solution is to stick to read-only operations (e.g. `snapshots`, `mount` and `restore`) and to always add the `--no-lock` flag.

The error message in question can look like something like this:
```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots
enter password for repository:
repository c08c9567 opened (version 2, compression level auto)
Save(<lock/b46701cca0>) returned error, retrying after 318.869268ms: client.PutObject: Access Denied.
```
As the output suggests, restic will automatically retry after a while. You can stop the retry loop with ctrl-c.

#### Slowness

The repository is big, so it is normal that operations do take some time: seconds for a listing directory contents in a mounted repository, up to a minute for listing snapshots, tens of minutes to multiple hours for fetching a single zip (depending on size). If things seem slower than they should, some of the following might be the reason:
- Unreliable or slow connection, e.g. unreliable wifi
- Not enough CPU available for fast de-encryption and decompression
- Using `mount` or `dump` instead of `restore` when fetching large files

If doing heavy work against the repository is slow on your machine, you can likely improve performance by using CSC's environments such as Puhti.


#### Access key ID does not exist

If you have other S3 credentials available on your system (e.g. AWS or MINIO credentials files or environment variables), restic can erroneously try to use them. This can result in the following error message:
```
Fatal: unable to open config file: Stat: The access key ID you provided does not exist in our records.
Is there a repository at the following location?
s3:https://a3s.fi/nlf-harvester-versioning
```

To get rid of it, you need to clear the other credentials.


#### Unsupported repository version

The repository format has been improved when the software has been updated, so older versions of restic are not compatible with new repositories. Trying to access the repository with too old restic will produce an error message stating that the repository version is unsupported. To get rid of the error, you need to update your restic installation. This can usually be done with `restic self-update` command.


## Listing snapshots

To see which older versions of the data set are available or to note the identifier of the version you have used in your research, you need to use `restic snapshots` command. It will produce an output that lists all available snapshots including their identifiers and date and time of initiating the snapshot creation.

The command and its output can look like something like this:
```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning snapshots --no-lock
repository c08c9567 opened (repository version 2) successfully, password is correct
ID        Time                 Host                 Tags        Paths
--------------------------------------------------------------------------------------------------------------
2f8df9e7  2023-11-29 15:24:12  puhti-login12.bullx              /scratch/project_2006633/nlf-harvester/targets
6fd4a7e4  2023-12-17 22:18:59  puhti-login12.bullx              /scratch/project_2006633/nlf-harvester/targets
--------------------------------------------------------------------------------------------------------------
2 snapshots
```


## Referencing old versions

Each snapshot of the dataset gets an unique identifier assigned by restic. These IDs are always alphanumeric and 8 characters long, e.g. "2f8df9e7". If it is important to know which version of the dataset was used for computations, the ID of the newest snapshot (i.e. the one whose files are currently available on Puhti) reported by `restic snapshot` should be noted.

Later it will be made possible to use the snapshot IDs as a part of the URN of the dataset to denote a specific version, but that is not yet officially supported.


## Browsing the backups

It is possible to browse the old versions of the data without extracting the backup if the repository is mounted on your local system.

> [!NOTE]
> Due to security reasons, users cannot create mounts on CSC supercomputers, so this will not work on Puhti.

First you must create a directory in which the backups will be mounted, e.g.
```
$ mkdir example/backup-browsing
```
and then you can use `restic mount` to mount the repository there:
```
$ restic -r s3:https://a3s.fi/nlf-harvester-versioning mount example/backup-browsing --no-lock
enter password for repository:
repository c08c9567 opened (version 2, compression level auto)
[0:09] 100.00%  109 / 109 index files loaded
Now serving the repository at example/backup-browsing
Use another terminal or tool to browse the contents of this folder.
When finished, quit with Ctrl-c here or umount the mountpoint.
```

The terminal in which you executed the mount command will remain reserved as long as the mount is active, as restic will need to provide the content from the repository. If you open another terminal window, you will be able to browse the mounted repository and read files within.

In the mount directory, there will be four directories:
```
$ ls example/backup-browsing/
hosts  ids  snapshots  tags
```
Out of these, `hosts` is not interesting (as the repository only contains backups from one host). The snapshots in the repository are currently not tagged, so `tags` will be empty. The other two directories, `ids` and `snapshots` offer two ways of browsing specific snapshots: `ids` will offer each snapshhot in a directory whose name is the ID of the snapshot (e.g. `ids/2f8df9e7`) while `snapshots` organizes the snapshots by their creation timestamp (e.g. `snapshots/2023-11-29T15:24:12+02:00`). The `snapshots` directory also offers a handy symbolic link `latest` which leads to the newest snapshot.  The same files are available through either of these routes.

Each snapshot directory contains the full directory tree from root up to the individual files, so for example the zip containing 18-prefixed bindings in snapshot 2f8df9e7 can be found in `ids/2f8df9e7/scratch/project_2006633/nlf-harvester/targets/col-861_18.zip`.

The mounted repository can be browsed and the files read using the normal command line tools or even by browsing in a graphical file browser. For example:
```
$ cd example/backup-browsing/ids/2f8df9e7/scratch/project_2006633/nlf-harvester/targets/
$ ls
col-861_10.zip  col-861_13.zip  col-861_16.zip  col-861_19.zip  col-861_4.zip  col-861_7.zip
col-861_11.zip  col-861_14.zip  col-861_17.zip  col-861_2.zip   col-861_5.zip  col-861_8.zip
col-861_12.zip  col-861_15.zip  col-861_18.zip  col-861_3.zip   col-861_6.zip  col-861_9.zip
$ unzip -l col-861_18.zip  | head
Archive:  col-861_18.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
    52705  2023-11-13 20:29   ./1/18/181/1816/18165/18165/mets/18165_METS.xml
   403607  2023-11-13 20:29   ./1/18/181/1816/18165/18165/alto/00001.xml
   929380  2023-11-13 20:29   ./1/18/181/1816/18165/18165/alto/00002.xml
   852434  2023-11-13 20:29   ./1/18/181/1816/18165/18165/alto/00003.xml
   485330  2023-11-13 20:29   ./1/18/181/1816/18165/18165/alto/00004.xml
  2934918  2023-11-13 20:29   ./1/18/181/1816/18165/18165/access_img/pr-00001.jp2
  3566746  2023-11-13 20:29   ./1/18/181/1816/18165/18165/access_img/pr-00002.jp2
$ unzip -j col-861_18.zip ./1/18/181/1816/18165/18165/mets/18165_METS.xml -d ~/example/extracted
Archive:  col-861_18.zip
  inflating: /home/ajarven/example/extracted/18165_METS.xml
```

## Downloading chosen files from the backup

If you already know which individual file(s) you need, you can extract them using `restic restore`. You need to provide snapshot identifier (e.g. `2f8df9e7` or `latest`) a destination directory using the `-t` or `--target` flag, and one or more files or patterns to include (`-i`/`--include`) or exclude (`-e`/`--exclude`). If you use `--include`, everything else is automatically excluded.

> [!NOTE]
> When using `restore`, restic will restore the full directory structure leading to the selected file(s). For example in the above example, the acual zip file will be found in `example/restic-restore/scratch/project_2006633/nlf-harvester/targets/col-861_16.zip`

Restoring a single zip from version 2f8df9e7 to a given directory:
```
$ restic restore 2f8df9e7 -i col-861_16.zip -t example/restic-restore/ --no-lock
repository c08c9567 opened (version 2, compression level auto)
[0:11] 100.00%  109 / 109 index files loaded
restoring <Snapshot 2f8df9e7 of [/scratch/project_2006633/nlf-harvester/targets] at 2023-11-29 15:24:12.699553873 +0200 EET by robot_2006633_puhti@puhti-login12.bullx> to example/restic-restore/
Summary: Restored 5 / 1 files/dirs (3.866 GiB / 3.866 GiB) in 30:42
```

> [!TIP]
> On Puhti, you have two options for saving the data: [scratch](https://docs.csc.fi/computing/disk/#scratch-directory) and [temporary disk](https://docs.csc.fi/computing/disk/#temporary-local-disk-areas). Reading and writing from scratch (e.g. `/scratch/project_1234`) is slower but more quota is available. Reading from `$TMPDIR`, on the other hand, is fast, but storage space is more limited. See e.g. [Using CSC environment efficiently course material on disk areas](https://csc-training.github.io/csc-env-eff/#3-disk-areas) for more information.

## Extracting the whole backup

> [!WARNING]
> The data set is very large (order of 10 TB), so ensure you have enough storage space available!

Invoking `restore` without providing any inclusion or exclusion criteria will restore the whole backup. For example getting the full version `2f8df9e7` to `example/restic-restore/` can be achieved with
```
$ restic restore 2f8df9e7 -t example/restic-restore/ --no-lock
```
