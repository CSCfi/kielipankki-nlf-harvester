# National Library of Finland Dataset @ CSC

- Out-of-copyright **newspapers**, **periodicals** and eventually **everything since 1771**.
- Comes with **page scans** and **OCRed text**.
- This collection is **periodically harvested** from NLF's OAI-PMH interface, and is intended to ultimately provide all of NLF's freely available material as it becomes available.
- Metadata comes as **[METS](https://en.wikipedia.org/wiki/Metadata_Encoding_and_Transmission_Standard)**, text and layout as **[ALTO](https://en.wikipedia.org/wiki/Analyzed_Layout_and_Text_Object)**, and page scans as **[JPG 2000](https://en.wikipedia.org/wiki/JPEG_2000)** files
- The dataset is currently based on [collection 861](https://digi.kansalliskirjasto.fi/collections?id=861&set_language=en) at the NLF.


## Dataset size

Approximate figures as of 2023:

- **~700K bindings** (one binding typically being one issue of a newspaper or periodical, or a book)

- **~4M pages**

- **~10 TB**

## Technical limitations

The data set is downloaded based on combined metadata from [the National Library of Finland OAI-PMH interface](https://wiki-emerita.it.helsinki.fi/display/Comhis/Interfaces+of+digi.kansalliskirjasto.fi#Interfacesofdigi.kansalliskirjasto.fi-OAI-PMH) (lists of bindings) and [METS](https://www.loc.gov/standards/mets/) files for each binding (number of pages, file formats). If for some reason the METS for a binding is not downloadable at the time of harvesting, it or any pages from the binding will not be present in the data set. Similarly, if any of the pages in either ALTO or image format are not downloadable from digi.kansalliskirjasto.fi, either temporarily or permanently, a binding can lack some files.

The METS files are also not 1-to-1 match to the public data. Currently we attempt to download all image files whose `fileGrp`'s `USE` is either `Images` or `reference` and `ID` is `IMGGRP` or `ACIMGGRP`. Both available and unavailable ALTO files look the same in the METS, so currently they are downloaded based on the access image page numbers. The scanned page files on the file system can be matched to their METS metadata by matching the number in file name to the `SEQ` attribute in the corresponding `file` element in METS. If you suspect that some data is missing, please contact [the Language Bank of Finland](https://www.kielipankki.fi/support/contact-us/).

At the moment, the harvesting does not automatically produce a list of files that were not available for download, but it will be added later.


## Accessing the dataset

### The newest version on Puhti

For immediate access on the shared file system, the newest dataset is kept in the directory `/scratch/project_2006633/nlf-harvester/zip/`. That directory should be accessible to all users on Puhti.

> [!NOTE]
> The RedHat version of `unzip` (installed on CSC supercomputers) is affected by a bug that causes error messages such as `error: End-of-centdir-64 signature not where expected (prepended bytes?)` and `error: not enough memory for bomb detection` to be displayed. To allow extracting zip files over 1 TB in size, you need to disable [zip bomb](https://en.wikipedia.org/wiki/Zip_bomb) detection by setting an environment variable:
> ```
> $ export UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE
> ```
> This will stay in effect for the duration of the active session. The files in this data set are guaranteed to not be zip bombs, but if you need to handle zip files from untrusted sources, you can re-enable the safety feature with `unset UNZIP_DISABLE_ZIPBOMB_DETECTION`.

The [recommended way](https://docs.csc.fi/computing/disk/) to process the data on Puhti is to use a suitable compute node with a SSD drive and extract parts of it to the SSD drive. On interactive compute nodes (launched with `sinteractive`), this is `$TMPDIR`, and for batch jobs, this is `$LOCAL_SCRATCH`.

For example, the following command will extract binding `1416885` into `$TMPDIR`:

`unzip /scratch/project_2006633/nlf-harvester/zip/col-861_14.zip "*/1416885/1416885/*" -d $TMPDIR`

The following will extract all metadata files:

`for filename in /scratch/project_2006633/nlf-harvester/zip/*; do unzip $filename "*METS.xml" -d $TMPDIR; done`

The resulting target directories will have segmented paths, like `1/16/163/1631/16318/16318/mets/16318_METS.xml`, and you can extract multiple times with different filters into the same target directory.

For more advanced searching and extracting, consider extracting the metadata files as above, and using them to generate a list of binding IDs of interest. See our [example](https://github.com/CSCfi/kielipankki-nlf-harvester/blob/main/docs/apptainer/filter.py) of parsing and matching, and using either `unzip` or eg. Python's [zipfile](https://docs.python.org/3/library/zipfile.html) library for extracting the files you want.

> [!WARNING]
> The dataset will be updated periodically to keep up with newly digitized bindings and remove bindings that are no longer available for public access. This means that the dataset can change while your computations are in progress, which can lead to your analysis crashing or producing inconsistent results.

Currently the only way to see if that has happened is to check the last edit time of the zip files by running `ls -l /scratch/project_2006633/nlf-harvester/zip/` on Puhti: if it is before your job started, your results have not been affected by an update.

### Previous versions in Allas

Versions of the data set will also be made available as [`restic`](https://restic.net/) backups on [Allas](https://docs.csc.fi/data/Allas/introduction/). The retention policy of the previous versions is still open, so don't rely on old versions being available in the long term.

See [versionig.md](versioning.md) for more information about previous versions of the dataset and how to access them.


## Structure of the dataset

Each `.zip` file in the distribution contains all the binding IDs with a particular prefix. For example, the file `col-861_13.zip` contains all the bindings with IDs beginning `13`, like `130010` and `1329879` while `col-861_3.zip` contains bindings with IDs beginning with `3`, like `30038` and `399915`.

Inside the zips, each binding is in its own directory, named with the binding ID number, and found at the end of a directory hierarchy. In the hierarchy, the first directory contains the first digit of the binding IDs contained within it, and each successive directory name has one more digit from the binding IDs contained. So the files for binding ID `123012` are found in directory
`1/12/123/1230/12301/123012/123012`. Note that the full binding ID is repeated twice at the end of the hierarchy: this prevents the same path containing both files for an individual binding, and further directory hierarchy for other bindings.

For each binding, we have three kinds of files:
- A [METS](https://www.loc.gov/standards/mets/) file containing metadata  about the binding, under `mets/`
- [ALTO](https://www.loc.gov/standards/alto/) files containing the text and layout information as reported by OCR performed by The National Library of Finland, under `alto/`
- The image files (most often JPEG2000 files, ending in `.jp2`, but other formats such as TIFF may also be present) containing the scanned pages, under `access_img/`

The files for a four-page binding with ID 123 would be found in the zip with the following directory structure:
```
.
└── 1/
    └── 12/
        └── 123/
            └── 123/
                ├── mets/
                │   └── 123_METS.xml
                ├── alto/
                │   ├── 00001.xml
                │   ├── 00002.xml
                │   ├── 00003.xml
                │   └── 00004.xml
                └── access_img/
                    ├── 00001.jp2
                    ├── 00002.jp2
                    ├── 00003.tif
                    └── 00004.jp2
```
