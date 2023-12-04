# National Library of Finland Dataset @ CSC

- Out-of-copyright **newspapers**, **periodicals** and eventually **everything since 1771**.
- Comes with **page scans** and **OCRed text**.
- This collection is **periodically harvested** from NLF's OAI-PMH interface, and is intended to ultimately provide all of NLF's freely available material as it becomes available.
- Metadata comes as **[METS](https://en.wikipedia.org/wiki/Metadata_Encoding_and_Transmission_Standard)**, text and layout as **[ALTO](https://en.wikipedia.org/wiki/Analyzed_Layout_and_Text_Object)**, and page scans as **[JPG 2000](https://en.wikipedia.org/wiki/JPEG_2000)** files
- The dataset is currently based on [collection 861](https://digi.kansalliskirjasto.fi/collections?id=861&set_language=en) at the NLF.

TODO: document the technical stuff that can affect completeness of the data set:
- we download files listed under USE "Images" or "reference" and their corresponding ALTOs
- how to match the image files to the metadata in METS and that this is not easy for ALTOs :---)

## Dataset size

Approximate figures as of 2023:

- **~700K bindings** (one binding typically being one issue of a newspaper or periodical, or a book)

- **~4M pages**

- **~10 TB**

## Accessing the dataset

### Puhti

For immediate access on the shared file system, the newest dataset is kept in the directory `/scratch/project_2006633/nlf-harvester/zip/`. That directory should be accessible to all users on Puhti.

The [recommended way](https://docs.csc.fi/computing/disk/) to process the data on Puhti is to use a suitable compute node with a SSD drive and extract parts of it to the SSD drive. On interactive compute nodes (launched with `sinteractive`), this is `$TMPDIR`, and for batch jobs, this is `$LOCAL_SCRATCH`.

For example, the following command will extract binding `1416885` into `$TMPDIR`:

`unzip /scratch/project_2006633/nlf-harvester/zip/col-861_14.zip "*/1416885/1416885/*" -d $TMPDIR`

The following will extract all metadata files:

`for filename in /scratch/project_2006633/nlf-harvester/zip/*; do unzip $filename "*METS.xml" -d $TMPDIR; done`

The resulting target directories will have segmented paths, like `1/16/163/1631/16318/16318/mets/16318_METS.xml`, and you can extract multiple times with different filters into the same target directory.

For more advanced searching and extracting, consider extracting the metadata files as above, and using them to generate a list of binding IDs of interest. See our [example](https://github.com/CSCfi/kielipankki-nlf-harvester/blob/main/docs/apptainer/filter.py) of parsing and matching, and using either `unzip` or eg. Python's [zipfile](https://docs.python.org/3/library/zipfile.html) library for extracting the files you want.

### Allas

Versions of the data set will also be made available as [`restic`](https://restic.net/) backups on [Allas](https://docs.csc.fi/data/Allas/introduction/). `restic` commands can be used to list old versions, and to extract either entire snapshots or single `.zip`s.

The retention policy of the previous versions is still open and the Allas versions of the data set have not yet been published.

TODO:
- allas bucket
- restic quickstart

## Versioning

Older versions of the data can be accessed by using `restic`, which also stores and displays timestamps for the archival of the previous dataset versions. The official way to reference old versions is the SHA-256 hash calculated by `restic` from the archive. The full hash is 256 bits long, but for referencing purposes, its 32-bit prefix is sufficient, and is represented by a 8-character string, like `9ed7c15f`.

TODO:
- when the data set on disk changes, how that can affect you if you have jobs running

## Structure of the dataset

Each `.zip` file in the distribution contains all the binding IDs with a particular prefix. For example, the file `col-861_13.zip` contains all the bindings with IDs beginning `13`, like `130010` and `1329879` while `col-861_3.zip` contains bindings with IDs beginning with `3`, like `30038` and `399915`.

Inside the zips, each binding is in its own directory, named with the binding ID number, and found at the end of a directory hierarchy. In the hierarchy, the first directory contains the first digit of the binding IDs contained within it, and each successive directory name has one more digit from the binding IDs contained. So the files for binding ID `123012` are found in directory
`1/12/123/1230/12301/123012/123012`. Note that the full binding ID is repeated twice at the end of the hierarchy: this prevents the same path containing both files for an individual binding, and further directory hierarchy for other bindings.

For each binding, we have three kinds of files:
- A [METS](https://www.loc.gov/standards/mets/) file containing metadata  about the binding, under `mets/`
- [ALTO](https://www.loc.gov/standards/alto/) files containing the text and layout information as reported by OCR performed by The National Library of Finland, under `alto/`
- The image files (most often JPEG2000 files, ending in `.jp2`, but other formats such as TIFF may also be present) containing the scanned pages, under `access_img/`

TODO:
- the new file naming for images

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
                    ├── pr-00001.jp2
                    ├── pr-00002.jp2
                    ├── pr-00003.jp2
                    └── pr-00004.jp2
```
