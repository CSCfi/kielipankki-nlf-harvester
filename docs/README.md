# National Library of Finland Dataset on Puhti

todo: one sentence description of the dataset

All in all, this file should be fairly short: so short that the user is likely
to glance all the headings with ease. If (when) the sections become too long,
the actual content should be put into separate md files and linked from here.

## Using the Dataset on Puhti

todo
- location of data
- format of data
- most common operations (list, partial extract based on binding id/file
  type/hand-picked list)
- using the zip in python script?
- fusermount


## Versioning

todo
- allas bucket
- restic quickstart
- when the data set on disk changes, how that can affect you if you have jobs
  running


## Dataset Description

The most recent version of the data set is located on Puhti supercomputer (see
https://docs.csc.fi/computing/ for more details) as a single zip. This zip is
found in `/scratch/project_2006633/nlf-harvester/zip/col-861.zip`.

Inside the zip each binding is in its own directory, named with the binding ID
number and found at the end of a directory hierarchy in which each successive
directory name contains one more digit from the binding ID. Thus e.g. the files
for binding with ID `123012` are found in `1/12/123/1230/12301/123012/123012`.
Note that the full binding ID is present twice in the path: this prevents the
same path containing both files for an individual binding and further directory
hierarchy for other bindings with the same binding ID prefix.

For each binding, we have three kinds of files:
- the [METS](https://www.loc.gov/standards/mets/) file containing metadata
  about the binding (found in `mets` subdirectory)
- the [ALTO](https://www.loc.gov/standards/alto/) files containing the text on
  pages as reported by OCR performed by The National Library of Finland (found
  in the `alto` subdirectory)
- the JPEG 2000 files containing the scanned pages (found in the `access_img`
  subdirectory)

Thus e.g. the files for a four-page binding with ID 123 are found in the zip as
the following directory structure (all other bindings and their directories
omitted):
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
