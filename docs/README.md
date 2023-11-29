# National Library of Finland Dataset @ CSC

Out-of-copyright publications since 1771, with page scans and OCRed text. This collection is periodically harvested from NLF's OAI-PMH interface, and is inteded to ultimately provide all of NLF's freely available material as it becomes available.

## Dataset size as of 2023

TODO

- XXX bindings (one binding typically being one issue of a newspaper or periodical, or a book)

- XXX pages

- XXX tokens

## Accessing the dataset

### Allas

### Puhti

TODO

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


## Structure of the dataset

The most recent version of the data set is located on Puhti supercomputer (see
https://docs.csc.fi/computing/ for more details) as a collection zips. They are
found in `/scratch/project_2006633/nlf-harvester/targets`. Each zip contains
bindings with a shared binding ID prefix, e.g. file col-861_13.zip contains
bindings 130010 and 1329879 while col-861_3.zip contains 30038 and 399915.

Inside the zips, each binding is in its own directory, named with the binding
ID number and found at the end of a directory hierarchy in which each
successive directory name contains one more digit from the binding ID. Thus
e.g. the files for binding with ID `123012` are found in directory
`1/12/123/1230/12301/123012/123012`.  Note that the full binding ID is present
twice in the path: this prevents the same path containing both files for an
individual binding and further directory hierarchy for other bindings with the
same binding ID prefix.

For each binding, we have three kinds of files:
- the [METS](https://www.loc.gov/standards/mets/) file containing metadata
  about the binding (found in `mets` subdirectory)
- the [ALTO](https://www.loc.gov/standards/alto/) files containing the text on
  pages as reported by OCR performed by The National Library of Finland (found
  in the `alto` subdirectory)
- the image files (most often JPEG2000 but other formats such as tiff are also
  present) containing the scanned pages (found in the `access_img`
  subdirectory)

Thus e.g. the files for an imaginary four-page binding with ID 123 would be
found in the zip as the following directory structure (all other bindings and
their directories omitted):
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
