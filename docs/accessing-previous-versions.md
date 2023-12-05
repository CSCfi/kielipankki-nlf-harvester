# Accessing Previous Versions of the Dataset

Older versions of the data can be accessed by using `restic`, which can be used to list old versions, and to extract either entire snapshots or single `.zip`s.

The `restic` repository also stores timestamps and unique identifiers for the archival of the previous dataset versions. The official way to reference old versions is the SHA-256 hash calculated by `restic` from the archive. The full hash is 256 bits long, but for referencing purposes, its 32-bit prefix is sufficient, and is represented by a 8-character string, like `9ed7c15f`.


TODO:
- allas bucket
- restic quickstart
