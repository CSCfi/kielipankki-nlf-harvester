## Work-offloading scripts

This directory contains a few scripts that have been used to test downloading files directly on Puhti, on parallel, using GNU Parallel. They can be used as copy-paste fodder to make the pipeline offload that work to Puhti, instead of doing it in Airflow.

The scripts are intended to be run in the "download batch root", eg. `/local_scratch/robot_2006633_puhti/harvester/col_861-9/10/` for download batch 10 in image number 9 of collection 861.

### Directory assignment task

If you send just a URL list to Puhti, `process_url_list.py` can be used to output a list that appends to earch url a TAB character and a relative path for the download target, eg.

`https://digi.kansalliskirjasto.fi/sanomalehti/binding/1150688/page-00002.xml`

becomes

`https://digi.kansalliskirjasto.fi/sanomalehti/binding/1150688/page-00002.xml	1/11/115/1150/11506/115068/1150688/1150688/page-00002.xml`

It reads the URL list from stdin and outputs to sdout.

### Dir creation task

Once you have a list as described in the previous sections, you can either create directories on Puhti or have Airflow do it. `create_dirs.sh` will do it on Puhti. It also takes a list in stdin.

### Downloading task

Once you have your tab-separated list and the directories are there, you can run downloads with GNU Parallel. The script `download_urls.sh` will read, you gessed it, a list from stdin and feed it to Parallel which runs wget. If you give it an argument, that will be given to Parallel as the `--jobs` argument, otherwise the default will be 10. The (non-verbose) output from wget will go to stderr.
