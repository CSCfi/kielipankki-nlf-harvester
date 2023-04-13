## Using the NLF collections on Puhti

### SquashFS

The collections are currently distributed as SquashFS filesystem images. These are files with the extension `.sqfs` that can be mounted into an Apptainer session.

The limitations of the shared filesystems on our supercomputers means that it's not a good idea to keep huge numbers of files on the main filesystems. That's why we have the mountable images, and for processing the files we recommend reserving a compute node, where you'll have a fast local filesystem, suitable for staging and processing the part of the dataset you need. We'll go over this in the tutorial.

### Apptainer

You can make your own Apptainer containers with whatever software you like, but for this demostration, we made a simple container that allows you to filter collections by date range and produce zip files of the results. These zip files can then be opened in the temporary storage of CSC supercomputers.

The Apptainer container will be in the form of a single file, usually with a `.sif` file extension. In our case it is `demo.sif`. It's generated from the definition file `demo.def`. If you make your own, you will need to make it either on a machine where you have root access, or use something like [tykky](https://docs.csc.fi/computing/containers/tykky/) on CSC's systems.

`demo.def` is simply:

```
BootStrap: debootstrap
OSVersion: jammy
MirrorURL: http://us.archive.ubuntu.com/ubuntu/

%post
	# Install software via apt
	apt-get update
	apt-get -y install vim python3 python3-lxml zip

%files
	# Copy files from build system to container
	filter.py /usr/local/bin/filter
```

`debootstrap` needs to be installed on the system you build the container on, and `filter.py` is our little demonstration program.

The easiest way to run Apptainer on CSC's systems is `apptainer_wrapper`. It will handle eg. mounting some default paths for you. It understands some useful environment variables, namely `SING_IMAGE`, which is a path to the image, and `SING_FLAGS`, which is useful for controlling mounting disk images. Here we will set them manually for full control, but ultimately they will be set in some automatic way for convenience.

### The demo

On puhti.csc.fi, we have prepared the Apptainer and some METS and ALTO data in `/projappl/project_2006633/`. To set the environment up, run the commands

```
export SING_IMAGE=/projappl/project_2006633/demo_filter.sif # the Apptainer image
export SING_FLAGS="-B /projappl/project_2006633/col-361.sqfs:/data/col-361:image-src=/" # bind a collection SquashFS to a path
export SING_FLAGS="-B /projappl/project_2006633/col82-subset.sqfs:/data/col-82-subset:image-src=/ $SING_FLAGS" # Add another binding
```

Now you could go into an interactive computing node session with `sinteractive`, but for this test that's not strictly necessary. Either way, you can to jump into a console session in the Apptainer with

`apptainer_wrapper shell`

You should now see a prompt like `Apptainer>`. Inside, your home directory, `/scratch` and `$TMPDIR` should work as usual. Software from puhti will however not generally be available.

The SquashFS images are mounted in `/data/`:

```
Apptainer> cd /data/
Apptainer> ls
col-361  col-82-subset
```

Our demo program `filter` operates on these collections, by parsing the METS file in each issue and checking it against a date range:

```
Apptainer> filter col-82-subset/ --start-date 1.1.1888 --end-date 31.1.1888
col-82-subset/10043
col-82-subset/3165
col-82-subset/3379
...
```

We get a listing of all the issues from January 1888.

```
Apptainer> filter col-82-subset/ --start-date 1.1.1888 --end-date 31.1.1888 | wc -l
40
```

There are 40 of them. We can also just use years:

```
Apptainer> filter col-82-subset/ --start-date 1880 --end-date 1889 | wc -l
1439
```

We have 1439 issues from the 1880's. Let's stage them in a zip file in `/scratch` (could also be somewhere else):

```
Apptainer> filter col-82-subset/ --start-date 1880 --end-date 1889 | zip -r -@ /scratch/<project>/<username>/col-82-1880-1889.zip 
```

It takes a while, and results in a 502M zip file.
