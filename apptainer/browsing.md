## Using the NLF collections on Puhti

[Puhti](https://docs.csc.fi/computing/systems-puhti/), aka `puhti.csc.fi` is a general-purpose supercomputer with partitions for different kinds of applications, including very parallel applications and gpu applications. Here we are going to demonstrate how NLF materials are made accessible there, and recommended ways to bring your software to the data.

### SquashFS

The collections are currently distributed as filesystem images. In this demo, that means SquashFS images, which are files with the extension `.sqfs` that can be mounted into an Apptainer session. Other filesystems that are compatible with Apptainer work much the same.

The limitations of the shared filesystems on our supercomputers means that it's not a good idea to keep huge numbers of files on the main filesystems. That's why we have the mountable images, and for processing the files we recommend reserving a compute node, where you'll have a fast local filesystem, suitable for staging and processing the part of the dataset you need. We'll go over this in the tutorial.

### Apptainer

#### From scratch on your own machine

You can [make your own Apptainer containers](https://docs.csc.fi/support/tutorials/singularity-scratch/) with whatever software you like, but for this demonstration, we made a simple container that allows you to filter collections by date range and produce zip files of the results. These zip files can then be opened in the temporary storage of CSC supercomputers.

The Apptainer container will be in the form of a single file, usually with a `.sif` file extension. In our case it is `demo_filter.sif`. It's generated from the definition file `demo_filter.def`. If you make your own, you will need to make it on a machine where you have root access (or see the following section about building with suppoerted tools directly on Puhti).

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

Building the Apptainer is done by running `apptainer build demo_filter.sif demo_filter.def`.

The easiest way to run homemade Apptainer containers on CSC's systems is `apptainer_wrapper`. It will handle eg. mounting some default paths for you. It understands some useful environment variables, namely `SING_IMAGE`, which is a path to the image, and `SING_FLAGS`, which is useful for controlling mounting disk images. Here we will set them manually for full control, but ultimately they will be set in some automatic way for convenience.

#### With `tykky` directly on Puhti

[Tykky](https://docs.csc.fi/computing/containers/tykky/) is a nifty tool for turning environment specifications or eg. Docker containers into Apptainer containers with some extra convenience features:

* tykky generates wrappers for executables which you can put in your `$PATH`, so that you can run the containerized programs in the regular environment
* some useful paths are automatically mounted when you use those wrappers
* compared to a `conda` or `venv` environment, startup times are much faster on HPC computers, which again don't like large numbers of files

And you can run it without having root access!

Please refer to the linked Tykky documentation for details, but for example if you wanted to run our `filter.py` on the mounted data, you could write a pip-style `requirements.txt` containing just the line `lxml`. Then (on puhti)

```
$ module purge
$ module load tykky/0.3.1 # we need at least 0.3 for binding the containers to work
$ pip-containerize new --prefix /scratch/<project>/<username>/<my-tykky-experiment> requirements.txt
```

will result in the path `/scratch/<project>/<username>/<my-tykky-experiment>` (put this in eg. `/projappl` if you want to keep it) containing `bin/`, containing the executable wrappers, and `container.sif`, which you can also use like any other Apptainer container. Now if you put `/scratch/<project>/<username>/<my-tykky-experiment>/bin` in your `$PATH` (eg. `export PATH="/scratch/<project>/<username>/<my-tykky-experiment>/bin:$PATH"`), `python` and any other executable you set up will come from the containerized environment.

Now you can control the mounts like this:

```
$ export CW_EXTRA_BIND_MOUNTS="/scratch/project_2006633/demo/col82-subset.sqfs:/data/col-82-subset:image-src=/"
$ export CW_EXTRA_BIND_MOUNTS="/scratch/project_2006633/demo/col-361.sqfs:/data/col-361:image-src=/,$CW_EXTRA_BIND_MOUNTS"
```

(Behind the scenes this is of course controlled by a script, most likely run automatically with a `module load` directive.

### The demo

#### With `apptainer_wrap` (recommended)

On puhti.csc.fi, we have prepared the Apptainer and some METS and ALTO data in `/scratch/project_2006633/`. To set the environment up, you can run `source setup.sh` there, or run the commands

```
export SING_IMAGE=/scratch/project_2006633/demo/demo_filter.sif # the Apptainer image
export SING_FLAGS="-B /scratch/project_2006633/demo/col-361.sqfs:/data/col-361:image-src=/" # bind a collection SquashFS to a path
export SING_FLAGS="-B /scratch/project_2006633/demo/col82-subset.sqfs:/data/col-82-subset:image-src=/ $SING_FLAGS" # Add another binding
```

Now you could go into an interactive computing node session with `sinteractive`, but for this test that's not strictly necessary. You can stick with the login node. Either way, you can jump into a console session in the Apptainer with

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

When you are done, you can close the Apptainer session with `exit` or ctrl-c.

#### With tykky (extra credit)

If you have a tykky-fied environment set up (see previous section), `setup.sh` also sets up the mounts for that, so you can run `python filter.py` in your regular puhti environment as if the data were really mounted there. For example,

```
$ ls /data # this won't work, because ls is looking in the regular puhti filesystem
ls: cannot access '/data': No such file or directory
$ # But this will work, because python is a wrapper to a tykkyfied environment which has the mounts
$ python filter.py --start-date 1.1.1888 --end-date 31.1.1888 /data/col-82-subset
...
```

Now you can pipe the output directly to any other command available on puhti!
