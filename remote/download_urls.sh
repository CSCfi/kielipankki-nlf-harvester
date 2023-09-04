#!/usr/bin/bash

JOBS=${1:-10}

parallel --jobs $JOBS --col-sep='\t' wget --no-verbose {1} -O {2}
