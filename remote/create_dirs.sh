#!/usr/bin/bash

cut -f2 | egrep --only-matching "^[0-9\/]+" | xargs mkdir -p
