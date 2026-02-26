#!/bin/bash

VOSI=$HOME/work/dev/ivoa-std/VOSI.git
DALI=$HOME/work/dev/ivoa-std/DALI.git
TAP=$HOME/work/dev/ivoa-std/TAP.git

ARGS="$1 -avc --delete"

rsync $ARGS $VOSI/openapi/vosi src/main/webapp/openapi/
rsync $ARGS $DALI/openapi/dali src/main/webapp/openapi/
rsync $ARGS $TAP/openapi/uws src/main/webapp/openapi/
rsync $ARGS $TAP/openapi/tap src/main/webapp/openapi/

