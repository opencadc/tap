#!/bin/bash

VOSI=$HOME/work/dev/ivoa-std/VOSI.git
DALI=$HOME/work/dev/ivoa-std/DALI.git
TAP=$HOME/work/dev/ivoa-std/TAP.git

ARGS="$1 -avu --delete"

echo "rsync components..."
rsync $ARGS $VOSI/openapi/ src/main/webapp/openapi/vosi/
rsync $ARGS $DALI/openapi/ src/main/webapp/openapi/dali/
rsync $ARGS $TAP/openapi/ src/main/webapp/openapi/tap/
echo

echo "diff TAP API..."
diff ${TAP}/openapi.yaml src/main/webapp/openapi.yaml

