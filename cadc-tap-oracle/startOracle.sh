#!/bin/bash
# This command will start the XE Database on Container Startup
/etc/init.d/oracle-xe start

# Show the output of the alert.log during the Startup

echo "The following output is now a tail of the alert.log:"
tail -f $ORACLE_BASE/diag/rdbms/*/*/trace/alert*.log &
childPID=$!
wait $childPID
