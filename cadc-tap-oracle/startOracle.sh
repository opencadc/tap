#!/bin/bash

############# Execute custom scripts ##############
function runUserScripts {

  SCRIPTS_ROOT="$1";

  # Check whether parameter has been passed on
  if [ -z "$SCRIPTS_ROOT" ]; then
    echo "$0: No SCRIPTS_ROOT passed on, no scripts will be run";
    exit 1;
  fi;
  
  # Execute custom provided files (only if directory exists and has files in it)
  if [ -d "$SCRIPTS_ROOT" ] && [ -n "$(ls -A $SCRIPTS_ROOT)" ]; then
      
    echo "";
    echo "Executing user defined scripts"
  
    for f in $SCRIPTS_ROOT/*; do
        case "$f" in
            *.sh)     echo "$0: running $f"; . "$f" ;;
            *.sql)    echo "$0: running $f"; echo "exit" | su -p oracle -c "$ORACLE_HOME/bin/sqlplus / as sysdba @$f"; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo "";
    done
    
    echo "DONE: Executing user defined scripts"
    echo "";
  
  fi;
  
}

# This command will start the XE Database on Container Startup
/etc/init.d/oracle-xe start

runUserScripts $ORACLE_BASE/scripts/startup

# Show the output of the alert.log during the Startup

echo "The following output is now a tail of the alert.log:"
tail -f $ORACLE_BASE/diag/rdbms/*/*/trace/alert*.log &
childPID=$!
wait $childPID
