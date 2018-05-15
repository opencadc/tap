#!/bin/bash
# waitForContainersReady.sh
set -e

# Max query attempts before consider setup failed
MAX_TRIES=10

# Return true-like values if and only if logs
# contain the expected "ready" line
function dbIsReady() {
  docker-compose logs tapdb | grep "MySQL init process done"
}
function railsIsReady() {
  docker-compose logs tap | grep "org.apache.catalina.startup.Catalina.start Server startup in"
}

function waitUntilServiceIsReady() {
  attempt=1
  while [ $attempt -le $MAX_TRIES ]; do
    if "$@"; then
      echo "$2 container is up!"
      break
    fi
    echo "Waiting for $2 container... (attempt: $((attempt++)))"
    sleep 5
  done

  if [ $attempt -gt $MAX_TRIES ]; then
    echo "Error: $2 not responding, cancelling set up"
    exit 1
  fi
}

waitUntilServiceIsReady dbIsReady "MySQL"
waitUntilServiceIsReady railsIsReady "Tomcat"
