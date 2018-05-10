FROM postgres:10.3-alpine

COPY sql/*.sql /docker-entrypoint-initdb.d/

# Add extra volume to mount for tablespace data
VOLUME /var/run/postgresql
