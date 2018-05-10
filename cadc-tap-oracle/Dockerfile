FROM oracle/database:11.2.0.2-xe
# oracle/database:11.2.0.2-xe built from Oracle's docker instructions
# https://github.com/oracle/docker-images/tree/master/OracleDatabase
# Modified here to create TAP schema and tables, but also to shrink some
# bloated resources.

MAINTAINER djenkins.cadc@gmail.com

COPY runOracle.sh /u01/app/oracle/
COPY startOracle.sh /u01/app/oracle/

ENV ORACLE_PWD=manager

RUN chmod 755 /u01/app/oracle/runOracle.sh ; chmod 755 /u01/app/oracle/startOracle.sh ; /u01/app/oracle/runOracle.sh
CMD exec /u01/app/oracle/startOracle.sh