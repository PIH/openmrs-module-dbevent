FROM mysql:5.6

MAINTAINER Michael Seaton <mseaton@pih.org>

COPY my.cnf /etc/mysql/my.cnf
COPY initial-25x.sql /docker-entrypoint-initdb.d/setup.sql

ENV MYSQL_ROOT_PASSWORD="test"
ENV MYSQL_DATABASE="dbevent"
