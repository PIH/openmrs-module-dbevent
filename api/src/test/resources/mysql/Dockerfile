FROM mysql:5.6

MAINTAINER Michael Seaton <mseaton@pih.org>

RUN rm /etc/mysql/my.cnf
COPY my.cnf /etc/mysql/my.cnf
COPY initial-25x.sql /docker-entrypoint-initdb.d/setup.sql
RUN chown -R mysql:mysql /docker-entrypoint-initdb.d/

ENV MYSQL_ROOT_PASSWORD="test"
ENV MYSQL_DATABASE="dbevent"
