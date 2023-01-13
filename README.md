# OpenMRS DBEvent Module

## Description

This module utilizes an embedded Debezium engine to track changes to the OpenMRS database as events.
This module is intended to be configured by a particular implementation in their distribution.  A distribution would
create an implementation of EventConsumer to respond to a stream of DbEvents.  The distribution would instantiate,
configure, and start a new DbEventSource for each EventConsumer.

## Event Sources

Each event source represents an independent process that streams events from the OpenMRS database.  In the case of 
MySQL, each Event Source is the equivalent of a new MySQL Replication Node, and is independently configured.  Each
of these would independently take an initial snapshot of the database and streaming changes from the MySQL binlog.

Each Event Source must be instantiated with a `sourceId` and a `sourceName`.
The `sourceId` is numeric and must be unique across all other sources and other MySQL server ids in the same cluster.
The `sourceName` should be descriptive, but must be a valid identifier (i.e. no white-space)

The default configuration of each Event Source is as follows:

```properties
name=<sourceName>
connector.class=io.debezium.connector.mysql.MySqlConnector
offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=<applicationDataDirectory>/dbevent/<sourceId>_offsets.dat
offset.flush.interval.ms=0
offset.flush.timeout.ms=15000
include.schema.changes=false
database.server.id=<sourceId>
database.server.name=<sourceName>
database.user=<from connection.username in runtime properties>
database.password=<from connection.password in runtime properties>
database.hostname=<from connection.url in runtime properties>
database.port=<from connection.url in runtime properties>
database.dbName=<from connection.url in runtime properties>
database.include.list=<from connection.url in runtime properties>
database.history=io.debezium.relational.history.FileDatabaseHistory
database.history.file.filename=<applicationDataDirectory>/dbevent/<sourceId>_schema_history.dat
decimal.handling.mode=double
tombstones.on.delete=false
```

Any of these properties can be overridden or new properties can be set after source instantiation, and before 
starting up the source.

For more details on these configuration settings and other available options, please consult the 
[Debezium documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties).

## Monitoring

Debezium outputs several useful metrics via JMX as MBeans.  Information on these for MySQL 
[can be found here](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-monitoring).

To access these in development mode, one can do the following in their SDK:

1. Run your server with extra system variables like this:

```shell
mvn openmrs-sdk:run -DserverId=myserverid -DMAVEN_OPTS="-Xmx1g -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9000 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
```

2. Open up JConsole (i.e. from terminal run `jconsole`).  Connect to `localhost` on port `9000` (changing as appropriate to match port used in step #1).
3. Navigate to `MBeans` and find `debezium.mysql`.

To access these from code, one can do so by getting the MBeanServer in the JVM: `ManagementFactory.getPlatformMBeanServer();`

