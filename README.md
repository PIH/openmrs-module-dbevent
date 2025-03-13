# OpenMRS DBEvent Module

## Description

This module utilizes an embedded Debezium engine to track changes to the OpenMRS database as events.
This module is intended to be configured by a particular implementation in their distribution by instantiating
one or more `DbEventListener` instances to process database-level changes.

## Prerequisites

This module should work with any database supported by Debezium...theoretically.  Currently, it is written with 
MySQL in mind - as such, the documentation may be slanted to a MySQL-based configuration, and some tweaks may be 
needed for other DBMS systems, specifically there likely need to be additional Debezium connector libraries included
as Maven dependencies.  This is left for a future enhancement for now.  This does _not_ support MySQL 8.4+, due to
the version of Debezium that is embedded, which is itself due the need to support Java 8 still in OpenMRS.

For a MySQL-based setup, the primary pre-requisite on the MySQL end is that the MySQL instance has 
[row-level bin logging enabled](https://debezium.io/documentation/reference/connectors/mysql.html#enable-mysql-binlog). 
Additionally, the database user (by default the user configured in the runtime properties file, though this can be 
overridden), needs to have [privileges to access the MySQL bin logs](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-creating-user).

## Listeners

Each `DbEventListener` represents an independent process that streams events from the OpenMRS database.  In the case of 
MySQL, each `DbEventListener` is the equivalent of a new MySQL Replication Node, and is independently configured.  Each
of these would independently take an initial snapshot of the database (if configured to do so) and
keep independent track of changes that have it has successfully processed.

In typical usage, one would:

* Create a new instance of a particular `DbEventListener` class to respond to a particular configuration of DbEvents.  
* Call the `init(DbEventListenerConfiguration)` method to configure and start up the Listener.
* As appropriate, use the `start` and `stop` methods if processing should stop and/or restart at anytime

## Listener Configuration

Each `DbEventListener` must be initialized with a `DbEventListenerConfig`.  Each `DbEventListenerConfig` must be 
configured explicitly with a particular `sourceId` and a `sourceName`.

The `sourceId` is numeric and must be unique across all other sources and other MySQL server ids in the same cluster.
The `sourceName` should be descriptive, but must be a valid identifier (i.e. no white-space)

All remaining configuration is done by setting property values.  These property values can be categorized as:

### Debezium Properties

All properties that start with a `debezium.` prefix are used to configure Debezium.  The default values that are
set on all listeners are as follows.

```properties
debezium.name=<sourceName>
debezium.connector.class=io.debezium.connector.mysql.MySqlConnector
debezium.offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
debezium.offset.storage.file.filename=<applicationDataDirectory>/dbevent/<sourceId>_offsets.dat
debezium.offset.flush.interval.ms=0
debezium.offset.flush.timeout.ms=15000
debezium.include.schema.changes=false
debezium.database.server.id=<sourceId>
debezium.database.server.name=<sourceName>
debezium.database.user=<from connection.username in runtime properties>
debezium.database.password=<from connection.password in runtime properties>
debezium.database.hostname=<from connection.url in runtime properties>
debezium.database.port=<from connection.url in runtime properties>
debezium.database.dbName=<from connection.url in runtime properties>
debezium.database.include.list=<from connection.url in runtime properties>
debezium.database.history=io.debezium.relational.history.FileDatabaseHistory
debezium.database.history.file.filename=<applicationDataDirectory>/dbevent/<sourceId>_schema_history.dat
debezium.decimal.handling.mode=double
debezium.tombstones.on.delete=false
```

Any property that [Debezium supports](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties) 
can be configured by prefixing that property with a `debezium.` prefix and setting it on the `DbEventListenerConfig`.

Any of these properties can be overridden or new properties can be set after source instantiation, and before 
starting up the source.

By default, all tables will be monitored in the given database.  This can be overridden programmatically on the source,
either by setting the property explicitly or using a convenience method.  Note, if setting manually, table names are
regular expression patterns, and must start with the database as a prefix.

### Non-Debezium Properties

Any property that does not start with a `debezium.` prefix may be used to further configure a particular Listener at runtime.
The following properties are supported on all `DbEventListener` implementations, along with their default values:

```properties
retryIntervalMillis=60000
enabled=true
```

The `retryIntervalMillis` defaults to 1 minute.  Any failures by the listener to process an event will result 
in a retry at this interval until it is successfully processed.  There is no Dead Letter Queue by default to prioritize
guaranteed delivery of all messages

One can disable the listener from exeuting by setting the `enabled=false` property

Specific listener implementations may support any number of additional properties as their use cases dictate.

### Runtime Configuration

Any property can be configured via the `openmrs-runtime.properties` file.  Any configuration from runtime properties
would override any property that is configured in code, whether a default value or an explictly configured value.

Runtime properties should be set following this convention:  `dbevent.{sourceId}.{property}={value}`

## Monitoring

Debezium outputs several useful metrics via JMX as MBeans.  Information on these for MySQL 
[can be found here](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-monitoring).

The DbEvent module also exposes several metrics for each `DbEventListener` that is running.  These metrics match
those that are found on the `DbEventListenerStatus` class.

To access these in development mode, one can do the following in their SDK:

1. Run your server with extra system variables like this:

```shell
mvn openmrs-sdk:run -DserverId=myserverid -DMAVEN_OPTS="-Xmx1g -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9000 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
```

2. Open up JConsole (i.e. from terminal run `jconsole`).  Connect to Remote Process at `localhost:9000` (match port used in step #1).
3. Navigate to `MBeans` and find `debezium.mysql` for the Debezium metrics, or find those under `org.openmrs.module.dbevent.monitoring` for each `DbEventListener`

To access these from code, one can do so by getting the MBeanServer in the JVM: `ManagementFactory.getPlatformMBeanServer();`

