package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

/**
 * Represents metadata for a Database
 */
@Data
public class Database implements Serializable {

    private static final Logger log = LogManager.getLogger(Database.class);

    private String username;
    private String password;
    private String url;
    private String hostname;
    private String port;
    private String databaseName;

    public Database(Properties properties) {
        this.url = properties.getProperty("connection.url");
        this.username = properties.getProperty("connection.username");
        this.password = properties.getProperty("connection.password");
        try {
            Driver driver = DriverManager.getDriver(url);
            for (DriverPropertyInfo driverPropertyInfo : driver.getPropertyInfo(url, null)) {
                switch (driverPropertyInfo.name.toLowerCase()) {
                    case "host": {
                        this.hostname = driverPropertyInfo.value;
                        break;
                    }
                    case "port": {
                        this.port = driverPropertyInfo.value;
                        break;
                    }
                    case "dbname": {
                        this.databaseName = driverPropertyInfo.value;
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid connection.url: " + url);
        }
    }

    /**
     * @return a new connection to the database specified in the runtime properties
     */
    public Connection openConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred opening a database connection", e);
        }
    }

    /**
     * @return a DatabaseMetadata that contains information on the tables in the configured database
     */
    public DatabaseMetadata getMetadata() {
        DatabaseMetadata ret = new DatabaseMetadata();
        ret.setDatabaseName(databaseName);
        try (Connection connection = openConnection();) {
            try (ResultSet tableRs = connection.getMetaData().getTables(databaseName, null, "%", new String[] { "TABLE" })) {
                while (tableRs.next()) {
                    String tableName = tableRs.getString("TABLE_NAME").toLowerCase();
                    DatabaseTable table = new DatabaseTable(databaseName, tableName);
                    try (ResultSet columnRs = connection.getMetaData().getColumns(databaseName, null, tableName, "%")) {
                        while (columnRs.next()) {
                            String columnName = columnRs.getString("COLUMN_NAME").toLowerCase();
                            boolean nullable = "YES".equals(columnRs.getString("IS_NULLABLE"));
                            table.addColumn(new DatabaseColumn(databaseName, tableName, columnName, nullable));
                        }
                    }
                    try (ResultSet pkRs = connection.getMetaData().getPrimaryKeys(databaseName, null , tableName)) {
                        while (pkRs.next()) {
                            String columnName = pkRs.getString("COLUMN_NAME").toLowerCase();
                            table.getColumns().get(columnName).setPrimaryKey(true);
                        }
                    }
                    ret.addTable(table);
                }
            }
            for (DatabaseTable table : ret.getTables().values()) {
                String tableName = table.getTableName();
                try (ResultSet fkRs = connection.getMetaData().getExportedKeys(databaseName, null, tableName)) {
                    while (fkRs.next()) {
                        String fkTableName = fkRs.getString("FKTABLE_NAME").toLowerCase();
                        String fkColumnName = fkRs.getString("FKCOLUMN_NAME").toLowerCase();
                        String pkColumnName = fkRs.getString("PKCOLUMN_NAME").toLowerCase();
                        DatabaseColumn pkColumn = table.getColumns().get(pkColumnName);
                        DatabaseTable fkTable = ret.getTables().get(fkTableName);
                        DatabaseColumn fkColumn = fkTable.getColumns().get(fkColumnName);
                        pkColumn.getReferencedBy().add(fkColumn);
                        fkColumn.getReferences().add(pkColumn);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to retrieve included tables", e);
        }
        return ret;
    }

    /**
     * Executes the given statement with parameter values against the database specified in the runtime properties
     * @param sql the statement to execute
     * @param values the values for each parameter in the statement
     */
    public <T> T executeQuery(String sql, ResultSetHandler<T> handler, Object... values)  {
        try (Connection conn = openConnection(); ) {
            QueryRunner qr = new QueryRunner();
            return qr.query(conn, sql, handler, values);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred updating executing statement " + sql, e);
        }
    }

    /**
     * Executes the given statement with parameter values against the database specified in the runtime properties
     * @param sql the statement to execute
     * @param values the values for each parameter in the statement
     */
    public void executeUpdate(String sql, Object... values) {
        if (log.isTraceEnabled()) {
            log.trace(sql + " " + Arrays.asList(values));
        }
        try (Connection conn = openConnection(); ) {
            QueryRunner qr = new QueryRunner();
            qr.update(conn, sql, values);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred updating executing statement " + sql, e);
        }
    }

    /**
     * Closes the given database connection, logging a warning if an exception is thrown
     * @param connection to close
     */
    public void closeConnection(Connection connection) {
        try {
            connection.close();
        }
        catch (Exception e) {
            log.warn("An error occurred closing the database connection", e);
        }
    }

    @Override
    public String toString() {
        return url;
    }
}