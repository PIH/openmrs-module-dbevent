package org.openmrs.module.dbevent.test;

import lombok.Data;
import org.openmrs.module.dbevent.ObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Data
public class Mysql implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Mysql.class);

    private MySQLContainer<?> container;

    private Mysql() {}

    public static Mysql open() throws Exception {
        Mysql mysql = new Mysql();
        MySQLContainer<?> container = new MySQLContainer<>(DockerImageName.parse("mysql:5.6"))
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mysql/my.cnf"),
                        "/etc/mysql/my.cnf"
                )
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("mysql/setup-platform-2.5.8.sql"),
                        "/docker-entrypoint-initdb.d/setup.sql"
                )
                .withDatabaseName("openmrs_dbevent")
                .withLogConsumer(new Slf4jLogConsumer(log));
        container.start();
        mysql.setContainer(container);
        return mysql;
    }

    public Properties getConnectionProperties() {
        Properties p = new Properties();
        p.setProperty("connection.username", "root");
        p.setProperty("connection.password", getEnvironmentVariable("MYSQL_ROOT_PASSWORD"));
        p.setProperty("connection.url", getContainer().getJdbcUrl());
        return p;
    }

    public String getEnvironmentVariable(String key) {
        for (String env : getContainer().getEnv()) {
            String[] keyValue = env.split("=", 2);
            if (keyValue[0].trim().equals("MYSQL_ROOT_PASSWORD")) {
                return keyValue.length == 2 ? keyValue[1].trim() : null;
            }
        }
        return null;
    }

    public void close() {
        if (container != null) {
            container.stop();
        }
    }

    public void executeUpdate(String sql) {
        try (Connection conn = container.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred executing update statement: " + sql);
        }
    }

    public Dataset executeQuery(String sql) {
        Dataset ret = new Dataset();
        try (Connection conn = container.createConnection(""); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    ObjectMap row = new ObjectMap();
                    for (int i=1; i<=columnCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    ret.add(row);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred executing update statement: " + sql);
        }
        return ret;
    }
}
