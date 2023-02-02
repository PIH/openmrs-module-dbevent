package org.openmrs.module.dbevent.test;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.util.Properties;

@Data
public class Mysql implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Mysql.class);

    public static final String DATABASE_NAME = "dbevent";

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
                        MountableFile.forClasspathResource("mysql/initial-25x.sql"),
                        "/docker-entrypoint-initdb.d/setup.sql"
                )
                .withDatabaseName(DATABASE_NAME)
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
            if (keyValue[0].trim().equals(key)) {
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
}
