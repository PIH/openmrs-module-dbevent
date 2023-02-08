package org.openmrs.module.dbevent.test;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.Closeable;
import java.util.Properties;

@Data
public class Mysql implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Mysql.class);

    public static final String DATABASE_NAME = "dbevent";

    private GenericContainer<?> container;

    private Mysql() {}

    public static Mysql open() throws Exception {
        Mysql mysql = new Mysql();
        GenericContainer<?> container = new GenericContainer<>(new ImageFromDockerfile()
                        .withFileFromClasspath("Dockerfile", "mysql/Dockerfile")
                        .withFileFromClasspath("my.cnf", "mysql/my.cnf")
                        .withFileFromClasspath("initial-25x.sql", "mysql/initial-25x.sql")
        ).withExposedPorts(3306).withLogConsumer(new Slf4jLogConsumer(log));
        container.start();
        mysql.setContainer(container);
        return mysql;
    }

    public Properties getConnectionProperties() {
        Properties p = new Properties();
        p.setProperty("connection.username", "root");
        p.setProperty("connection.password", "test");
        p.setProperty("connection.url", "jdbc:mysql://" + getHost() + ":" + getPort() + "/" + DATABASE_NAME);
        return p;
    }

    public String getHost() {
        return container.getHost();
    }

    public int getPort() {
        return container.getMappedPort(3306);
    }

    public void close() {
        if (container != null) {
            container.stop();
        }
    }
}
