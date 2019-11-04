package org.shmaks.kafkasample;

import org.testcontainers.containers.PostgreSQLContainer;

public class PgTestContainer extends PostgreSQLContainer<PgTestContainer> {
    private static final String IMAGE_VERSION = "postgres:11.1";
    private static PgTestContainer container;

    private PgTestContainer() {
        super(IMAGE_VERSION);
    }

    public static PgTestContainer getInstance() {
        if (container == null) {
            container = new PgTestContainer();
        }
        return container;
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        //do nothing, JVM handles shut down
    }
}
