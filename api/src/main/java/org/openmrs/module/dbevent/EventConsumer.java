package org.openmrs.module.dbevent;

import java.util.function.Consumer;

/**
 * Implementations should implement this interface to provide logic for handing Events from a DbEventSource
 */
public interface EventConsumer extends Consumer<DbEvent> {

    /**
     * Any logic that should be executed at startup (prior to any Event processing) can be implemented here
     */
    default void startup() {}

    /**
     * Any logic that should be executed at shutdown can be implemented here.
     */
    default void shutdown() {}
}
