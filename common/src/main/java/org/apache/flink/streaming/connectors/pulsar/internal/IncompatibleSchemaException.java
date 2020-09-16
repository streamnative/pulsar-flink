package org.apache.flink.streaming.connectors.pulsar.internal;

/**
 * Exception designates the incompatibility between pulsar and flink type.
 */
public class IncompatibleSchemaException extends Exception {
    public IncompatibleSchemaException(String message, Throwable e) {
        super(message, e);
    }

    public IncompatibleSchemaException(String message) {
        this(message, null);
    }
}