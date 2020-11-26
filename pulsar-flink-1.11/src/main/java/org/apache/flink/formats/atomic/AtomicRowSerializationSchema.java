package org.apache.flink.formats.atomic;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializer;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Schema;

public class AtomicRowSerializationSchema implements SerializationSchema<Row> {
    private static final long serialVersionUID = -2885556750743978636L;
    private final DataType atomicType;
    private final String className;
    private final boolean useExtendFields;
    private final Class<?> clazz;
    private final PulsarDeserializer.Function<Object, byte[]> converter;

    private AtomicRowSerializationSchema(String className, boolean useExtendFields) {
        this.className = className;
        this.useExtendFields = useExtendFields;
        try {
            this.clazz = Class.forName(className);
            this.converter = getRuntimeConverter(clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.atomicType = TypeConversions.fromClassToDataType(clazz).
                orElseThrow(()->new IllegalStateException(clazz.getCanonicalName() + "cant cast to flink dataType"));
    }

    /**
     * Builder for {@link AtomicRowSerializationSchema}.
     */
    @PublicEvolving
    public static class Builder {

        private final String className;
        private boolean useExtendFields;

        public Builder(String className) {
            this.className = className;
        }

        public AtomicRowSerializationSchema.Builder useExtendFields(boolean useExtendFields) {
            this.useExtendFields = useExtendFields;
            return this;
        }

        public AtomicRowSerializationSchema build() {
            return new AtomicRowSerializationSchema(className, useExtendFields);
        }
    }

    @Override
    public byte[] serialize(Row row) {
        try {
            Object value = row.getField(0);
            byte[] valueData = this.converter.apply(value);
            return valueData;
        } catch (Throwable t) {
            throw new RuntimeException("Could not serialize row '" + row + "'. " +
                    "Make sure that the schema matches the input.", t);
        }
    }

    private PulsarDeserializer.Function<Object, byte[]> getRuntimeConverter(Class<?> clazz){
        return (PulsarDeserializer.Function<Object, byte[]>) o -> {
            try {
                Schema schema = SimpleSchemaTranslator.sqlType2PulsarSchema(atomicType);
                return schema.encode(o);
            } catch (IncompatibleSchemaException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public DataType getAtomicType(){
        return atomicType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AtomicRowSerializationSchema that = (AtomicRowSerializationSchema) o;

        if (useExtendFields != that.useExtendFields) {
            return false;
        }
        return className.equals(that.className);
    }

    @Override
    public int hashCode() {
        int result = className.hashCode();
        result = 31 * result + (useExtendFields ? 1 : 0);
        return result;
    }
}
