package org.apache.flink.streaming.util.serialization;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.Serializable;
import java.util.Map;

/**
 * wrapper {@link SchemaInfo}, Resolve that a job cannot be serialized when submitted to flink.
 * https://github.com/streamnative/pulsar-flink/issues/237
 */
public class SchemaInfoWrapper implements Serializable {

    private static final long serialVersionUID = 997809857078533653L;

    private String name;
    private byte[] schema;
    private SchemaType type;
    private Map<String, String> properties;

    private transient SchemaInfo schemaInfo;

    public SchemaInfoWrapper(SchemaInfo schemaInfo) {
        this.name = schemaInfo.getName();
        this.schema = schemaInfo.getSchema();
        this.type = schemaInfo.getType();
        this.properties = schemaInfo.getProperties();
    }

    public SchemaInfo getSchemaInfo() {
        if (schemaInfo == null) {
            schemaInfo = new SchemaInfo(name, schema, type, properties);
        }
        return schemaInfo;
    }
}
