package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public class PulsarSerializationSchemaWrapper<T> implements PulsarSerializationSchema<T>, PulsarContextAware<T> {
    private final String topic;
    private final SerializationSchema<T> serializationSchema;
    private final RecordSchemaType recordSchemaType;
    private final Schema<?> schema;
    private final Class<?> clazz;
    private final DataType dataType;

    private int parallelInstanceId;
    private int numParallelInstances;

    public PulsarSerializationSchemaWrapper(String topic,
                                            SerializationSchema<T> serializationSchema,
                                            DataType dataType){
        this(topic, serializationSchema, null, null, null, dataType);
    }

    public PulsarSerializationSchemaWrapper(String topic,
                                            SerializationSchema<T> serializationSchema,
                                            RecordSchemaType recordSchemaType,
                                            Class<?> clazz){
        this(topic, serializationSchema, recordSchemaType, clazz, null, null);
    }

    public PulsarSerializationSchemaWrapper(String topic,
                                            SerializationSchema<T> serializationSchema,
                                            Schema<?> schema){
       this(topic, serializationSchema, null, null, schema, null);
    }

    public PulsarSerializationSchemaWrapper(String topic,
                                            SerializationSchema<T> serializationSchema,
                                            RecordSchemaType recordSchemaType,
                                            Class<?> clazz,
                                            Schema<?> schema,
                                            DataType dataType){
        this.topic = topic;
        this.serializationSchema = serializationSchema;
        this.recordSchemaType = recordSchemaType;
        this.schema = schema;
        this.clazz = clazz;
        this.dataType = dataType;
    }

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {
        this.numParallelInstances = numParallelInstances;
    }

    @Override
    public void setPartitions(int[] partitions) {
        return;
    }

    @Override
    public String getTargetTopic(T element) {
        return topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        this.serializationSchema.open(context);
    }

    @Override
    public byte[] serialize(T element){
        return serializationSchema.serialize(element);
    }

    @Override
    public void serialize(T element, TypedMessageBuilder<byte[]> messageBuilder) {
        messageBuilder.value(serializationSchema.serialize(element));
    }

    @Override
    public Schema<?> getPulsarSchema() {
        if(schema != null){
            return schema;
        }
        try{
            if(dataType instanceof AtomicDataType){
                return SchemaTranslator.atomicType2PulsarSchema(dataType);
            } else{
                // for pojo type, use avro or json
                Preconditions.checkNotNull(clazz, "for non-atomic type, you must set clazz");
                Preconditions.checkNotNull(clazz, "for non-atomic type, you must set recordSchemaType");
                return SchemaUtils.buildSchemaForRecordClazz(clazz, recordSchemaType);
            }
        }catch (IncompatibleSchemaException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getKey(T element) {
        return null;
    }
}
