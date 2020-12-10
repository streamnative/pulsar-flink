/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.SerializableFunction;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.shade.org.apache.http.util.Asserts;

/**
 * A wrapper that warp flink {@link SerializationSchema} to {@link PulsarSerializationSchema}.
 */
public class PulsarSerializationSchemaWrapper<T> implements PulsarSerializationSchema<T>, PulsarContextAware<T> {
    private final String topic;
    private final SerializationSchema<T> serializationSchema;
    private final RecordSchemaType recordSchemaType;
    private final Schema<?> schema;
    private final Class<?> clazz;
    private final DataType dataType;
    private final SchemaMode schemaMode;
    private final SerializableFunction<T, String> topicExtractor;
    private final SerializableFunction<T, byte[]> keyExtractor;

    private int parallelInstanceId;
    private int numParallelInstances;

    private PulsarSerializationSchemaWrapper(String topic,
                                             SerializationSchema<T> serializationSchema,
                                             RecordSchemaType recordSchemaType,
                                             Class<?> clazz,
                                             Schema<?> schema,
                                             DataType dataType,
                                             SchemaMode schemaMode,
                                             SerializableFunction<T, String> topicExtractor,
                                             SerializableFunction<T, byte[]> keyExtractor) {
        this.topic = topic;
        this.serializationSchema = serializationSchema;
        this.recordSchemaType = recordSchemaType;
        this.schema = schema;
        this.clazz = clazz;
        this.dataType = dataType;
        this.schemaMode = schemaMode;
        this.topicExtractor = topicExtractor;
        this.keyExtractor = keyExtractor;
    }

    /**
     * Builder for {@link PulsarSerializationSchemaWrapper}.
     */
    @PublicEvolving
    public static class Builder<T> {
        private String topic;
        private final SerializationSchema<T> serializationSchema;
        private RecordSchemaType recordSchemaType;
        private Schema<?> schema;
        private Class<?> clazz;
        private DataType dataType;
        private SchemaMode mode;
        private SerializableFunction<T, String> topicExtractor;
        private SerializableFunction<T, byte[]> keyExtractor;

        public Builder(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> useSpecialMode(Schema<?> schema) {
            Asserts.check(mode == null, "you can only set one schemaMode");
            this.mode = SchemaMode.SPECIAL;
            this.schema = schema;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> useAtomicMode(DataType dataType) {
            Asserts.check(mode == null, "you can only set one schemaMode");
            this.mode = SchemaMode.ATOMIC;
            Asserts.check(dataType instanceof AtomicDataType, "you must set an atomic dataType");
            this.dataType = dataType;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> usePojoMode(Class<?> clazz,
                                                                       RecordSchemaType recordSchemaType) {
            Asserts.check(mode == null, "you can only set one schemaMode");
            this.mode = SchemaMode.POJO;
            Asserts.check(recordSchemaType != RecordSchemaType.ATOMIC,
                    "cant ues RecordSchemaType.ATOMIC to build pojo type schema");
            this.clazz = clazz;
            this.recordSchemaType = recordSchemaType;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> useRowMode(DataType dataType,
                                                                      RecordSchemaType recordSchemaType) {
            Asserts.check(mode == null, "you can only set one schemaMode");
            this.mode = SchemaMode.ROW;
            this.dataType = dataType;
            this.recordSchemaType = recordSchemaType;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> setTopicExtractor(
                SerializableFunction<T, String> topicExtractor) {
            this.topicExtractor = topicExtractor;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> setKeyExtractor(
                SerializableFunction<T, byte[]> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public PulsarSerializationSchemaWrapper.Builder<T> setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PulsarSerializationSchemaWrapper<T> build() {
            return (PulsarSerializationSchemaWrapper<T>) new PulsarSerializationSchemaWrapper<>(
                    topic,
                    serializationSchema,
                    recordSchemaType,
                    clazz,
                    schema,
                    dataType,
                    mode,
                    topicExtractor,
                    keyExtractor);
        }
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
        if (topicExtractor != null) {
            return topicExtractor.apply(element);
        }
        return topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        this.serializationSchema.open(context);
    }

    @Override
    public byte[] serialize(T element) {
        return serializationSchema.serialize(element);
    }

    @Override
    public void serialize(T element, TypedMessageBuilder<byte[]> messageBuilder) {
        messageBuilder.value(serializationSchema.serialize(element));
    }

    @Override
    public Schema<?> getPulsarSchema() {
        try {
            switch (schemaMode) {
                case SPECIAL:
                    return schema;
                case ATOMIC:
                    return SchemaTranslator.atomicType2PulsarSchema(dataType);
                case POJO:
                    return SchemaUtils.buildSchemaForRecordClazz(clazz, recordSchemaType);
                case ROW:
                    return SchemaUtils.buildRowSchema(dataType, recordSchemaType);
            }
        } catch (IncompatibleSchemaException e) {
            throw new IllegalStateException(e);
        }
        if (schema != null) {
            return schema;
        }
        try {
            if (dataType instanceof AtomicDataType) {
                return SchemaTranslator.atomicType2PulsarSchema(dataType);
            } else {
                // for pojo type, use avro or json
                Asserts.notNull(clazz, "for non-atomic type, you must set clazz");
                Asserts.notNull(clazz, "for non-atomic type, you must set recordSchemaType");
                return SchemaUtils.buildSchemaForRecordClazz(clazz, recordSchemaType);
            }
        } catch (IncompatibleSchemaException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getKey(T element) {
        if (keyExtractor != null) {
            return keyExtractor.apply(element);
        }
        return null;
    }

    enum SchemaMode {
        ATOMIC,
        POJO,
        SPECIAL,
        ROW
    }
}
