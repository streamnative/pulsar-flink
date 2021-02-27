/*
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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.util.serialization.FlinkSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

import javax.validation.constraints.NotNull;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.junit.Assert.assertEquals;

/**
 * Test for the Avro、json serialization and deserialization schema.
 */
@Slf4j
public class RowDataDerSerializationSchemaTest extends PulsarTestBase {

    @Test
    public void testAvroSerializeDeserialize() throws Exception {
        String topicName = newTopic();
        final DataType dataType = getAvroDataType();
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        Schema avroSchema = AvroSchemaConverter.convertToSchema(rowType);
        if (avroSchema.isNullable()) {
            avroSchema = avroSchema.getTypes().stream().filter(s -> s.getType() == RECORD).findAny().get();
        }
        final GenericRecord record = getGenericRecord(avroSchema);
        byte[] input = buildGenericRecordBytes(avroSchema, record);

        AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
        serializationSchema.open(null);
        AvroRowDataDeserializationSchema deserializationSchema =
                new AvroRowDataDeserializationSchema(rowType, typeInfo);
        deserializationSchema.open(null);
        RowData rowData = deserializationSchema.deserialize(input);
        final org.apache.pulsar.client.api.Schema<RowData> pulsarSchema =
                toPulsarSchema(SchemaType.AVRO, avroSchema,
                        serializationSchema, deserializationSchema);
        sendMessage(topicName, pulsarSchema, rowData);
        final CompletableFuture<byte[]> consumer = autoConsumer(topicName
        );

        RowData newRowData = deserializationSchema.deserialize(consumer.get(2000, TimeUnit.MILLISECONDS));
        assertEquals(rowData, newRowData);
    }

    @Test
    public void testJsonSerializeDeserialize() throws Exception {
        String topicName = newTopic();
        DataType dataType = getJsonDataType();
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        Schema avroSchema = AvroSchemaConverter.convertToSchema(rowType);
        if (avroSchema.isNullable()) {
            avroSchema = avroSchema.getTypes().stream().filter(s -> s.getType() == RECORD).findAny().get();
        }
        byte[] serializedJson = getJsonBytes();

        JsonRowDataSerializationSchema serializationSchema =
                new JsonRowDataSerializationSchema(rowType, TimestampFormat.ISO_8601, JsonOptions.MapNullKeyMode.DROP,
                        "");
        serializationSchema.open(null);
        JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(rowType, typeInfo,
                false, false, TimestampFormat.ISO_8601);
        deserializationSchema.open(null);
        RowData rowData = deserializationSchema.deserialize(serializedJson);

        sendMessage(topicName, toPulsarSchema(SchemaType.JSON, avroSchema, serializationSchema, deserializationSchema),
                rowData);
        final CompletableFuture<byte[]> consumer = autoConsumer(topicName);

        RowData newRowData = deserializationSchema.deserialize(consumer.get(2000, TimeUnit.MILLISECONDS));
        assertEquals(rowData, newRowData);
    }

    @NotNull
    private DataType getJsonDataType() {
        return ROW(
                FIELD("bool", BOOLEAN()),
                FIELD("tinyint", TINYINT()),
                FIELD("smallint", SMALLINT()),
                FIELD("int", INT()),
                FIELD("bigint", BIGINT()),
                FIELD("float", FLOAT()),
                FIELD("name", STRING()),
                FIELD("bytes", BYTES()),
                FIELD("decimal", DECIMAL(9, 6)),
                FIELD("doubles", ARRAY(DOUBLE())),
                FIELD("date", DATE()),
                FIELD("time", TIME(0)),
                FIELD("timestamp3", TIMESTAMP(3)),
                FIELD("map", MAP(STRING(), BIGINT())),
                FIELD("multiSet", MULTISET(STRING())),
                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
    }

    private byte[] getJsonBytes() throws JsonProcessingException {
        byte tinyint = 'c';
        short smallint = 128;
        int intValue = 45536;
        float floatValue = 33.333F;
        long bigint = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        BigDecimal decimal = new BigDecimal("123.456789");

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);

        Map<String, Integer> multiSet = new HashMap<>();
        multiSet.put("blink", 2);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode doubleNode = objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D);

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("bool", true);
        root.put("tinyint", tinyint);
        root.put("smallint", smallint);
        root.put("int", intValue);
        root.put("bigint", bigint);
        root.put("float", floatValue);
        root.put("name", name);
        root.put("bytes", bytes);
        root.put("decimal", decimal);
        root.set("doubles", doubleNode);
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43");
        root.put("timestamp3", "1990-10-14T12:12:43.123");
        root.putObject("map").put("flink", 123);
        root.putObject("multiSet").put("blink", 2);
        root.putObject("map2map").putObject("inner_map").put("key", 234);
        return objectMapper.writeValueAsBytes(root);
    }

    public <T> org.apache.pulsar.client.api.Schema<T> toPulsarSchema(SchemaType schemaType, Schema avroSchema,
                                                                     SerializationSchema<T> serializationSchema,
                                                                     DeserializationSchema<T> deserializationSchema) {
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfo si = new SchemaInfo();
        si.setName("Record");
        si.setSchema(schemaBytes);
        si.setType(schemaType);
        return new FlinkSchema<>(si, serializationSchema, deserializationSchema);
    }

    public void sendMessage(String topic, org.apache.pulsar.client.api.Schema<RowData> schema, RowData data)
            throws Exception {
        try (
                PulsarClient pulsarClient = PulsarClient.builder()
                        .serviceUrl(serviceUrl)
                        .build();
                final Producer<RowData> producer = pulsarClient.newProducer(schema)
                        .topic(topic)
                        .create()) {
            pulsarClient
                    .newConsumer(new AutoConsumeSchema())
                    .topic(topic)
                    .subscriptionName("test")
                    .subscribe()
                    .close();
            producer.send(data);
        }
    }

    public CompletableFuture<byte[]> autoConsumer(String topic)
            throws Exception {
        return CompletableFuture.supplyAsync(() -> {
                    try (
                            PulsarClient pulsarClient = PulsarClient.builder()
                                    .serviceUrl(serviceUrl)
                                    .build();
                            final Consumer<org.apache.pulsar.client.api.schema.GenericRecord> consumer = pulsarClient
                                    .newConsumer(new AutoConsumeSchema())
                                    .topic(topic)
                                    .subscriptionName("test")
                                    .subscribe();
                    ) {
                        final Message<org.apache.pulsar.client.api.schema.GenericRecord> receive = consumer.receive();
                        return receive.getData();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private byte[] buildGenericRecordBytes(Schema schema, GenericRecord record) throws java.io.IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        return byteArrayOutputStream.toByteArray();
    }

    @NotNull
    private GenericRecord getGenericRecord(Schema schema) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put(0, true);
        record.put(1, (int) Byte.MAX_VALUE);
        record.put(2, (int) Short.MAX_VALUE);
        record.put(3, 33);
        record.put(4, 44L);
        record.put(5, 12.34F);
        record.put(6, 23.45);
        record.put(7, "hello avro");
        record.put(8, ByteBuffer.wrap(new byte[]{1, 2, 4, 5, 6, 7, 8, 12}));

        record.put(9, ByteBuffer.wrap(
                BigDecimal.valueOf(123456789, 6).unscaledValue().toByteArray()));

        List<Double> doubles = new ArrayList<>();
        doubles.add(1.2);
        doubles.add(3.4);
        doubles.add(567.8901);
        record.put(10, doubles);

        record.put(11, 18397);
        record.put(12, 10087);
        record.put(13, 1589530213123L);
        record.put(14, 1589530213122L);

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 12L);
        map.put("avro", 23L);
        record.put(15, map);

        Map<String, Map<String, Integer>> map2map = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("inner_key1", 123);
        innerMap.put("inner_key2", 234);
        map2map.put("outer_key", innerMap);
        record.put(16, map2map);

        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> list2 = Arrays.asList(11, 22, 33, 44, 55);
        Map<String, List<Integer>> map2list = new HashMap<>();
        map2list.put("list1", list1);
        map2list.put("list2", list2);
        record.put(17, map2list);

        Map<String, String> map2 = new HashMap<>();
        map2.put("key1", null);
        record.put(18, map2);
        return record;
    }

    @NotNull
    private DataType getAvroDataType() {
        final DataType dataType = ROW(
                FIELD("bool", BOOLEAN()),
                FIELD("tinyint", TINYINT()),
                FIELD("smallint", SMALLINT()),
                FIELD("int", INT()),
                FIELD("bigint", BIGINT()),
                FIELD("float", FLOAT()),
                FIELD("double", DOUBLE()),
                FIELD("name", STRING()),
                FIELD("bytes", BYTES()),
                FIELD("decimal", DECIMAL(19, 6)),
                FIELD("doubles", ARRAY(DOUBLE())),
                FIELD("time", TIME(0)),
                FIELD("date", DATE()),
                FIELD("timestamp3", TIMESTAMP(3)),
                FIELD("timestamp3_2", TIMESTAMP(3)),
                FIELD("map", MAP(STRING(), BIGINT())),
                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
                FIELD("map2array", MAP(STRING(), ARRAY(INT()))),
                FIELD("nullEntryMap", MAP(STRING(), STRING())))
                .notNull();
        return dataType;
    }
}
