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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.pulsar.testutils.SingletonStreamSink;
import org.apache.flink.streaming.connectors.pulsar.util.RowDataUtil;
import org.apache.flink.streaming.util.serialization.FlinkSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.avro.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BOOLEAN_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BYTES_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.DOUBLE_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.FLOAT_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INTEGER_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_16_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_64_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_8_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.STRING_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.localDateList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.localDateTimeList;

/**
 * Schema related integration tests.
 */
public class SchemaITest extends PulsarTestBaseWithFlink {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaITest.class);

    @Before
    public void clearState() {
        SingletonStreamSink.clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test(timeout = 100 * 1000L)
    public void testBooleanRead() throws Exception {
        checkRead(SchemaType.BOOLEAN, DataTypes.BOOLEAN(), BOOLEAN_LIST, null, Boolean.class);
    }

    @Test(timeout = 100 * 1000L)
    public void testBooleanWrite() throws Exception {
        checkWrite(SchemaType.BOOLEAN, DataTypes.BOOLEAN(), BOOLEAN_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testINT32Read() throws Exception {
        checkRead(SchemaType.INT32, DataTypes.INT(), INTEGER_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testINT32Write() throws Exception {
        checkWrite(SchemaType.INT32, DataTypes.INT(), INTEGER_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testINT64Read() throws Exception {
        checkRead(SchemaType.INT64, DataTypes.BIGINT(), INT_64_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testINT64Write() throws Exception {
        checkWrite(SchemaType.INT64, DataTypes.BIGINT(), INT_64_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testStringRead() throws Exception {
        checkRead(SchemaType.STRING, DataTypes.STRING(), STRING_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testStringWrite() throws Exception {
        checkWrite(SchemaType.STRING, DataTypes.STRING(), STRING_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testByteRead() throws Exception {
        checkRead(SchemaType.INT8, DataTypes.TINYINT(), INT_8_LIST, null, null);
    }

    @Test
    public void testByteWrite() throws Exception {
        checkWrite(SchemaType.INT8, DataTypes.TINYINT(), INT_8_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testShortRead() throws Exception {
        checkRead(SchemaType.INT16, DataTypes.SMALLINT(), INT_16_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testShortWrite() throws Exception {
        checkWrite(SchemaType.INT16, DataTypes.SMALLINT(), INT_16_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testFloatRead() throws Exception {
        checkRead(SchemaType.FLOAT, DataTypes.FLOAT(), FLOAT_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testFloatWrite() throws Exception {
        checkWrite(SchemaType.FLOAT, DataTypes.FLOAT(), FLOAT_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testDoubleRead() throws Exception {
        checkRead(SchemaType.DOUBLE, DataTypes.DOUBLE(), DOUBLE_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testDoubleWrite() throws Exception {
        checkWrite(SchemaType.DOUBLE, DataTypes.DOUBLE(), DOUBLE_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testDateRead() throws Exception {
        checkRead(SchemaType.LOCAL_DATE, DataTypes.DATE(),
                localDateList, null, null);
    }

    @Test
    public void testDateWrite() throws Exception {
        checkWrite(SchemaType.LOCAL_DATE,
                DataTypes.DATE(),
                localDateList, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testTimestampRead() throws Exception {
        checkRead(SchemaType.LOCAL_DATE_TIME,
                DataTypes.TIMESTAMP(3), localDateTimeList, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testTimestampWrite() throws Exception {
        checkWrite(SchemaType.LOCAL_DATE_TIME,
                DataTypes.TIMESTAMP(3), localDateTimeList, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testByteArrayRead() throws Exception {
        checkRead(SchemaType.BYTES, DataTypes.BYTES(), BYTES_LIST, null, null);
    }

    @Test(timeout = 100 * 1000L)
    public void testByteArrayWrite() throws Exception {
        checkWrite(SchemaType.BYTES, DataTypes.BYTES(), BYTES_LIST, t -> StringUtils.arrayAwareToString(t), null);
    }

    private <T> void checkRead(SchemaType type, DataType dt, List<T> datas, Function<T, String> toStr, Class<T> tClass)
            throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);
        String table = newTopic();
        String tableName = TopicName.get(table).getLocalName();

        TableSchema tSchema = TableSchema.builder().field("value", dt).build();

        final Schema schema = AvroSchemaConverter.convertToSchema(tSchema.toRowDataType().getLogicalType());

        final AvroRowDataSerializationSchema serializationSchema =
                new AvroRowDataSerializationSchema((RowType) tSchema.toRowDataType().getLogicalType());
        serializationSchema.open(null);
        final FlinkSchema<RowData> flinkSchema = new FlinkSchema<>(avroSchema2SchemaInfo(schema),
                serializationSchema, null);
        List<RowData> rowData = wrapperRowData(datas);
        sendAvroMessages(table, type, rowData, Optional.empty(), flinkSchema);

        tEnv.executeSql(createTableSql(tableName, table, tSchema, "avro")).print();

        Table t = tEnv.sqlQuery("select `value` from " + tableName);

        tEnv.toAppendStream(t, InternalTypeInfo.of(tSchema.toRowDataType().getLogicalType()))
                .map(new FailingIdentityMapper<>(datas.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);
        TestUtils.tryExecute(see, "read from earliest");
        if (toStr == null) {
            SingletonStreamSink.compareWithList(
                    rowData.subList(0, datas.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
        } else {
//            SingletonStreamSink.compareWithList(rowData.subList(0, datas.size() - 1).stream().map(e -> toStr.apply(e)).collect(Collectors.toList()));
        }
    }

    private <T> List<RowData> wrapperRowData(List<T> datas) {
        return datas.stream().map(t -> {
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
            RowDataUtil.setField(rowData, 0, t);
            return rowData;
        }).collect(Collectors.toList());
    }

    private SchemaInfo avroSchema2SchemaInfo(Schema schema) {
        byte[] schemaBytes = schema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfo si = new SchemaInfo();
        si.setName("Avro");
        si.setSchema(schemaBytes);
        si.setType(SchemaType.AVRO);
        return si;
    }

    private <T> void checkWrite(SchemaType type, DataType dt, List<T> datas, Function<T, String> toStr, Class<T> tClass)
            throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

        String topic = newTopic();
        String tableName = TopicName.get(topic).getLocalName();

        TableSchema tSchema = TableSchema.builder().field("value", dt).build();

        TypeInformation<RowData> ti = InternalTypeInfo.of(tSchema.toRowDataType().getLogicalType());

        DataStream<RowData> stream = see.fromCollection(wrapperRowData(datas), ti);
        tEnv.executeSql(createTableSql(tableName, topic, tSchema, "avro")).print();
        tEnv.fromDataStream(stream).executeInsert(tableName).print();

        Thread.sleep(3000);
        StreamExecutionEnvironment se2 = StreamExecutionEnvironment.getExecutionEnvironment();
        se2.setParallelism(1);
        StreamTableEnvironment tEnv2 = StreamTableEnvironment.create(se2);

        tEnv2.executeSql(createTableSql(tableName, topic, tSchema, "avro")).print();
        Table t = tEnv2.sqlQuery("select `value` from " + tableName);
        tEnv2.toAppendStream(t, t.getSchema().toRowType())
                .map(new FailingIdentityMapper<>(datas.size()))
                .addSink(new SingletonStreamSink.StringSink<>()).setParallelism(1);

        FailingIdentityMapper.failedBefore = false;
        SingletonStreamSink.clear();

        Thread reader = new Thread("read") {
            @Override
            public void run() {
                try {
                    TestUtils.tryExecute(se2, "read");
                } catch (Throwable e) {
                    // do nothing
                    LOGGER.error("read fail", e);
                }
            }
        };

        reader.start();
        reader.join();

        if (toStr == null) {
            SingletonStreamSink.compareWithList(
                    datas.subList(0, datas.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
        } else {
            SingletonStreamSink.compareWithList(
                    datas.subList(0, datas.size() - 1).stream().map(e -> toStr.apply(e)).collect(Collectors.toList()));
        }
    }
}
