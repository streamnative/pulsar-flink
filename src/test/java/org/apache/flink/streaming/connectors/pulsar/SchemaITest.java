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
package org.apache.flink.streaming.connectors.pulsar;

import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Pulsar;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.bytesList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.dateList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.doubleList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.floatList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.int16List;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.int32List;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.int8List;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.stringList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.timestampList;
import static org.junit.Assert.assertEquals;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.booleanList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.fooList;

public class SchemaITest extends PulsarTestBaseWithFlink {

    @Before
    public void clearState() {
        StreamITCase.testResults().clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test
    public void testBooleanRead() throws Exception {
        checkRead(SchemaType.BOOLEAN, booleanList, null, null);
    }

    @Test
    public void testBooleanWrite() throws Exception {
        checkWrite(SchemaType.BOOLEAN, DataTypes.BOOLEAN(), booleanList, null, null);
    }

    @Test
    public void testINT32Read() throws Exception {
        checkRead(SchemaType.INT32, int32List, null, null);
    }

    @Test
    public void testINT32Write() throws Exception {
        checkWrite(SchemaType.INT32, DataTypes.INT(), int32List, null, null);
    }


    @Test
    public void testStringRead() throws Exception {
        checkRead(SchemaType.STRING, stringList, null, null);
    }

    @Test
    public void testStringWrite() throws Exception {
        checkWrite(SchemaType.STRING, DataTypes.STRING(), stringList, null, null);
    }


    @Test
    public void testByteRead() throws Exception {
        checkRead(SchemaType.INT8, int8List, null, null);
    }

    @Test
    public void testByteWrite() throws Exception {
        checkWrite(SchemaType.INT8, DataTypes.TINYINT(), int8List, null, null);
    }


    @Test
    public void testShortRead() throws Exception {
        checkRead(SchemaType.INT16, int16List, null, null);
    }

    @Test
    public void testShortWrite() throws Exception {
        checkWrite(SchemaType.INT16, DataTypes.SMALLINT(), int16List, null, null);
    }


    @Test
    public void testFloatRead() throws Exception {
        checkRead(SchemaType.FLOAT, floatList, null, null);
    }

    @Test
    public void testFloatWrite() throws Exception {
        checkWrite(SchemaType.FLOAT, DataTypes.FLOAT(), floatList, null, null);
    }


    @Test
    public void testDoubleRead() throws Exception {
        checkRead(SchemaType.DOUBLE, doubleList, null, null);
    }

    @Test
    public void testDoubleWrite() throws Exception {
        checkWrite(SchemaType.DOUBLE, DataTypes.DOUBLE(), doubleList, null, null);
    }

    @Test
    public void testDateRead() throws Exception {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        checkRead(SchemaType.DATE, dateList, t -> dateFormat.format(t), null);
    }

    @Test
    public void testDateWrite() throws Exception {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        checkWrite(SchemaType.DATE,
            DataTypes.DATE().bridgedTo(java.sql.Date.class),
            dateList.stream().map(t -> new java.sql.Date(t.getTime())).collect(Collectors.toList()),
            t -> dateFormat.format(t), null);
    }

    @Test
    public void testTimestampRead() throws Exception {
        checkRead(SchemaType.TIMESTAMP, timestampList, null, null);
    }

    @Test
    public void testTimestampWrite() throws Exception {
        checkWrite(SchemaType.TIMESTAMP,
            DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class), timestampList, null, null);
    }

    @Test
    public void testByteArrayRead() throws Exception {
        checkRead(SchemaType.BYTES, bytesList, t -> StringUtils.arrayAwareToString(t), null);
    }

    @Test
    public void testByteArrayWrite() throws Exception {
        checkWrite(SchemaType.BYTES, DataTypes.BYTES(), bytesList, t -> StringUtils.arrayAwareToString(t), null);
    }

    private <T> void checkRead(SchemaType type, List<T> datas, Function<T, String> toStr, Class<T> tClass) throws Exception {
        val see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        val tEnv = StreamTableEnvironment.create(see);

        val table = newTopic();
        sendTypedMessages(table, type, datas, Optional.empty(), tClass);

        tEnv.connect(getPulsarSourceDescriptor(table))
            .inAppendMode()
            .registerTableSource(table);

        val t = tEnv.scan(table).select("value");
        tEnv.toAppendStream(t, Row.class)
            .map(new FailingIdentityMapper<>(datas.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val reader = new Thread("read") {
            @Override
            public void run() {
                try {
                    see.execute("read from earliest");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        reader.start();
        reader.join();

        if (toStr == null) {
            assertEquals(StreamITCase.testResults(),
                datas.subList(0, datas.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
        } else {
            assertEquals(StreamITCase.testResults(),
                datas.subList(0, datas.size() - 1).stream().map(e -> toStr.apply(e)).collect(Collectors.toList()));
        }
    }

    private <T> void checkWrite(SchemaType type, DataType dt, List<T> datas, Function<T, String> toStr, Class<T> tClass) throws Exception {
        val see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        val tEnv = StreamTableEnvironment.create(see);

        val table = newTopic();
        val tableName = TopicName.get(table).getLocalName();

        val tSchema = TableSchema.builder().field("value", dt).build();

        val stream = see.fromCollection(datas);
        tEnv.registerDataStream("origin", stream);

        tEnv.connect(getPulsarSinkDescriptor(table))
            .withSchema(new Schema().schema(tSchema))
            .inAppendMode()
            .registerTableSink(tableName);

        tEnv.sqlUpdate("insert into " + tableName + " select * from origin");

        val sinkThread = new Thread("sink") {
            @Override
            public void run() {
                try {
                    see.execute("sink");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        sinkThread.start();
        sinkThread.join();

        val se2 = StreamExecutionEnvironment.getExecutionEnvironment();
        se2.setParallelism(1);
        val tEnv2 = StreamTableEnvironment.create(se2);

        tEnv2.connect(getPulsarSourceDescriptor(table))
            .inAppendMode()
            .registerTableSource(table);


        val t = tEnv2.scan(table).select("value");
        tEnv2.toAppendStream(t, Row.class)
            .map(new FailingIdentityMapper<>(datas.size()))
            .addSink(new StreamITCase.StringSink<>()).setParallelism(1);

        val reader = new Thread("read") {
            @Override
            public void run() {
                try {
                    see.execute("read");
                } catch (Throwable e) {
                    // do nothing
                }
            }
        };

        reader.start();
        reader.join();

        if (toStr == null) {
            assertEquals(StreamITCase.testResults(),
                datas.subList(0, datas.size() - 1).stream().map(Objects::toString).collect(Collectors.toList()));
        } else {
            assertEquals(StreamITCase.testResults(),
                datas.subList(0, datas.size() - 1).stream().map(e -> toStr.apply(e)).collect(Collectors.toList()));
        }
    }


    private ConnectorDescriptor getPulsarSourceDescriptor(String tableName) {
        return new Pulsar()
            .urls(getServiceUrl(), getAdminUrl())
            .topic(tableName)
            .startFromEarliest()
            .property(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000");
    }

    private ConnectorDescriptor getPulsarSinkDescriptor(String tableName) {
        return new Pulsar()
            .urls(getServiceUrl(), getAdminUrl())
            .topic(tableName)
            .property(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true")
            .property(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "true");
    }

    private final AtomicInteger topicId = new AtomicInteger(0);

    private String newTopic() {
        return TopicName.get("topic-" + topicId.getAndIncrement()).toString();
    }
}
