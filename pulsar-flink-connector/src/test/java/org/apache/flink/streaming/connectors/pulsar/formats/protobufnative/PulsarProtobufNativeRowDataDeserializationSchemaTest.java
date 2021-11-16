/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.formats.protobufnative;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.intellij.lang.annotations.Language;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator.proto2SqlType;
import static org.junit.Assert.assertEquals;

/** Unit test of PulsarProtobufNativeRowDataDeserializationSchema. */
public class PulsarProtobufNativeRowDataDeserializationSchemaTest {

    public byte[] protobufData;

    public Descriptors.Descriptor descriptor;

    @Before
    public void setUp() {
        @Language("JSON")
        String schema =
                "{\n"
                        + "  \"fileDescriptorSet\": \"CtcGCg5wYjNfdGVzdC5wcm90bxInb3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLnByb3RvYnVmLnByb3RvIuIFCgdQYjNUZXN0EgkKAWEYASABKAUSCQoBYhgCIAEoAxIJCgFjGAMgASgJEgkKAWQYBCABKAISCQoBZRgFIAEoARJCCgFmGAYgASgOMjcub3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLnByb3RvYnVmLnByb3RvLlBiM1Rlc3QuQ29ycHVzEkwKAWcYByABKAsyQS5vcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMucHJvdG9idWYucHJvdG8uUGIzVGVzdC5Jbm5lck1lc3NhZ2VUZXN0EkwKAWgYCCADKAsyQS5vcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMucHJvdG9idWYucHJvdG8uUGIzVGVzdC5Jbm5lck1lc3NhZ2VUZXN0EgkKAWkYCSABKAwSSAoEbWFwMRgKIAMoCzI6Lm9yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5wcm90b2J1Zi5wcm90by5QYjNUZXN0Lk1hcDFFbnRyeRJICgRtYXAyGAsgAygLMjoub3JnLmFwYWNoZS5mbGluay5mb3JtYXRzLnByb3RvYnVmLnByb3RvLlBiM1Rlc3QuTWFwMkVudHJ5GisKCU1hcDFFbnRyeRILCgNrZXkYASABKAkSDQoFdmFsdWUYAiABKAk6AjgBGm4KCU1hcDJFbnRyeRILCgNrZXkYASABKAkSUAoFdmFsdWUYAiABKAsyQS5vcmcuYXBhY2hlLmZsaW5rLmZvcm1hdHMucHJvdG9idWYucHJvdG8uUGIzVGVzdC5Jbm5lck1lc3NhZ2VUZXN0OgI4ARooChBJbm5lck1lc3NhZ2VUZXN0EgkKAWEYASABKAUSCQoBYhgCIAEoAyJaCgZDb3JwdXMSDQoJVU5JVkVSU0FMEAASBwoDV0VCEAESCgoGSU1BR0VTEAISCQoFTE9DQUwQAxIICgRORVdTEAQSDAoIUFJPRFVDVFMQBRIJCgVWSURFTxAHQi8KK29yZy5hcGFjaGUuZmxpbmsuZm9ybWF0cy5wcm90b2J1Zi50ZXN0cHJvdG9QAWIGcHJvdG8z\",\n"
                        + "  \"rootMessageTypeName\": \"org.apache.flink.formats.protobuf.proto.Pb3Test\",\n"
                        + "  \"rootFileDescriptorName\": \"pb3_test.proto\"\n"
                        + "}";
        descriptor = ProtobufNativeSchemaUtils.deserialize(schema.getBytes(StandardCharsets.UTF_8));
        protobufData =
                Base64.getDecoder()
                        .decode(
                                "CGMQARoEdGVzdCXNzIw/KcP1KFyPwvE/MAE6BAhjEGNCBAhjEGNCBAhjEGNKATFSCgoDMTExEgMyMjJSDAoEMTExMhIEMjIyM1oLCgMxMTESBAhjEGNaDAoEMTExMhIECGMQYw==");
    }

    @Test
    public void deserialize() throws Exception {
        RowType rowType = (RowType) proto2SqlType(descriptor).getLogicalType();
        PulsarProtobufNativeRowDataDeserializationSchema deserializationSchema =
                new PulsarProtobufNativeRowDataDeserializationSchema(() -> descriptor, rowType);
        deserializationSchema.open(null);

        final RowData rowData = deserializationSchema.deserialize(protobufData);
        RowData newRowData = rowData;
        assertEquals(11, newRowData.getArity());
        assertEquals(99, newRowData.getInt(0));
        assertEquals(1L, newRowData.getLong(1));
        assertEquals("test", newRowData.getString(2).toString());
        assertEquals(Float.valueOf(1.1f), Float.valueOf(newRowData.getFloat(3)));
        assertEquals(Double.valueOf(1.11d), Double.valueOf(newRowData.getDouble(4)));
        assertEquals("WEB", newRowData.getString(5).toString());
        assertEquals("+I(99,99)", newRowData.getRow(6, 2).toString());
        assertEquals(2, newRowData.getArray(7).size());
        assertEquals(99, newRowData.getArray(7).getRow(0, 1).getInt(0));
        assertEquals(99, newRowData.getArray(7).getRow(1, 1).getInt(0));
        assertEquals(49, newRowData.getBinary(8)[0]);
        assertEquals(2, newRowData.getMap(9).size());
        assertEquals("1112", newRowData.getMap(9).keyArray().getString(0).toString());
        assertEquals("2223", newRowData.getMap(9).valueArray().getString(0).toString());
        assertEquals(2, newRowData.getMap(10).size());
        assertEquals("1112", newRowData.getMap(10).keyArray().getString(0).toString());
        assertEquals("+I(99,99)", newRowData.getMap(10).valueArray().getRow(0, 2).toString());
    }
}
