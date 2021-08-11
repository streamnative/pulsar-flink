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

package org.apache.flink.formats.protobufnative;

import org.apache.flink.formats.protobuf.PbRowTypeInformation;
import org.apache.flink.streaming.connectors.pulsar.SchemaData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PulsarProtobufNativeRowDataDeserializationSchemaTest {

    @Test
    public void deserialize() throws Exception {
        RowType rowType = PbRowTypeInformation.generateRowType(SchemaData.descriptor);
        PulsarProtobufNativeRowDataDeserializationSchema deserializationSchema = new PulsarProtobufNativeRowDataDeserializationSchema(() -> SchemaData.descriptor, rowType);
        deserializationSchema.open(null);

        final RowData rowData = deserializationSchema.deserialize(SchemaData.protobufData);
        RowData newRowData = rowData;
        assertEquals(9, newRowData.getArity());
        assertEquals(1, newRowData.getInt(0));
        assertEquals(2L, newRowData.getLong(1));
        assertFalse((boolean) newRowData.getBoolean(2));
        assertEquals(Float.valueOf(0.1f), Float.valueOf(newRowData.getFloat(3)));
        assertEquals(Double.valueOf(0.01d), Double.valueOf(newRowData.getDouble(4)));
        assertEquals("haha", newRowData.getString(5).toString());
        assertEquals(1, (newRowData.getBinary(6))[0]);
        assertEquals("IMAGES", newRowData.getString(7).toString());
        assertEquals(1, newRowData.getInt(8));
    }
}
