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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.deserialize.PbRowDeserializationSchema;
import org.apache.flink.formats.protobuf.testproto.SimpleTestOuterMulti;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OuterMultiProtoToRowTest {
    @Test
    public void testSimple() throws Exception {
        RowType rowType =
                PbRowTypeInformation.generateRowType(SimpleTestOuterMulti.getDescriptor());
        PbRowDeserializationSchema deserializationSchema =
                new PbRowDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        SimpleTestOuterMulti.class.getName(),
                        false,
                        false);

        SimpleTestOuterMulti simple =
                SimpleTestOuterMulti.newBuilder()
                        .setA(1)
                        .setB(2L)
                        .setC(false)
                        .setD(0.1f)
                        .setE(0.01)
                        .setF("haha")
                        .setG(ByteString.copyFrom(new byte[]{1}))
                        .build();

        RowData row = deserializationSchema.deserialize(simple.toByteArray());
        row = ProtobufTestHelper.validateRow(row, rowType);

        assertEquals(7, row.getArity());
        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertFalse(row.getBoolean(2));
        assertEquals(Float.valueOf(0.1f), Float.valueOf(row.getFloat(3)));
        assertEquals(Double.valueOf(0.01d), Double.valueOf(row.getDouble(4)));
        assertEquals("haha", row.getString(5).toString());
        assertEquals(1, (row.getBinary(6))[0]);
    }
}
