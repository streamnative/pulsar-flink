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

package org.apache.flink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import org.apache.pulsar.client.api.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * A deserializationSchema for integer type.
 */
public class IntegerDeserializer implements DeserializationSchema<Integer> {
    private final TypeInformation<Integer> ti;
    private final TypeSerializer<Integer> ser;

    public IntegerDeserializer() {
        this.ti = Types.INT();
        this.ser = ti.createSerializer(new ExecutionConfig());
    }

    @Override
    public Integer deserialize(byte[] message) throws IOException {
        return Schema.INT32.decode(message);
    }

    @Override
    public boolean isEndOfStream(Integer nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Integer> getProducedType() {
        return ti;
    }
}
