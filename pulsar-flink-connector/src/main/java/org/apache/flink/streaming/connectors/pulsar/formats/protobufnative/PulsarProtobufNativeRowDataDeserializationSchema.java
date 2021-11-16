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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/** The serializer which supports Pulsar protobuf native schema. */
@Slf4j
public class PulsarProtobufNativeRowDataDeserializationSchema
        implements DeserializationSchema<RowData> {

    private SerializableSupplier<Descriptors.Descriptor> loadDescriptor;
    private RowType rowType;
    private TypeInformation<RowData> rowDataTypeInfo;

    // TODO maybe add nested ProtobufRowDataDeserializationSchema structured by Descriptor?
    private transient Descriptors.Descriptor descriptor;
    /** Runtime instance that performs the actual work. */
    private transient PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter runtimeConverter;

    public PulsarProtobufNativeRowDataDeserializationSchema(
            SerializableSupplier<Descriptors.Descriptor> loadDescriptor, RowType rowType) {
        this.loadDescriptor = loadDescriptor;
        this.rowType = rowType;
        this.rowDataTypeInfo = InternalTypeInfo.of(rowType);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.descriptor = loadDescriptor.get();
        this.runtimeConverter = PulsarProtobufToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            DynamicMessage deserialize = DynamicMessage.parseFrom(descriptor, message);
            return (RowData) runtimeConverter.convert(deserialize);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize ProtobufNative record.", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }
}
