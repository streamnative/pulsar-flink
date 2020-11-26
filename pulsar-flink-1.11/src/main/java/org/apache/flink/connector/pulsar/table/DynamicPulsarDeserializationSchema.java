/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;

public class DynamicPulsarDeserializationSchema implements PulsarDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final boolean hasMetadata;

    private final ReadableRowDataMetadataConverter[] metadataConverters;

    private final TypeInformation<RowData> producedTypeInfo;

    DynamicPulsarDeserializationSchema(
            DeserializationSchema<RowData> valueDeserialization,
            boolean hasMetadata,
            ReadableRowDataMetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo) {
        this.hasMetadata = hasMetadata;
        this.valueDeserialization = valueDeserialization;
        this.producedTypeInfo = producedTypeInfo;
        this.metadataConverters = metadataConverters;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(Message record) throws IOException {
        // shortcut if no metadata is required
        if (!hasMetadata) {
            return valueDeserialization.deserialize(record.getData());
        } else {
            RowData physicalRow = valueDeserialization.deserialize(record.getData());
            final GenericRowData genericPhysicalRow = (GenericRowData) physicalRow;
            final int physicalArity = physicalRow.getArity();
            final int metadataArity = metadataConverters.length;

            final GenericRowData producedRow = new GenericRowData(
                    physicalRow.getRowKind(),
                    physicalArity + metadataArity);

            for (int i = 0; i < physicalArity; i++) {
                producedRow.setField(i, genericPhysicalRow.getField(i));
            }

            for (int i = 0; i < metadataArity; i++) {
                producedRow.setField(i + physicalArity, metadataConverters[i].read(record));
            }
            return producedRow;
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        if (hasMetadata) {
            return producedTypeInfo;
        } else {
            return valueDeserialization.getProducedType();
        }
    }

    // --------------------------------------------------------------------------------------------

    interface ReadableRowDataMetadataConverter extends Serializable {
        Object read(Message record);
    }
}
