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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class PbRowDeserializationSchema implements DeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PbRowDeserializationSchema.class);
    private static final long serialVersionUID = -4040917522067315718L;

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;

    private final String messageClassName;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;

    private transient ProtoToRowConverter protoToRowConverter;

    public PbRowDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            String messageClassName,
            boolean ignoreParseErrors,
            boolean readDefaultValues) {
        checkNotNull(rowType, "Type information");
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
        // do it in client side to report error in the first place
        new PbSchemaValidator(PbFormatUtils.getDescriptor(messageClassName), rowType).validate();
        // this step is only used to validate codegen in client side in the first place
        try {
            protoToRowConverter =
                    new ProtoToRowConverter(messageClassName, rowType, readDefaultValues);
        } catch (PbCodegenException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoToRowConverter = new ProtoToRowConverter(messageClassName, rowType, readDefaultValues);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            return protoToRowConverter.convertProtoBinaryToRow(message);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            LOG.error("Failed to deserialize PB object.", t);
            throw new IOException("Failed to deserialize PB object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbRowDeserializationSchema that = (PbRowDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && readDefaultValues == that.readDefaultValues
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo)
                && Objects.equals(messageClassName, that.messageClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rowType, resultTypeInfo, messageClassName, ignoreParseErrors, readDefaultValues);
    }
}
