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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PbSchemaValidator {
    private Descriptors.Descriptor descriptor;
    private RowType rowType;
    private Map<JavaType, List<LogicalTypeRoot>> typeMatchMap = new HashMap();

    public PbSchemaValidator(Descriptors.Descriptor descriptor, RowType rowType) {
        this.descriptor = descriptor;
        this.rowType = rowType;
        typeMatchMap.put(JavaType.BOOLEAN, Collections.singletonList(LogicalTypeRoot.BOOLEAN));
        typeMatchMap.put(
                JavaType.BYTE_STRING,
                Arrays.asList(LogicalTypeRoot.BINARY, LogicalTypeRoot.VARBINARY));
        typeMatchMap.put(JavaType.DOUBLE, Collections.singletonList(LogicalTypeRoot.DOUBLE));
        typeMatchMap.put(JavaType.FLOAT, Collections.singletonList(LogicalTypeRoot.FLOAT));
        typeMatchMap.put(
                JavaType.ENUM, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
        typeMatchMap.put(
                JavaType.STRING, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
        typeMatchMap.put(JavaType.INT, Collections.singletonList(LogicalTypeRoot.INTEGER));
        typeMatchMap.put(JavaType.LONG, Collections.singletonList(LogicalTypeRoot.BIGINT));
    }

    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public void validate() {
        validateTypeMatch(descriptor, rowType);
        if (!descriptor
                .getFile()
                .getOptions()
                .getJavaPackage()
                .equals(descriptor.getFile().getPackage())) {
            throw new IllegalArgumentException(
                    "java_package and package must be the same in proto definition");
        }
        if (!descriptor.getFile().getOptions().getJavaMultipleFiles()) {
            throw new IllegalArgumentException("java_multiple_files must set to true");
        }
    }

    public void validateTypeMatch(Descriptors.Descriptor descriptor, RowType rowType) {
        rowType.getFields()
                .forEach(
                        rowField -> {
                            FieldDescriptor fieldDescriptor =
                                    descriptor.findFieldByName(rowField.getName());
                            if (null != fieldDescriptor) {
                                validateTypeMatch(fieldDescriptor, rowField.getType());
                            } else {
                                throw new ValidationException(
                                        "Column "
                                                + rowField.getName()
                                                + " does not exists in definition of proto class.");
                            }
                        });
    }

    public void validateTypeMatch(FieldDescriptor fd, LogicalType logicalType) {
        if (!fd.isRepeated()) {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                // simple type
                validateSimpleType(fd, logicalType.getTypeRoot());
            } else {
                // message type
                validateTypeMatch(fd.getMessageType(), (RowType) logicalType);
            }
        } else {
            if (fd.isMapField()) {
                // map type
                MapType mapType = (MapType) logicalType;
                validateSimpleType(
                        fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME),
                        mapType.getKeyType().getTypeRoot());
                validateTypeMatch(
                        fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME),
                        mapType.getValueType());
            } else {
                // array type
                ArrayType arrayType = (ArrayType) logicalType;
                if (fd.getJavaType() == JavaType.MESSAGE) {
                    // array message type
                    validateTypeMatch(fd.getMessageType(), (RowType) arrayType.getElementType());
                } else {
                    // array simple type
                    validateSimpleType(fd, arrayType.getElementType().getTypeRoot());
                }
            }
        }
    }

    private void validateSimpleType(FieldDescriptor fd, LogicalTypeRoot logicalTypeRoot) {
        if (!typeMatchMap.containsKey(fd.getJavaType())) {
            throw new ValidationException("Unsupported protobuf java type: " + fd.getJavaType());
        }
        if (typeMatchMap.get(fd.getJavaType()).stream().noneMatch(x -> x == logicalTypeRoot)) {
            throw new ValidationException(
                    "Protobuf field type does not match column type, "
                            + fd.getJavaType()
                            + "(pb) is not compatible of "
                            + logicalTypeRoot
                            + "(table DDL)");
        }
    }
}
