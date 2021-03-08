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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.formats.protobuf.PbCodegenAppender;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import java.util.List;

public class PbCodegenRowDeserializer implements PbCodegenDeserializer {
    private List<Descriptors.FieldDescriptor> fds;
    private Descriptors.Descriptor descriptor;
    private RowType rowType;
    private boolean readDefaultValues;
    private PbCodegenAppender appender = new PbCodegenAppender();

    public PbCodegenRowDeserializer(
            Descriptors.Descriptor descriptor, RowType rowType, boolean readDefaultValues) {
        this.fds = descriptor.getFields();
        this.rowType = rowType;
        this.descriptor = descriptor;
        this.readDefaultValues = readDefaultValues;
    }

    @Override
    public String codegen(String returnVarName, String messageGetStr) throws PbCodegenException {
        // The type of messageGetStr is a native pb object,
        // it should be converted to RowData of flink internal type
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        String pbMessageVar = "message" + uid;
        String rowDataVar = "rowData" + uid;

        int fieldSize = rowType.getFieldNames().size();
        String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
        appender.appendLine(pbMessageTypeStr + " " + pbMessageVar + " = " + messageGetStr);
        appender.appendLine(
                "GenericRowData " + rowDataVar + " = new GenericRowData(" + fieldSize + ")");
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            int subUid = varUid.getAndIncrement();
            String elementDataVar = "elementDataVar" + subUid;

            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            Descriptors.FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
            String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
            PbCodegenDeserializer codegen =
                    PbCodegenDeserializeFactory.getPbCodegenDes(
                            elementFd, subType, readDefaultValues);
            appender.appendLine("Object " + elementDataVar + " = null");
            if (!readDefaultValues) {
                // only works in syntax=proto2 and readDefaultValues=false
                // readDefaultValues must be true in pb3 mode
                String isMessageNonEmptyStr =
                        isMessageNonEmptyStr(pbMessageVar, strongCamelFieldName, elementFd);
                appender.appendSegment("if(" + isMessageNonEmptyStr + "){");
            }
            String elementMessageGetStr =
                    pbMessageElementGetStr(pbMessageVar, strongCamelFieldName, elementFd);
            if (!elementFd.isRepeated()) {
                // field is not map or array
                // this step is needed to convert primitive type to boxed type to unify the object
                // interface
                switch (elementFd.getJavaType()) {
                    case INT:
                        elementMessageGetStr = "Integer.valueOf(" + elementMessageGetStr + ")";
                        break;
                    case LONG:
                        elementMessageGetStr = "Long.valueOf(" + elementMessageGetStr + ")";
                        break;
                    case FLOAT:
                        elementMessageGetStr = "Float.valueOf(" + elementMessageGetStr + ")";
                        break;
                    case DOUBLE:
                        elementMessageGetStr = "Double.valueOf(" + elementMessageGetStr + ")";
                        break;
                    case BOOLEAN:
                        elementMessageGetStr = "Boolean.valueOf(" + elementMessageGetStr + ")";
                        break;
                }
            }

            String code = codegen.codegen(elementDataVar, elementMessageGetStr);
            appender.appendSegment(code);
            if (!readDefaultValues) {
                appender.appendSegment("}");
            }
            appender.appendLine(rowDataVar + ".setField(" + index + ", " + elementDataVar + ")");
            index += 1;
        }
        appender.appendLine(returnVarName + " = " + rowDataVar);
        return appender.code();
    }

    private String pbMessageElementGetStr(
            String message, String fieldName, Descriptors.FieldDescriptor fd) {
        if (fd.isMapField()) {
            return message + ".get" + fieldName + "Map()";
        } else if (fd.isRepeated()) {
            return message + ".get" + fieldName + "List()";
        } else {
            return message + ".get" + fieldName + "()";
        }
    }

    private String isMessageNonEmptyStr(
            String message, String fieldName, Descriptors.FieldDescriptor fd) {
        if (fd.isRepeated()) {
            return message + ".get" + fieldName + "Count() > 0";
        } else {
            // proto syntax class do not have hasName() interface
            return message + ".has" + fieldName + "()";
        }
    }
}
