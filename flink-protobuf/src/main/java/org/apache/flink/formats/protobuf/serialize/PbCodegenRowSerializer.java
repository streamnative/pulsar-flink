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

package org.apache.flink.formats.protobuf.serialize;

import org.apache.flink.formats.protobuf.PbCodegenAppender;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbCodegenUtils;
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

import java.util.List;

public class PbCodegenRowSerializer implements PbCodegenSerializer {
    private List<Descriptors.FieldDescriptor> fds;
    private Descriptors.Descriptor descriptor;
    private RowType rowType;

    public PbCodegenRowSerializer(Descriptors.Descriptor descriptor, RowType rowType) {
        this.fds = descriptor.getFields();
        this.rowType = rowType;
        this.descriptor = descriptor;
    }

    @Override
    public String codegen(String returnVarName, String rowFieldGetStr) throws PbCodegenException {
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        PbCodegenAppender appender = new PbCodegenAppender();
        String rowDataVar = "rowData" + uid;
        String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
        String messageBuilderVar = "messageBuilder" + uid;
        appender.appendLine("RowData " + rowDataVar + " = " + rowFieldGetStr);
        appender.appendLine(
                pbMessageTypeStr
                        + ".Builder "
                        + messageBuilderVar
                        + " = "
                        + pbMessageTypeStr
                        + ".newBuilder()");
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            Descriptors.FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            int subUid = varUid.getAndIncrement();
            String elementPbVar = "elementPbVar" + subUid;
            String elementPbTypeStr;
            if (elementFd.isMapField()) {
                elementPbTypeStr = PbCodegenUtils.getTypeStrFromProto(elementFd, false);
            } else {
                elementPbTypeStr =
                        PbCodegenUtils.getTypeStrFromProto(elementFd, elementFd.isRepeated());
            }
            String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);

            appender.appendSegment("if(!" + rowDataVar + ".isNullAt(" + index + ")){");
            appender.appendLine(elementPbTypeStr + " " + elementPbVar);
            String subRowGetCode =
                    PbCodegenUtils.getContainerDataFieldGetterCodePhrase(
                            rowDataVar, index + "", subType);
            PbCodegenSerializer codegen =
                    PbCodegenSerializeFactory.getPbCodegenSer(elementFd, subType);
            String code = codegen.codegen(elementPbVar, subRowGetCode);
            appender.appendSegment(code);
            if (subType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
                appender.appendLine(
                        messageBuilderVar
                                + ".addAll"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            } else if (subType.getTypeRoot() == LogicalTypeRoot.MAP) {
                appender.appendLine(
                        messageBuilderVar
                                + ".putAll"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            } else {
                appender.appendLine(
                        messageBuilderVar
                                + ".set"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            }
            appender.appendSegment("}");
            index += 1;
        }
        appender.appendLine(returnVarName + " = " + messageBuilderVar + ".build()");
        return appender.code();
    }
}
