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
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class PbCodegenSimpleSerializer implements PbCodegenSerializer {
    private Descriptors.FieldDescriptor fd;
    private LogicalType type;

    public PbCodegenSimpleSerializer(Descriptors.FieldDescriptor fd, LogicalType type) {
        this.fd = fd;
        this.type = type;
    }

    /**
     * @param returnPbVar
     * @param dataGetStr  dataGetStr is the expression string from row data, the real value of
     *                    rowFieldGetStr may be null, String, int, long, double, float, boolean, byte[]
     * @return
     */
    @Override
    public String codegen(String returnPbVar, String dataGetStr) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return returnPbVar + " = " + dataGetStr + ";";
            case VARCHAR:
            case CHAR:
                PbCodegenAppender appender = new PbCodegenAppender();
                int uid = PbCodegenVarId.getInstance().getAndIncrement();
                String fromVar = "fromVar" + uid;
                appender.appendLine("String " + fromVar);
                appender.appendLine(fromVar + " = " + dataGetStr + ".toString()");
                if (fd.getJavaType() == JavaType.ENUM) {
                    String enumValueDescVar = "enumValueDesc" + uid;
                    String enumTypeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
                    appender.appendLine(
                            "Descriptors.EnumValueDescriptor "
                                    + enumValueDescVar
                                    + "="
                                    + enumTypeStr
                                    + ".getDescriptor().findValueByName("
                                    + fromVar
                                    + ")");
                    appender.appendSegment("if(null == " + enumValueDescVar + "){");
                    appender.appendLine(returnPbVar + " = " + enumTypeStr + ".values()[0]");
                    appender.appendSegment("}");
                    appender.appendSegment("else{");
                    appender.appendLine(
                            returnPbVar
                                    + " = "
                                    + enumTypeStr
                                    + ".valueOf("
                                    + enumValueDescVar
                                    + ")");
                    appender.appendLine("}");
                } else {
                    appender.appendLine(returnPbVar + " = " + fromVar);
                }
                return appender.code();
            case VARBINARY:
            case BINARY:
                return returnPbVar + " = ByteString.copyFrom(" + dataGetStr + ");";
            default:
                throw new IllegalArgumentException("Unsupported data type in schema: " + type);
        }
    }
}
