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
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowToProtoConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoToRowConverter.class);
    private ScriptEvaluator se;

    public RowToProtoConverter(String messageClassName, RowType rowType) throws PbCodegenException {
        try {
            Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(messageClassName);
            se = new ScriptEvaluator();
            se.setParameters(new String[]{"rowData"}, new Class[]{RowData.class});
            se.setReturnType(AbstractMessage.class);
            se.setDefaultImports(
                    // pb
                    AbstractMessage.class.getName(),
                    Descriptors.class.getName(),
                    // flink row
                    RowData.class.getName(),
                    ArrayData.class.getName(),
                    StringData.class.getName(),
                    ByteString.class.getName(),
                    // java common
                    List.class.getName(),
                    ArrayList.class.getName(),
                    Map.class.getName(),
                    HashMap.class.getName());

            StringBuilder sb = new StringBuilder();
            sb.append("AbstractMessage message = null;\n");
            PbCodegenSerializer codegenSer =
                    PbCodegenSerializeFactory.getPbCodegenTopRowSer(descriptor, rowType);
            String genCode = codegenSer.codegen("message", "rowData");
            sb.append(genCode);
            sb.append("return message;\n");
            String code = sb.toString();

            String printCode = PbCodegenAppender.printWithLineNumber(code);
            LOG.debug("Protobuf decode codegen: \n" + printCode);

            se.cook(code);
        } catch (Exception ex) {
            throw new PbCodegenException(ex);
        }
    }

    public byte[] convertRowToProtoBinary(RowData rowData) throws Exception {
        AbstractMessage message = (AbstractMessage) se.evaluate(new Object[]{rowData});
        return message.toByteArray();
    }
}
