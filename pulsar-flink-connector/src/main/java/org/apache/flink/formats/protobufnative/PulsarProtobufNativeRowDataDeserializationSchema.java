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

package org.apache.flink.formats.protobufnative;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.IOException;

@Slf4j
public class PulsarProtobufNativeRowDataDeserializationSchema implements DeserializationSchema<RowData>{

    private String topic;
    private String adminUrl;
    private String serviceUrl;
    private RowType rowType;
    private TypeInformation<RowData> rowDataTypeInfo;

    //TODO maybe add nested ProtobufRowDataDeserializationSchema structured by Descriptor?
    private transient Descriptors.Descriptor descriptor;
    /** Runtime instance that performs the actual work. */
    private transient PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter runtimeConverter;

    //TOOD add class-based-schema support?
    public PulsarProtobufNativeRowDataDeserializationSchema(String topic, String adminUrl, String serviceUrl, RowType rowType, TypeInformation<RowData> rowDataTypeInfo) {
        this.topic = topic;
        this.adminUrl = adminUrl;
        this.serviceUrl = serviceUrl;
        this.rowDataTypeInfo = rowDataTypeInfo;
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        PulsarAdmin pulsarAdmin;
        try {
            ClientConfigurationData clientConf = new ClientConfigurationData();
            clientConf.setServiceUrl(serviceUrl);
            pulsarAdmin = PulsarClientUtils.newAdminFromConf(adminUrl, clientConf);
        } catch (PulsarClientException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            //TODO add cache ?
            SchemaInfo schemaInfo = pulsarAdmin.schemas().getSchemaInfo(TopicName.get(topic).toString());
            this.descriptor = ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schemaInfo)).getProtobufNativeSchema();
        } catch (PulsarAdminException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
