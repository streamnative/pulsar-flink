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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.protobuf.PbFormatOptions;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import com.google.protobuf.GeneratedMessageV3;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.pulsar.client.impl.schema.SchemaUtils.convertKeyValueSchemaInfoDataToString;
import static org.apache.pulsar.shade.com.google.common.base.Preconditions.checkNotNull;

/** Various utilities to working with Pulsar Schema and Flink type system. */
@Slf4j
public class SchemaUtils {

    public static void uploadPulsarSchema(PulsarAdmin admin, String topic, SchemaInfo schemaInfo) {
        checkNotNull(schemaInfo);

        SchemaInfo existingSchema;
        try {
            existingSchema = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
        } catch (PulsarAdminException pae) {
            if (pae.getStatusCode() == 404) {
                existingSchema = null;
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Failed to get schema information for %s",
                                TopicName.get(topic).toString()),
                        pae);
            }
        } catch (Throwable e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to get schema information for %s",
                            TopicName.get(topic).toString()),
                    e);
        }

        if (existingSchema == null) {
            PostSchemaPayload pl = new PostSchemaPayload();
            pl.setType(schemaInfo.getType().name());
            pl.setSchema(getSchemaString(schemaInfo));
            pl.setProperties(schemaInfo.getProperties());

            try {
                admin.schemas().createSchema(TopicName.get(topic).toString(), pl);
            } catch (PulsarAdminException pae) {
                if (pae.getStatusCode() == 404) {
                    throw new IllegalStateException(
                            String.format(
                                    "Create schema for %s get 404",
                                    TopicName.get(topic).toString()),
                            pae);
                } else {
                    throw new IllegalStateException(
                            String.format(
                                    "Failed to create schema information for %s",
                                    TopicName.get(topic).toString()),
                            pae);
                }
            } catch (Throwable e) {
                throw new IllegalStateException(
                        String.format(
                                "Failed to create schema information for %s",
                                TopicName.get(topic).toString()),
                        e);
            }
        } else if (!schemaEqualsIgnoreProperties(schemaInfo, existingSchema)
                && !compatibleSchema(existingSchema, schemaInfo)) {
            throw new IllegalStateException("Writing to a topic which have incompatible schema");
        }
    }

    public static void deletePulsarSchema(PulsarAdmin admin, String topic) {
        try {
            admin.schemas().deleteSchema(topic);
        } catch (PulsarAdminException e) {
            // TODO: refine error handling logic
            e.printStackTrace();
        }
    }

    private static boolean schemaEqualsIgnoreProperties(
            SchemaInfo schemaInfo, SchemaInfo existingSchema) {
        return existingSchema.getType().equals(schemaInfo.getType())
                && Arrays.equals(existingSchema.getSchema(), schemaInfo.getSchema());
    }

    private static String getSchemaString(SchemaInfo schemaInfo) {
        final byte[] schemaData = schemaInfo.getSchema();
        if (null == schemaData) {
            return null;
        }
        if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
            try {
                return convertKeyValueSchemaInfoDataToString(
                        DefaultImplementation.getDefaultImplementation()
                                .decodeKeyValueSchemaInfo(schemaInfo));
            } catch (IOException e) {
                throw new RuntimeException("failed to convert KeyValueSchema into string");
            }
        }
        return new String(schemaData, StandardCharsets.UTF_8);
    }

    public static boolean compatibleSchema(SchemaInfo s1, SchemaInfo s2) {
        if (s1.getType() == SchemaType.NONE && s2.getType() == SchemaType.BYTES) {
            return true;
        } else {
            return s1.getType() == SchemaType.BYTES && s2.getType() == SchemaType.NONE;
        }
    }

    static GenericSchema<GenericRecord> avroSchema2PulsarSchema(Schema avroSchema) {
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfoImpl si = new SchemaInfoImpl();
        si.setName("Avro");
        si.setSchema(schemaBytes);
        si.setType(SchemaType.AVRO);
        return org.apache.pulsar.client.api.Schema.generic(si);
    }

    public static SchemaInfoImpl emptySchemaInfo() {
        return SchemaInfoImpl.builder()
                .name("empty")
                .type(SchemaType.NONE)
                .schema(new byte[0])
                .build();
    }

    private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

    private static int[] minBytesForPrecision = new int[39];

    static {
        for (int i = 0; i < minBytesForPrecision.length; i++) {
            minBytesForPrecision[i] = computeMinBytesForPrecision(i);
        }
    }

    private static int computeMinBytesForPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public static SchemaInfo buildRowSchema(DataType dataType, RecordSchemaType recordSchemaType) {
        org.apache.avro.Schema avroSchema =
                AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
        SchemaInfoImpl si = new SchemaInfoImpl();
        si.setSchema(schemaBytes);
        switch (recordSchemaType) {
            case AVRO:
                si.setName("Avro");
                si.setType(SchemaType.AVRO);
                break;
            case JSON:
                si.setName("Json");
                si.setType(SchemaType.JSON);
                break;
            case ATOMIC:
                try {
                    FieldsDataType fieldType = (FieldsDataType) dataType;
                    RowType rowType = (RowType) fieldType.getLogicalType();
                    DataType atomicType =
                            TypeConversions.fromLogicalToDataType(rowType.getTypeAt(0));
                    return SimpleSchemaTranslator.atomicType2PulsarSchema(atomicType)
                            .getSchemaInfo();
                } catch (IncompatibleSchemaException e) {
                    throw new IllegalStateException(e);
                }
            default:
                throw new IllegalStateException(
                        "for now we just support json、avro、atomic format for rowData");
        }
        return si;
    }

    public static <T> org.apache.pulsar.client.api.Schema<T> buildSchemaForRecordClazz(
            Class<T> recordClazz, RecordSchemaType recordSchemaType) {
        if (recordSchemaType == null) {
            return org.apache.pulsar.client.api.Schema.AVRO(recordClazz);
        }
        switch (recordSchemaType) {
            case AVRO:
                return org.apache.pulsar.client.api.Schema.AVRO(recordClazz);
            case JSON:
                return org.apache.pulsar.client.api.Schema.JSON(recordClazz);
            case PROTOBUF:
                @SuppressWarnings("unchecked")
                final org.apache.pulsar.client.api.Schema<T> tSchema =
                        (org.apache.pulsar.client.api.Schema<T>)
                                org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(
                                        convertProtobuf(recordClazz));
                return tSchema;
            default:
                throw new IllegalArgumentException("not support schema type " + recordSchemaType);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends GeneratedMessageV3> Class<T> convertProtobuf(Class recordClazz) {
        if (!GeneratedMessageV3.class.isAssignableFrom(recordClazz)) {
            throw new IllegalArgumentException(
                    "Message classes must extend GeneratedMessageV3" + recordClazz);
        }
        return recordClazz;
    }

    public static SchemaInfo tableSchemaToSchemaInfo(
            String format, DataType dataType, Configuration configuration)
            throws IncompatibleSchemaException {
        switch (StringUtils.lowerCase(format)) {
            case "raw":
                return getSchemaInfo(SchemaType.BYTES, dataType);
            case "json":
                return getSchemaInfo(SchemaType.JSON, dataType);
            case "avro":
                return getSchemaInfo(SchemaType.AVRO, dataType);
            case "protobuf":
                final String messageClassName =
                        configuration.get(PbFormatOptions.MESSAGE_CLASS_NAME);
                return getProtobufSchemaInfo(messageClassName, SchemaUtils.class.getClassLoader());
            case "atomic":
                org.apache.pulsar.client.api.Schema pulsarSchema =
                        SimpleSchemaTranslator.sqlType2PulsarSchema(dataType.getChildren().get(0));
                return pulsarSchema.getSchemaInfo();
            default:
                throw new UnsupportedOperationException(
                        "Generic schema is not supported on format '"
                                + format
                                + "' schema type '"
                                + dataType
                                + "'");
        }
    }

    private static <T extends GeneratedMessageV3> SchemaInfo getProtobufSchemaInfo(
            String messageClassName, ClassLoader classLoader) {
        try {
            final org.apache.pulsar.client.api.Schema<T> tSchema =
                    org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(
                            convertProtobuf(classLoader.loadClass(messageClassName)));
            return tSchema.getSchemaInfo();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("not load Protobuf class: " + messageClassName, e);
        }
    }

    public static SchemaInfoImpl getSchemaInfo(SchemaType type, DataType dataType) {
        byte[] schemaBytes = getAvroSchema(dataType).toString().getBytes(StandardCharsets.UTF_8);
        return SchemaInfoImpl.builder()
                .name("Record")
                .schema(schemaBytes)
                .type(type)
                .properties(Collections.emptyMap())
                .build();
    }

    public static org.apache.avro.Schema getAvroSchema(DataType dataType) {
        org.apache.avro.Schema schema =
                AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        if (schema.isNullable()) {
            schema =
                    schema.getTypes().stream()
                            .filter(s -> s.getType() == RECORD)
                            .findAny()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "not support DataType: "
                                                            + dataType.toString()));
        }
        return schema;
    }
}
