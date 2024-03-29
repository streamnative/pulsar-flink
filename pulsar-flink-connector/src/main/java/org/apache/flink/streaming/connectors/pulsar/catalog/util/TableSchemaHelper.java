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

package org.apache.flink.streaming.connectors.pulsar.catalog.util;

import org.apache.flink.table.runtime.util.JsonUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Util to convert between flink table map representation and pulsar SchemaInfo. */
public final class TableSchemaHelper {

    private TableSchemaHelper() {}

    public static SchemaInfo generateSchemaInfo(Map<String, String> properties)
            throws JsonProcessingException {
        return SchemaInfoImpl.builder()
                .name("flink_table_schema")
                .type(SchemaType.BYTES)
                .schema(JsonUtils.MAPPER.writeValueAsBytes(properties))
                .build();
    }

    public static Map<String, String> generateTableProperties(SchemaInfo schemaInfo)
            throws IOException {
        return JsonUtils.MAPPER.readValue(
                schemaInfo.getSchema(), new TypeReference<HashMap<String, String>>() {});
    }
}
