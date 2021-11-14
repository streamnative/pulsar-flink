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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.impl.schema.JSONSchema;

/**
 * Read JSON encoded data from Pulsar.
 *
 * @param <T> the type of record class.
 */
public class JsonSer<T> implements SerializationSchema<T> {

    private final Class<T> recordClazz;

    private transient JSONSchema<T> pulsarSchema;

    private JsonSer(Class<T> recordClazz) {
        Preconditions.checkNotNull(recordClazz, "JSON record class must not be null");
        this.recordClazz = recordClazz;
    }

    public static <T> JsonSer<T> of(Class<T> recordClazz) {
        return new JsonSer<>(recordClazz);
    }

    @Override
    public byte[] serialize(T message) {
        checkPulsarJsonSchemaInitialized();
        return pulsarSchema.encode(message);
    }

    private void checkPulsarJsonSchemaInitialized() {
        if (pulsarSchema != null) {
            return;
        }

        this.pulsarSchema = JSONSchema.of(recordClazz);
    }
}
