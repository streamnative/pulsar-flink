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

import org.apache.pulsar.client.impl.schema.AvroSchema;

/**
 * Read Avro encoded data from Pulsar.
 *
 * @param <T> the type of record class.
 */
public class AvroSer<T> implements SerializationSchema<T> {

    private final Class<T> recordClazz;

    private transient AvroSchema<T> pulsarSchema;

    private AvroSer(Class<T> recordClazz) {
        Preconditions.checkNotNull(recordClazz, "Avro record class must not be null");
        this.recordClazz = recordClazz;
    }

    public static <T> AvroSer<T> of(Class<T> recordClazz) {
        return new AvroSer<>(recordClazz);
    }

    @Override
    public byte[] serialize(T message) {
        checkPulsarAvroSchemaInitialized();
        return pulsarSchema.encode(message);
    }

    private void checkPulsarAvroSchemaInitialized() {
        if (pulsarSchema != null) {
            return;
        }

        this.pulsarSchema = AvroSchema.of(recordClazz);
    }
}
