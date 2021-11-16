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

package org.apache.flink.streaming.connectors.pulsar.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Because the Pulsar Source is designed to be multi-threaded, Flink's internal design of the Source
 * is single-threaded, so, DeserializationSchema instances are oriented to single-threaded, and
 * thread safety issues exist when they are accessed by multiple threads at the same time. Cause the
 * message deserialization to fail.
 */
public class ThreadSafeDeserializationSchema<T> implements DeserializationSchema<T> {

    private DeserializationSchema<T> deserializationSchema;

    private ThreadSafeDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public static ThreadSafeDeserializationSchema of(DeserializationSchema deserializationSchema) {
        return deserializationSchema != null
                ? new ThreadSafeDeserializationSchema(deserializationSchema)
                : null;
    }

    @Override
    public synchronized void open(InitializationContext context) throws Exception {
        deserializationSchema.open(context);
    }

    @Override
    public synchronized T deserialize(byte[] bytes) throws IOException {
        return deserializationSchema.deserialize(bytes);
    }

    @Override
    public synchronized void deserialize(byte[] message, Collector<T> out) throws IOException {
        deserializationSchema.deserialize(message, out);
    }

    @Override
    public synchronized boolean isEndOfStream(T object) {
        return deserializationSchema.isEndOfStream(object);
    }

    @Override
    public synchronized TypeInformation getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
