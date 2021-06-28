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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * thread safe {@link ThreadSafeDeserializationSchema} test.
 */
public class ThreadSafeDeserializationSchemaTest {

    @Test
    public void deserialize() throws InterruptedException {
        NoThreadSafeDeserializationSchema noThreadSafeDeserializationSchema = new NoThreadSafeDeserializationSchema();
        DeserializationSchema deserializationSchema = ThreadSafeDeserializationSchema.of(noThreadSafeDeserializationSchema);
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        deserializationSchema.deserialize(null);
                    }
                } catch (IOException e) {
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }
        Assert.assertEquals(noThreadSafeDeserializationSchema.getCount(), 1000);
    }

    @Test
    public void isEndOfStream() {
    }

    @Test
    public void getProducedType() {
    }

    class NoThreadSafeDeserializationSchema implements DeserializationSchema {

        int count = 0;
        int tmpCount = 0;

        public int getCount() {
            return count;
        }

        @Override
        public Object deserialize(byte[] bytes) throws IOException {
            tmpCount = count;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            tmpCount++;
            count = tmpCount;
            return null;
        }

        @Override
        public boolean isEndOfStream(Object o) {
            return false;
        }

        @Override
        public TypeInformation getProducedType() {
            return null;
        }
    }
}
