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

package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * a messageRouter that route message by key hash.
 */
public class KeyHashMessageRouterImpl implements MessageRouter {

    public static final KeyHashMessageRouterImpl INSTANCE = new KeyHashMessageRouterImpl();

    private KeyHashMessageRouterImpl() {

    }

    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        int hashing = Hashing.murmur3_32().hashBytes(msg.getKeyBytes()).asInt();
        return Math.abs(hashing) % metadata.numPartitions();
    }
}
