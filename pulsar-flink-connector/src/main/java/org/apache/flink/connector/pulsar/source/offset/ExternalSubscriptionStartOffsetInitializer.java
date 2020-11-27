/**
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

package org.apache.flink.connector.pulsar.source.offset;

import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;

import org.apache.pulsar.client.api.MessageId;

/**
 * An implementation of {@link StartOffsetInitializer} for external subscription.
 */
public class ExternalSubscriptionStartOffsetInitializer implements StartOffsetInitializer {
    private final MessageId defaultOffset;
    private final String subscriptionName;

    public ExternalSubscriptionStartOffsetInitializer(String subscriptionName, MessageId defaultOffset) {
        this.subscriptionName = subscriptionName;
        this.defaultOffset = defaultOffset;
    }

}
