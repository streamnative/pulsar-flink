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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Range;

import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Collections;

/**
 * The SplitDivisionStrategy represent no range splitting is required.
 */
public class NoSplitDivisionStrategy implements SplitDivisionStrategy {
    public static final NoSplitDivisionStrategy INSTANCE = new NoSplitDivisionStrategy();

    private NoSplitDivisionStrategy() {

    }

    @Override
    public Collection<Range> getRanges(String topic, PulsarAdmin pulsarAdmin, SplitEnumeratorContext<PulsarPartitionSplit> context) throws PulsarAdminException {
        return Collections.singletonList(
                Range.of(SerializableRange.fullRangeStart, SerializableRange.fullRangeEnd)
        );
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }

}
