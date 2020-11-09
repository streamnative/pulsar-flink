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

package org.apache.flink.connector.pulsar.source;

import org.apache.pulsar.client.api.Range;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A class to represent plusar partition.
 */
public class Partition implements Serializable {
    public static final Collection<Range> AUTO_KEY_RANGE = Collections.emptyList();

    private final String topic;
    private transient Collection<Range> keyRanges;
    /**
     * Used for comparison, as Range does not implement equals.
     */
    private final int[] rawKeys;

    public Partition(String topic, Collection<Range> keyRanges) {
        this.topic = topic;
        this.keyRanges = keyRanges;
        rawKeys = keyRanges.stream().flatMapToInt(range -> IntStream.of(range.getStart(), range.getEnd())).toArray();
    }

    public Collection<Range> getKeyRanges() {
        return keyRanges;
    }

    public boolean hasStickyKeys() {
        return keyRanges != AUTO_KEY_RANGE;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Partition partition = (Partition) o;
        return topic.equals(partition.topic) && Arrays.equals(rawKeys, partition.rawKeys);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic);
        result = 31 * result + Arrays.hashCode(rawKeys);
        return result;
    }

    @Override
    public String toString() {
        return topic + keyRanges;
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (rawKeys.length == 0) {
            keyRanges = AUTO_KEY_RANGE;
        } else {
            keyRanges = new ArrayList<>(rawKeys.length / 2);
            for (int index = 0; index + 1 < rawKeys.length; index += 2) {
                keyRanges.add(Range.of(rawKeys[index], rawKeys[index + 1]));
            }
        }
    }
}
