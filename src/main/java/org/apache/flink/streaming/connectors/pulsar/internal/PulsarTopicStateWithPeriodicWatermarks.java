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
package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A special version of the per-pulsar-partition-state that additionally holds
 * a periodic watermark generator (and timestamp extractor) per partition.
 *
 */
public class PulsarTopicStateWithPeriodicWatermarks<T> extends PulsarTopicState {

    private final AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks;

    private volatile long partitionWatermark;

    public PulsarTopicStateWithPeriodicWatermarks(
            String topic,
            AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks) {
        super(topic);
        this.timestampsAndWatermarks = timestampsAndWatermarks;
        this.partitionWatermark = Long.MIN_VALUE;
    }

    public long getTimestampForRecord(T record, long timestamp) {
        return timestampsAndWatermarks.extractTimestamp(record, timestamp);
    }

    public long getCurrentWatermarkTimestamp() {
        Watermark wm = timestampsAndWatermarks.getCurrentWatermark();
        if (wm != null) {
            partitionWatermark = Math.max(partitionWatermark, wm.getTimestamp());
        }
        return partitionWatermark;
    }

    @Override
    public String toString() {
        return String.format("%s: %s, offset = %s, watermark = %d",
            getClass().getName(),
            getTopic(),
            getOffset(),
            partitionWatermark);
    }
}
