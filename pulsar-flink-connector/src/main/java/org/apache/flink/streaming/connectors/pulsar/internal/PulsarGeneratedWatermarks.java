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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.streaming.connectors.pulsar.PulsarWatermarkStrategy;

/**
 * A watermark strategy based on Pulsar-generated watermarks.
 */
public class PulsarGeneratedWatermarks<T> implements PulsarWatermarkStrategy<T> {
    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new NoWatermarksGenerator<>();
    }
}
