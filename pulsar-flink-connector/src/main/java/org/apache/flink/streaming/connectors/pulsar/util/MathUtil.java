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

import org.apache.commons.codec.digest.MurmurHash2;

/**
 * Util for math related work.
 */
public class MathUtil {
    public static int toPositive(int number) {
        return number & 2147483647;
    }

    public static int murmur2(byte[] data) {
        return MurmurHash2.hash32(data, data.length);
    }
}