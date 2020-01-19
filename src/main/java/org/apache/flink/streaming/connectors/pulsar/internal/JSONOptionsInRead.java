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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Read JSON Options.
 */
public class JSONOptionsInRead extends JSONOptions {
    private static final List<Charset> blackList = Arrays.asList(
            StandardCharsets.UTF_16,
            Charset.forName("UTF-32"));

    private final transient Map<String, String> parameters;
    private final String defaultTimeZoneId;
    private final String defaultColumnNameOfCorruptRecord;

    public JSONOptionsInRead(Map<String, String> parameters, String defaultTimeZoneId, String defaultColumnNameOfCorruptRecord) {
        super(parameters, defaultTimeZoneId, defaultColumnNameOfCorruptRecord);
        this.parameters = parameters;
        this.defaultTimeZoneId = defaultTimeZoneId;
        this.defaultColumnNameOfCorruptRecord = defaultColumnNameOfCorruptRecord;
    }

    @Override
    protected String checkEncoding(String enc) {
        boolean isBlacklisted = JSONOptionsInRead.blackList.contains(Charset.forName(enc));
        if (!multiLine && isBlacklisted) {
            throw new IllegalArgumentException(String.format(
                    "The %s encoding must not be included in the blacklist when multiLine is disabled",
                    enc));
        }

        boolean isLineSepRequired =
                multiLine || Charset.forName(enc) == StandardCharsets.UTF_8 || lineSeparator != null;

        if (!isLineSepRequired) {
            throw new IllegalArgumentException(String.format(
                    "The lineSep must be specified for the %s encoding", enc));
        }

        return enc;
    }
}
