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

import org.apache.flink.util.Preconditions;

import lombok.Getter;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonParser;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Various options for decoding a JSON record.
 */
@Getter
public class JSONOptions implements Serializable {

    private final boolean primitivesAsString;
    private final boolean prefersDecimal;
    private final boolean allowComments;
    private final boolean allowUnquotedFieldNames;
    private final boolean allowSingleQuotes;
    private final boolean allowNumericLeadingZeros;
    private final boolean allowNonNumericNumbers;
    private final boolean allowBackslashEscapingAnyCharacter;
    private final boolean allowUnquotedControlChars;
    private final ParseMode parseMode;
    private final String columnNameOfCorruptRecord;
    private final boolean dropFieldIfAllNull;
    private final ConcurrentHashMap<String, TimeZone> computedTimeZones;
    private final Function<String, TimeZone> computeTimeZone;
    private final TimeZone timeZone;
    private final FastDateFormat dateFormat;
    private final FastDateFormat timestampFormat;
    protected final boolean multiLine;
    protected final String lineSeparator;
    private final String encoding;
    private final byte[] lineSeparatorInRead;
    private final String lineSeparatorInWrite;

    private final transient Map<String, String> parameters;
    private final String defaultTimeZoneId;
    private final String defaultColumnNameOfCorruptRecord;

    public JSONOptions(
            Map<String, String> parameters,
            String defaultTimeZoneId,
            String defaultColumnNameOfCorruptRecord) {
        this.parameters = parameters;
        this.defaultTimeZoneId = defaultTimeZoneId;
        this.defaultColumnNameOfCorruptRecord = defaultColumnNameOfCorruptRecord;

        this.primitivesAsString =
                Boolean.valueOf(parameters.getOrDefault("primitivesAsString", "false"));
        this.prefersDecimal =
                Boolean.valueOf(parameters.getOrDefault("prefersDecimal", "false"));
        this.allowComments =
                Boolean.valueOf(parameters.getOrDefault("allowComments", "false"));
        this.allowUnquotedFieldNames =
                Boolean.valueOf(parameters.getOrDefault("allowUnquotedFieldNames", "false"));
        this.allowSingleQuotes =
                Boolean.valueOf(parameters.getOrDefault("allowSingleQuotes", "false"));
        this.allowNumericLeadingZeros =
                Boolean.valueOf(parameters.getOrDefault("allowNumericLeadingZeros", "false"));
        this.allowNonNumericNumbers =
                Boolean.valueOf(parameters.getOrDefault("allowNonNumericNumbers", "false"));
        this.allowBackslashEscapingAnyCharacter =
                Boolean.valueOf(parameters.getOrDefault("allowBackslashEscapingAnyCharacter", "false"));
        this.allowUnquotedControlChars =
                Boolean.valueOf(parameters.getOrDefault("allowUnquotedControlChars", "false"));
        this.columnNameOfCorruptRecord =
                parameters.getOrDefault("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord);
        this.dropFieldIfAllNull =
                Boolean.valueOf(parameters.getOrDefault("dropFieldIfAllNull", "false"));
        this.parseMode =
                ParseMode.get(parameters.getOrDefault("mode", "PERMISSIVE"));
        this.multiLine =
                Boolean.valueOf(parameters.getOrDefault("multiLine", "false"));

        if (parameters.containsKey("encoding") || parameters.containsKey("charset")) {
            String enc = parameters.getOrDefault("encoding", parameters.getOrDefault("charset", null));
            if (enc != null) {
                this.encoding = checkEncoding(enc);
            } else {
                this.encoding = null;
            }
        } else {
            this.encoding = null;
        }

        if (parameters.containsKey("lineSep")) {
            String lineSep = parameters.get("lineSep");
            Preconditions.checkArgument(!lineSep.isEmpty());
            this.lineSeparator = lineSep;
            try {
                this.lineSeparatorInRead = encoding == null ? lineSep.getBytes("UTF-8") : lineSep.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            this.lineSeparatorInWrite = lineSep;
        } else {
            this.lineSeparator = null;
            this.lineSeparatorInRead = null;
            this.lineSeparatorInWrite = "\n";
        }

        this.computedTimeZones = new ConcurrentHashMap<>();
        this.computeTimeZone = timezoneId -> TimeZone.getTimeZone(timezoneId);

        this.timeZone = getTimeZone(parameters.getOrDefault("timezone", defaultTimeZoneId));

        this.dateFormat = FastDateFormat.getInstance(parameters.getOrDefault("dateFormat", "yyyy-MM-dd"), Locale.US);
        this.timestampFormat = FastDateFormat.getInstance(
                parameters.getOrDefault("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS"), timeZone, Locale.US);

    }

    protected String checkEncoding(String enc) {
        return enc;
    }

    private TimeZone getTimeZone(String timeZoneeId) {
        return computedTimeZones.computeIfAbsent(timeZoneeId, computeTimeZone);
    }

    /** Sets config options on a Jackson [[JsonFactory]]. */
    public void setJacksonOptions(JsonFactory factory) {
        factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments);
        factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames);
        factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes);
        factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros);
        factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers);
        factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, allowBackslashEscapingAnyCharacter);
        factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, allowUnquotedControlChars);
    }
}
