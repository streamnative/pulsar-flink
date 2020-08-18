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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * JSON record parser.
 */
@Slf4j
public class JacksonRecordParser {

    private final DataType schema;

    private final JSONOptions options;

    private final JsonFactory factory;

    private final BiFunction<JsonParser, Row, Row> rootConverter;

    public JacksonRecordParser(DataType schema, JSONOptions options) {
        assert schema instanceof FieldsDataType;
        this.schema = schema;
        this.options = options;

        this.rootConverter = makeStructRootConverter((FieldsDataType) schema);

        this.factory = new JsonFactory();
        options.setJacksonOptions(factory);
    }

    private BiFunction<JsonParser, Row, Row> makeStructRootConverter(FieldsDataType st) {
        List<String> fieldNames = ((RowType) st.getLogicalType()).getFieldNames();
        List<Function<JsonParser, Object>> fieldConverters = new ArrayList<Function<JsonParser, Object>>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            DataType type = st.getFieldDataTypes().get(fieldName);
            fieldConverters.add(makeConverter(type));
        }
        return (parser, row) -> {
            try {
                parseJsonToken(parser, st, new PartialFunc() {
                    @Override
                    public boolean isDefinedAt(JsonToken token) {
                        return token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY;
                    }

                    @Override
                    public Object apply(JsonToken token) {
                        if (token == JsonToken.START_OBJECT) {
                            try {
                                return convertObject(parser, st, fieldConverters, row);
                            } catch (IOException e) {
                                suroundWithRuntimeE(e);
                            }
                        } else {
                            throw new IllegalStateException("Message should be a single JSON object");
                        }
                        return null;
                    }
                });
            } catch (IOException e) {
                suroundWithRuntimeE(e);
            }
            return null;
        };
    }

    public Row parse(String record, BiFunction<JsonFactory, String, JsonParser> createParser, Row row) throws BadRecordException {
        try (JsonParser parser = createParser.apply(factory, record)) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                return new Row(0);
            } else {
                Row result = rootConverter.apply(parser, row);
                if (result == null) {
                    throw new RuntimeException("Root converter returned null");
                } else {
                    return result;
                }
            }
        } catch (Exception e) {
            throw new BadRecordException(record, e);
        }
    }

    private Row convertObject(JsonParser parser, FieldsDataType fdt, List<Function<JsonParser, Object>> fieldConverters, Row row) throws IOException {

        RowType rowType = (RowType) fdt.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();
        while (nextUntil(parser, JsonToken.END_OBJECT)) {
            int index = fieldNames.indexOf(parser.getCurrentName());
            if (index == -1) {
                parser.skipChildren();
            } else {
                row.setField(index, fieldConverters.get(index).apply(parser));
            }
        }
        return row;
    }

    private void suroundWithRuntimeE(Exception e) {
        log.error("Failed to parse json due to {}", ExceptionUtils.stringifyException(e));
        throw new RuntimeException(e);
    }

    private boolean nextUntil(JsonParser parser, JsonToken stopOn) throws IOException {
        JsonToken token = parser.nextToken();
        if (token == null) {
            return false;
        } else {
            return token != stopOn;
        }
    }

    private Function<JsonParser, Object> makeConverter(DataType dataType) {
        LogicalTypeRoot tpe = dataType.getLogicalType().getTypeRoot();

        switch (tpe) {
            case BOOLEAN:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                return token == JsonToken.VALUE_TRUE;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };
            case TINYINT:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getByteValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case SMALLINT:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getShortValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case INTEGER:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getIntValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case BIGINT:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getLongValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case FLOAT:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT
                                        || token == JsonToken.VALUE_NUMBER_FLOAT
                                        || token == JsonToken.VALUE_STRING;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    if (token == JsonToken.VALUE_NUMBER_INT
                                            || token == JsonToken.VALUE_NUMBER_FLOAT) {
                                        return parser.getFloatValue();
                                    } else {
                                        String txt = parser.getText();
                                        if (txt.equals("NaN")) {
                                            return Float.NaN;
                                        } else if (txt.equals("Infinity")) {
                                            return Float.POSITIVE_INFINITY;
                                        } else if (txt.equals("-Infinity")) {
                                            return Float.NEGATIVE_INFINITY;
                                        } else {
                                            throw new RuntimeException("Cannot parse " + txt + " as Float");
                                        }
                                    }
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case DOUBLE:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT
                                        || token == JsonToken.VALUE_NUMBER_FLOAT
                                        || token == JsonToken.VALUE_STRING;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    if (token == JsonToken.VALUE_NUMBER_INT
                                            || token == JsonToken.VALUE_NUMBER_FLOAT) {
                                        return parser.getDoubleValue();
                                    } else {
                                        String txt = parser.getText();
                                        if (txt.equals("NaN")) {
                                            return Float.NaN;
                                        } else if (txt.equals("Infinity")) {
                                            return Float.POSITIVE_INFINITY;
                                        } else if (txt.equals("-Infinity")) {
                                            return Float.NEGATIVE_INFINITY;
                                        } else {
                                            throw new RuntimeException("Cannot parse " + txt + " as Float");
                                        }
                                    }
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case VARCHAR:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return true;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    if (token == JsonToken.VALUE_STRING) {
                                        return parser.getText();
                                    } else {
                                        ByteArrayOutputStream writer = new ByteArrayOutputStream();
                                        try (JsonGenerator generator = factory.createGenerator(writer, JsonEncoding.UTF8)) {
                                            generator.copyCurrentStructure(parser);
                                        }
                                        return new String(writer.toByteArray(), StandardCharsets.UTF_8);
                                    }

                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_STRING;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    if (token == JsonToken.VALUE_STRING) {
                                        String v = parser.getText();
                                        long t = options.getTimestampFormat().parse(v).getTime() * 1000L;
                                        return DateTimeUtils.toJavaTimestamp(t);
                                    } else {
                                        return DateTimeUtils.toJavaTimestamp(parser.getLongValue() * 1000000L);
                                    }
                                } catch (IOException | ParseException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case DATE:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_STRING;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    String v = parser.getText();
                                    int t = DateTimeUtils.millisToDays(options.getDateFormat().parse(v).getTime());
                                    return DateTimeUtils.toJavaDate(t);

                                } catch (IOException | ParseException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case VARBINARY:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_STRING;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getBinaryValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case DECIMAL:
                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    return parser.getDecimalValue();
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            case ROW:
                RowType rowType = (RowType) dataType.getLogicalType();
                Map<String, DataType> types = ((FieldsDataType) dataType).getFieldDataTypes();
                List<String> fieldNames = rowType.getFieldNames();
                List<Function<JsonParser, Object>> fieldConverters = new ArrayList<Function<JsonParser, Object>>();
                for (int i = 0; i < fieldNames.size(); i++) {
                    fieldConverters.add(makeConverter(types.get(fieldNames.get(i))));
                }

                return parser -> {
                    try {
                        return parseJsonToken(parser, dataType, new PartialFunc() {
                            @Override
                            public boolean isDefinedAt(JsonToken token) {
                                return token == JsonToken.START_OBJECT;
                            }

                            @Override
                            public Object apply(JsonToken token) {
                                try {
                                    Row record = new Row(rowType.getFieldCount());
                                    return convertObject(parser, (FieldsDataType) dataType, fieldConverters, record);
                                } catch (IOException e) {
                                    suroundWithRuntimeE(e);
                                }
                                return null;
                            }
                        });
                    } catch (IOException e) {
                        suroundWithRuntimeE(e);
                    }
                    return null;
                };

            default:
                throw new RuntimeException(String.format(
                        "Failed to parse a value for data type %s (current: %s).", dataType.toString(), tpe.toString()));
        }
    }

    /**
     * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying
     * to parse the JSON token using given function `f`. If the `f` failed to parse and convert the
     * token, call `failedConversion` to handle the token.
     */
    public Object parseJsonToken(JsonParser parser, DataType dataType, PartialFunc f) throws IOException {
        while (true) {
            JsonToken currentToken = parser.getCurrentToken();
            if (!JsonToken.FIELD_NAME.equals(currentToken)) {
                Object result;
                if (currentToken == null || JsonToken.VALUE_NULL.equals(currentToken)) {
                    result = null;
                } else {
                    result = f.applyOrElse(currentToken, parser, dataType);
                }

                return result;
            }

            parser.nextToken();
        }
    }

    interface PartialFunc {
        boolean isDefinedAt(JsonToken token);

        Object apply(JsonToken token);

        default Object applyOrElse(JsonToken token, JsonParser parser, DataType dataType) throws IOException {
            if (isDefinedAt(token)) {
                return apply(token);
            } else {
                if (token == JsonToken.VALUE_STRING && parser.getTextLength() < 1) {
                    // If conversion is failed, this produces `null` rather than throwing exception.
                    // This will protect the mismatch of types.
                    return null;
                } else {
                    throw new RuntimeException(String.format(
                            "Failed to parse a value for data type %s (current token: %s).", dataType.toString(), token.toString()));
                }
            }
        }
    }

    interface BiFunctionWithException<T, U, R> {
        R apply(T t, U u) throws BadRecordException;
    }

    static class FailureSafeRecordParser {
        private final BiFunctionWithException<String, Row, Row> rawParser;
        private final ParseMode mode;
        private final FieldsDataType schema;

        FailureSafeRecordParser(BiFunctionWithException<String, Row, Row> rawParser, ParseMode mode, FieldsDataType schema) {
            this.rawParser = rawParser;
            this.mode = mode;
            this.schema = schema;
        }

        Row parse(String input, Row row) {
            try {
                return rawParser.apply(input, row);
            } catch (BadRecordException e) {
                switch (mode) {
                    case PERMISSIVE:
                        return row;
                    case DROPMALFORMED:
                        return null;
                    case FAILFAST:
                        throw new RuntimeException("Malformed records are detected in record parsing", e);
                }
            }
            return null;
        }
    }

    static class BadRecordException extends Exception {
        public String record;

        public BadRecordException(String record, Exception e) {
            super(e);
            this.record = record;
        }
    }
}
