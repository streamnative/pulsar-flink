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

package org.apache.flink.formats.protobufnative;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.EnumValue;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PulsarProtobufToRowDataConverters {

    protected static final String PROTOBUF_MAP_KEY_NAME = "key";
    protected static final String PROTOBUF_MAP_VALUE_NAME = "value";

    /**
     * Runtime converter that converts protobuf data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface ProtobufToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter createRowConverter(RowType rowType) {
        final PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PulsarProtobufToRowDataConverters::createNullableConverter)
                        .toArray(PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();
        return protobufObject -> {
            DynamicMessage record = (DynamicMessage) protobufObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                String fieldName = rowType.getFieldNames().get(i);
                row.setField(i, fieldConverters[i].convert(record.getField(record.getDescriptorForType().findFieldByName(fieldName))));
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter createNullableConverter(LogicalType type) {
        final PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter converter = createConverter(type);
        return object -> {
            if (object == null) {
                return null;
            }
            return converter.convert(object);
        };
    }

    private static PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN: // boolean
            case INTEGER: // int
            case BIGINT: // long
            case FLOAT:  // float
            case DOUBLE: // double
                return object -> object;
            case CHAR:
            case VARCHAR:
                return object -> {
                    // enum
                    if (object instanceof EnumValue) {
                        return ((EnumValue) object).getName();
                    } else {
                        return StringData.fromString(object.toString());
                    }
                };
            case BINARY:
            case VARBINARY:
                return PulsarProtobufToRowDataConverters::convertToBytes;
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
                return createMapConverter((MapType) type);
            default:
                return object -> {
                    throw new UnsupportedOperationException("Unsupported protobuf field type " + object.getClass() +
                            " convert to" +
                            " flink type: " + type);
                };
        }
    }

    private static PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return protobufObject -> {
            final List<?> list = (List<?>) protobufObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    private static PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter createMapConverter(MapType type) {

        final PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter keyConverter =
                createNullableConverter(type.getKeyType());
        final PulsarProtobufToRowDataConverters.ProtobufToRowDataConverter valueConverter =
                createNullableConverter(type.getValueType());

        return object -> {
            Map<Object, Object> result = new HashMap<>();
            final Collection<DynamicMessage> messages = (Collection<DynamicMessage>) object;
            for (DynamicMessage message : messages) {
                Object key = keyConverter.convert(
                        message.getField(message.getDescriptorForType().findFieldByName(PROTOBUF_MAP_KEY_NAME))
                );
                Object value = valueConverter.convert(
                        message.getField(message.getDescriptorForType().findFieldByName(PROTOBUF_MAP_VALUE_NAME))
                );
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof ByteString) {
            return ((ByteString) object).toByteArray();
        } else {
            return (byte[]) object;
        }
    }
}
