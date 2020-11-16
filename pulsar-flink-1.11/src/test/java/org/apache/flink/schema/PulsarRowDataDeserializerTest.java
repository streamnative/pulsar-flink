package org.apache.flink.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.connectors.pulsar.SchemaData;
import org.apache.flink.streaming.connectors.pulsar.internal.JSONOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.ParseMode;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.ClassDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BOOLEAN_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.BYTES_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.DOUBLE_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.FLOAT_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INTEGER_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_16_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_64_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.INT_8_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.STRING_LIST;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.localDateList;
import static org.apache.flink.streaming.connectors.pulsar.SchemaData.localDateTimeList;
import static org.junit.Assert.*;

public class PulsarRowDataDeserializerTest {


    @Test
    @Ignore
    public void testJson() throws Exception {

        Field[] fields = FlinkTestObject.class.getFields();
        int fieldsLength = fields.length;
        DataTypes.Field[] dataTypeFields = new DataTypes.Field[fieldsLength];
        for (int i = 0; i < fieldsLength; i++) {
            Field field = fields[i];
            final DataType dataType = ClassDataTypeConverter.fromClassToDataType(field.getType());
            dataTypeFields[i] = DataTypes.FIELD(field.getName(), dataType);
        }



        for (SchemaData.FL fa : SchemaData.flList) {
            deserialize(SchemaType.JSON, DataTypes.ROW(dataTypeFields),fa, null);
        }

    }

    @Test
    public void testBoolean() throws Exception {
        for (Boolean data : BOOLEAN_LIST) {
            deserialize(SchemaType.BOOLEAN, DataTypes.BOOLEAN(), data, r -> r.getBoolean(0));
        }
    }

    @Test
    public void testINT32() throws Exception {
        for (Integer data : INTEGER_LIST) {
            deserialize(SchemaType.INT32, DataTypes.INT(), data, r -> r.getInt(0));
        }
    }

    @Test
    public void testINT64() throws Exception {
        for (Long data : INT_64_LIST) {
            deserialize(SchemaType.INT64, DataTypes.BIGINT(), data, r -> r.getLong(0));
        }
    }

    @Test
    public void testString() throws Exception {
        for (String data : STRING_LIST) {
            deserialize(SchemaType.STRING, DataTypes.STRING(), data, r -> r.getString(0).toString());
        }
    }

    @Test
    public void testByte() throws Exception {
        for (Byte data : INT_8_LIST) {
            deserialize(SchemaType.INT8, DataTypes.TINYINT(), data, r -> r.getByte(0));
        }
    }

    @Test
    public void testShort() throws Exception {
        for (Short data : INT_16_LIST) {
            deserialize(SchemaType.INT16, DataTypes.SMALLINT(), data, r -> r.getShort(0));
        }
    }

    @Test
    public void testFloat() throws Exception {
        for (Float data : FLOAT_LIST) {
            deserialize(SchemaType.FLOAT, DataTypes.FLOAT(), data, r -> r.getFloat(0));
        }
    }

    @Test
    public void testDouble() throws Exception {
        for (Double data : DOUBLE_LIST) {
            deserialize(SchemaType.DOUBLE, DataTypes.DOUBLE(), data, r -> r.getDouble(0));
        }
    }

    @Test
    public void testDate() throws Exception {
        for (LocalDate data : localDateList) {
            deserialize(SchemaType.LOCAL_DATE, DataTypes.DATE(), data, r -> LocalDate.ofEpochDay(r.getInt(0)));
        }
    }

    @Test
    public void testTimestampRead() throws Exception {
        for (LocalDateTime data : localDateTimeList) {
            deserialize(SchemaType.LOCAL_DATE_TIME, DataTypes.TIMESTAMP(3), data, r -> r.getTimestamp(0,3).toLocalDateTime());
        }
    }

    @Test
    public void testByteArray() throws Exception {
        for (byte[] data : BYTES_LIST) {
            deserialize(SchemaType.BYTES, DataTypes.BYTES(), data, r -> r.getArray(0).toByteArray());
        }
    }

    public <T> void deserialize(SchemaType schemaType, DataType dataType, T data,
                                Function<RowData, T> getValue) throws Exception {
        final Map<String, String> parameters = Maps.newHashMap();
        parameters.put("mode", ParseMode.PERMISSIVE.getName());
        final JSONOptions parsedOptions = new JSONOptions(parameters, "", "");

        final PulsarRowDataDeserializer deserializer =
                new PulsarRowDataDeserializer(dataType, schemaType, parsedOptions,
                        (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(dataType));
        final Schema pulsarSchema = SimpleSchemaTranslator.sqlType2PulsarSchema(dataType);

        final byte[] encode = pulsarSchema.encode(data);
        final RowData deserialize = deserializer.deserialize(encode);

        if (data.getClass().isArray()) {
            Assert.assertArrayEquals((byte[]) data, (byte[]) getValue.apply(deserialize));
        }else {
            Assert.assertEquals(data, getValue.apply(deserialize));

        }
    }

    /**
     * test pojo.
     */
    @Data
    public static class FlinkTestObject {
        private String name;
        private int age;
        private long id;
    }
}