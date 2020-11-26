package org.apache.flink.formats.atomic;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class AtomicRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final long serialVersionUID = -228294330688809195L;

    private final String className;
    private final boolean useExtendFields;
    private final Class<?> clazz;

    public AtomicRowDeserializationSchema(String className, boolean useExtendFields) {
        this.className = className;
        this.useExtendFields = useExtendFields;
        try {
            this.clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isUseExtendFields() {
        return useExtendFields;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        DataType dataType = TypeConversions.fromClassToDataType(clazz).
                orElseThrow(()->new IllegalStateException(clazz.getCanonicalName() + "cant cast to flink dataType"));
        try {
            Schema schema = SimpleSchemaTranslator.sqlType2PulsarSchema(dataType);
            Object data = schema.decode(message);
            Row row = new Row(1);
            row.setField(0, data);
            return row;
        } catch (IncompatibleSchemaException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        DataType dataType = TypeConversions.fromClassToDataType(clazz).
                orElseThrow(()->new IllegalStateException(clazz.getCanonicalName() + "cant cast to flink dataType"));
        RowType.RowField rowField = new RowType.RowField("value", dataType.getLogicalType());
        List<RowType.RowField> fields = Collections.singletonList(rowField);
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(TypeConversions.fromLogicalToDataType(new RowType(fields)));

        //return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(dataType);

        /*List<DataTypes.Field> mainSchema = new ArrayList<>();
        DataType dataType = TypeConversions.fromClassToDataType(clazz).
                orElseThrow(()->new IllegalStateException(clazz.getCanonicalName() + "cant cast to flink dataType"));
        if (dataType instanceof FieldsDataType) {
            FieldsDataType fieldsDataType = (FieldsDataType) dataType;
            RowType rowType = (RowType) fieldsDataType.getLogicalType();
            List<String> fieldNames = rowType.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
                DataTypes.Field field = DataTypes.FIELD(fieldNames.get(i), TypeConversions.fromLogicalToDataType(logicalType));
                mainSchema.add(field);
            }

        } else {
            mainSchema.add(DataTypes.FIELD("value", dataType));
        }

        if (useExtendFields) {
            mainSchema.addAll(SimpleSchemaTranslator.METADATA_FIELDS);
        }
        FieldsDataType fieldsDataType = (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(fieldsDataType);*/
    }

    /**
     * Builder for {@link AtomicRowDeserializationSchema}.
     */
    public static class Builder<T> {
        private final String className;
        private boolean useExtendFields;

        public Builder(String className) {
            this.className = className;
        }

        public AtomicRowDeserializationSchema.Builder useExtendFields(boolean useExtendFields) {
            this.useExtendFields = useExtendFields;
            return this;
        }

        public AtomicRowDeserializationSchema build() {
            return new AtomicRowDeserializationSchema(className, useExtendFields);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AtomicRowDeserializationSchema that = (AtomicRowDeserializationSchema) o;

        if (useExtendFields != that.useExtendFields) {
            return false;
        }
        return className.equals(that.className);
    }

    @Override
    public int hashCode() {
        int result = className.hashCode();
        result = 31 * result + (useExtendFields ? 1 : 0);
        return result;
    }
}
