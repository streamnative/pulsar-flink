package org.apache.flink.table.catalog.pulsar;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.flink.table.types.DataType;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class TableSchemaHelper {

    public static SchemaInfo serialize(CatalogBaseTable table) throws IOException {
        // handle table meta data
        Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("COMMENT", table.getComment());
        propertyMap.put("OPTIONS", JsonUtils.MAPPER.writeValueAsString(table.getOptions()));

        FlinkTableSchemaWrapper tableSchema = new FlinkTableSchemaWrapper();
        // watermark
        tableSchema.setWatermarkSpecs(table.getSchema().getWatermarkSpecs()
            .stream()
            .map(spec -> new Tuple3<String, String, DataType>(spec.getRowtimeAttribute(),
                spec.getWatermarkExpr(),
                spec.getWatermarkExprOutputType()))
            .collect(Collectors.toList())
        );

        // primaryKey
        Optional<UniqueConstraint> pkey = table.getSchema().getPrimaryKey();
        if (pkey.isPresent()) {
            tableSchema.setPrimaryKey(
                new Tuple3<Boolean, String, List<String>>(true, pkey.get().getName(), pkey.get().getColumns()));
        } else {
            tableSchema.setPrimaryKey(new Tuple3<Boolean, String, List<String>>(false, "", null));
        }

        // columns
        List<Tuple4<Integer, String, DataType, String>> columns = table.getSchema().getTableColumns()
            .stream()
            .map(tc -> FlinkTableSchemaWrapper.fromTableColumn(tc))
            .collect(Collectors.toList());
        tableSchema.setTableColumns(columns);

        byte[] schemaBytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(tableSchema);
            out.flush();
            schemaBytes = Base64.getEncoder().encode(bos.toByteArray());
        }

        SchemaInfo tableInfo = SchemaInfoImpl.builder()
            .name("tableMetadata")
            .type(SchemaType.BYTES)
            .schema(schemaBytes)
            .properties(propertyMap)
            .build();

        System.out.println("table schema: " + table.getSchema().toString());
        System.out.println("stored metadata: " + tableInfo.toString());

        return tableInfo;
    }

    public static CatalogTable deserialize(SchemaInfo metadata, Map<String, String> defaultOptions) throws IOException, ClassNotFoundException {
        Map<String, String> properties = metadata.getProperties();
        String comment = properties.get("COMMENT");

        HashMap<String, String> tableOptions = JsonUtils.MAPPER.readValue(properties.get("OPTIONS"),
            new TypeReference<HashMap<String, String>>() {});
        // add and override default options with table specific options
        defaultOptions.putAll(tableOptions);

        FlinkTableSchemaWrapper schemaWrapper;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(metadata.getSchema()));
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            schemaWrapper = (FlinkTableSchemaWrapper) ois.readObject();
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        // watermark
        schemaWrapper.watermarkSpecs
            .forEach(t -> tableSchemaBuilder.watermark(new WatermarkSpec(t.f0, t.f1, t.f2)));

        // primarykey
        if (schemaWrapper.primaryKey.f0) {
            tableSchemaBuilder.primaryKey(schemaWrapper.primaryKey.f1,
                schemaWrapper.primaryKey.f2.toArray(new String[0]));
        }

        // columns
        schemaWrapper.tableColumns
            .forEach(t -> tableSchemaBuilder.add(FlinkTableSchemaWrapper.toTableColumn(t)));

        return new CatalogTableImpl(tableSchemaBuilder.build(), defaultOptions, comment);
    }
}
