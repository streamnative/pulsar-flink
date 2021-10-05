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


package org.apache.flink.table.catalog.pulsar;

import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.List;

// wrap table schema info for serialization/deserialization purpose
@Data
public class FlinkTableSchemaWrapper implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;

    // Watermark
    List<Tuple3<String, String, DataType>> watermarkSpecs;

    // PrimaryKey (isDefined, name, columns)
    Tuple3<Boolean, String, List<String>> primaryKey;

    // Columns, (column type, name, datatype, expression)
    List<Tuple4<Integer, String, DataType, String>> tableColumns;

    public static TableColumn toTableColumn(Tuple4<Integer, String, DataType, String> tuple) {
        switch (tuple.f0) {
            case 0:
                return TableColumn.physical(tuple.f1, tuple.f2);
            case 1:
                return TableColumn.computed(tuple.f1, tuple.f2, tuple.f3);
            case 2:
                return TableColumn.metadata(tuple.f1, tuple.f2);
            default:
                throw new CatalogException("Failed to restore table column");
        }
    }

    public static Tuple4<Integer, String, DataType, String> fromTableColumn(TableColumn tableColumn) {
        if (tableColumn instanceof TableColumn.PhysicalColumn) {
            return new Tuple4<>(0, tableColumn.getName(), tableColumn.getType(), "");
        } else if (tableColumn instanceof TableColumn.ComputedColumn) {
            return new Tuple4<>(1, tableColumn.getName(), tableColumn.getType(), ((TableColumn.ComputedColumn) tableColumn).getExpression());
        } else if (tableColumn instanceof TableColumn.MetadataColumn) {
            return new Tuple4<>(2, tableColumn.getName(), tableColumn.getType(), "");
        } else {
            throw new CatalogException("Can't recognize table column type.");
        }
    }
}
