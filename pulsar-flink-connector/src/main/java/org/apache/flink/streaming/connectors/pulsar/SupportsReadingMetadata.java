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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Before Flink 1.12, we simulated this interface, after which we will use the official interface.
 */
@PublicEvolving
public interface SupportsReadingMetadata {

    /**
     * Returns the map of metadata keys and their corresponding data types that can be produced by this
     * table source for reading.
     *
     * <p>The returned map will be used by the planner for validation and insertion of explicit casts
     * (see {@link LogicalTypeCasts#supportsExplicitCast(LogicalType, LogicalType)}) if necessary.
     *
     * <p>The iteration order of the returned map determines the order of metadata keys in the list
     * passed in {@link #applyReadableMetadata(List, DataType)}. Therefore, it might be beneficial to
     * return a {@link LinkedHashMap} if a strict metadata column order is required.
     *
     * <p>If a source forwards metadata from one or more formats, we recommend the following column
     * order for consistency:
     *
     * <pre>{@code
     *   KEY FORMAT METADATA COLUMNS + VALUE FORMAT METADATA COLUMNS + SOURCE METADATA COLUMNS
     * }</pre>
     *
     * <p>Metadata key names follow the same pattern as mentioned in {@link Factory}. In case of duplicate
     * names in format and source keys, format keys shall have higher precedence.
     *
     * <p>Regardless of the returned {@link DataType}s, a metadata column is always represented using
     * internal data structures (see {@link RowData}).
     *
     */
    Map<String, DataType> listReadableMetadata();

    /**
     * Provides a list of metadata keys that the produced {@link RowData} must contain as appended
     * metadata columns.
     *
     * <p>Note: Use the passed data type instead of {@link TableSchema#toPhysicalRowDataType()} for
     * describing the final output data type when creating {@link TypeInformation}. If the source implements
     * {@link SupportsProjectionPushDown}, the projection is already considered in the given output
     * data type.
     *
     * @param metadataKeys a subset of the keys returned by {@link #listReadableMetadata()}, ordered
     *                     by the iteration order of returned map
     * @param producedDataType the final output type of the source
     *
     */
    void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType);
}
