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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.Serializable;

/**
 * schema translator.
 */
public interface SchemaTranslator extends Serializable {

    SchemaInfo tableSchemaToPulsarSchema(TableSchema schema) throws IncompatibleSchemaException;

    TableSchema pulsarSchemaToTableSchema(SchemaInfo pulsarSchema) throws IncompatibleSchemaException;

    FieldsDataType pulsarSchemaToFieldsDataType(SchemaInfo pulsarSchema) throws IncompatibleSchemaException;

    DataType schemaInfo2SqlType(SchemaInfo si) throws IncompatibleSchemaException;
}
