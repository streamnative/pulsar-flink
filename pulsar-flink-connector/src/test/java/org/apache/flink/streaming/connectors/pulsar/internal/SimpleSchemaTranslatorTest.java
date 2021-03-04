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

import org.apache.flink.streaming.connectors.pulsar.SchemaData;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit test of {@link SimpleSchemaTranslator}.
 */
public class SimpleSchemaTranslatorTest extends TestLogger {

    private SimpleSchemaTranslator schemaTranslator;

    @Before
    public void initSchemaTranslator() {
        this.schemaTranslator = new SimpleSchemaTranslator(false);
    }

    @Test
    public void testSchemaInfo2TableProperties() throws Exception {
        //json
        SchemaInfo jsonSchemaInfo = JSONSchema.of(SchemaData.Foo.class).getSchemaInfo();
        Map<String, String> jsonProperties = schemaTranslator.schemaInfo2TableProperties(jsonSchemaInfo);
        assertEquals(jsonProperties.get("format"), "json");
        //avro
        SchemaInfo avroSchemaInfo = AvroSchema.of(SchemaData.Foo.class).getSchemaInfo();
        Map<String, String> avroProperties = schemaTranslator.schemaInfo2TableProperties(avroSchemaInfo);
        assertEquals(jsonProperties.get("format"), "avro");

        //keyValue
        SchemaInfo kvSchemaInfo =
                KeyValueSchema.of(new StringSchema(), AvroSchema.of(SchemaData.Foo.class)).getSchemaInfo();
        Map<String, String> kvProperties = schemaTranslator.schemaInfo2TableProperties(kvSchemaInfo);
        assertEquals(jsonProperties.get("value.format"), "avro");
        assertEquals(jsonProperties.get("key.format"), "raw");
        assertEquals(jsonProperties.get("key.fields"), "key");

        //primitive
        SchemaInfo primitiveSchemaInfo =
                new StringSchema().getSchemaInfo();
        Map<String, String> primitiveProperties = schemaTranslator.schemaInfo2TableProperties(primitiveSchemaInfo);
        assertEquals(primitiveProperties.get("format"), "raw");
    }
}
