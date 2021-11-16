/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.table.descriptors;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

import static org.apache.flink.streaming.connectors.pulsar.table.descriptors.AtomicValidator.FORMAT_TYPE_VALUE;

/** Atomic {@link ConnectorDescriptor}. */
public class Atomic extends FormatDescriptor {

    // private boolean useExtendFields;
    private String className;

    /** Format descriptor for JSON. */
    public Atomic() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /*public Atomic useExtendFields(boolean useExtendFields){
        this.useExtendFields = useExtendFields;
        return this;
    }*/

    public Atomic setClass(String className) {
        this.className = className;
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putString(AtomicValidator.FORMAT_CLASS_NAME, className);
        return properties.asMap();
    }
}
