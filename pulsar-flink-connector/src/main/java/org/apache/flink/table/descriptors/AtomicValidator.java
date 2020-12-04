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

package org.apache.flink.table.descriptors;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Atomic connector validator.
 */
public class AtomicValidator extends FormatDescriptorValidator {
    public static final String FORMAT_TYPE_VALUE = "atomic";
    public static final String FORMAT_ATOMIC_SCHEMA = "format.atomic-schema";
    public static final String FORMAT_CLASS_NAME = "format.classname";

    public static final List<Class<?>> PULSAR_PRIMITIVES = new ArrayList<>();

    static {
        PULSAR_PRIMITIVES.add(Boolean.TYPE);
        PULSAR_PRIMITIVES.add(Byte.TYPE);
        PULSAR_PRIMITIVES.add(Short.TYPE);
        PULSAR_PRIMITIVES.add(Integer.TYPE);
        PULSAR_PRIMITIVES.add(Long.TYPE);
        PULSAR_PRIMITIVES.add(Float.TYPE);
        PULSAR_PRIMITIVES.add(Double.TYPE);

        PULSAR_PRIMITIVES.add(Byte.class);
        PULSAR_PRIMITIVES.add(byte[].class);
        PULSAR_PRIMITIVES.add(Byte[].class);
        PULSAR_PRIMITIVES.add(Boolean.class);
        PULSAR_PRIMITIVES.add(Short.class);
        PULSAR_PRIMITIVES.add(Integer.class);
        PULSAR_PRIMITIVES.add(Long.class);
        PULSAR_PRIMITIVES.add(String.class);
        PULSAR_PRIMITIVES.add(Float.class);
        PULSAR_PRIMITIVES.add(Double.class);
        PULSAR_PRIMITIVES.add(Date.class);
        PULSAR_PRIMITIVES.add(Time.class);
        PULSAR_PRIMITIVES.add(Timestamp.class);
        PULSAR_PRIMITIVES.add(LocalDate.class);
        PULSAR_PRIMITIVES.add(LocalTime.class);
        PULSAR_PRIMITIVES.add(LocalDateTime.class);
        PULSAR_PRIMITIVES.add(Instant.class);
    }

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        final String classname = properties.getString(FORMAT_CLASS_NAME);
        Class<?> clazz;
        try {
            clazz = Class.forName(classname);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        if (!PULSAR_PRIMITIVES.contains(clazz)) {
            throw new IllegalArgumentException("not support class type: " + classname);
        }
    }
}
