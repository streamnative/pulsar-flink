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

package org.apache.flink.formats.protobuf;

import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;

public class PbFormatUtils {

    // protobuf code has a bug that, f_abc_7d will be convert to fAbc7d, but actually we need fAbc7D
    public static String fieldNameToJsonName(String name) {
        final int length = name.length();
        StringBuilder result = new StringBuilder(length);
        boolean isNextUpperCase = false;
        for (int i = 0; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                isNextUpperCase = true;
            } else if (isNextUpperCase) {
                // This closely matches the logic for ASCII characters in:
                // http://google3/google/protobuf/descriptor.cc?l=249-251&rcl=228891689
                if ('a' <= ch && ch <= 'z') {
                    ch = (char) (ch - 'a' + 'A');
                    isNextUpperCase = false;
                }
                result.append(ch);
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    public static boolean isSimpleType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static String getStrongCamelCaseJsonName(String name) {
        String jsonName = fieldNameToJsonName(name);
        if (jsonName.length() == 1) {
            return jsonName.toUpperCase();
        } else {
            return jsonName.substring(0, 1).toUpperCase() + jsonName.substring(1);
        }
    }

    public static Descriptors.Descriptor getDescriptor(String className) {
        try {
            Class<?> pbClass = Class.forName(className);
            return (Descriptors.Descriptor)
                    pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
        } catch (Exception y) {
            throw new IllegalArgumentException(
                    String.format("get %s descriptors error!", className), y);
        }
    }

    public static String getFullJavaName(Descriptors.Descriptor descriptor) {
        if (null != descriptor.getContainingType()) {
            return getFullJavaName(descriptor.getContainingType()) + "." + descriptor.getName();
        } else {
            return descriptor.getFullName();
        }
    }

    public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
        if (null != enumDescriptor.getContainingType()) {
            return getFullJavaName(enumDescriptor.getContainingType())
                    + "."
                    + enumDescriptor.getName();
        } else {
            return enumDescriptor.getFullName();
        }
    }
}
