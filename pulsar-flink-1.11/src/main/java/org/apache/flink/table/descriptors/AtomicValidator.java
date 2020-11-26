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

package org.apache.flink.table.descriptors;

/**
 * Atomic connector validator.
 */
public class AtomicValidator extends FormatDescriptorValidator {
    public static final String FORMAT_TYPE_VALUE = "atomic";
    public static final String FORMAT_ATOMIC_SCHEMA = "format.atomic-schema";
    public static final String FORMAT_CLASS_NAME = "format.classname";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        //TODO add validate for classname
    }
}
