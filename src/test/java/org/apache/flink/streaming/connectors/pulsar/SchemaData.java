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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SchemaData {

    public static final List<Boolean> booleanList = Arrays.asList(true, false, true, true, false);
    public static final List<Integer> int32List = Arrays.asList(1, 2, 3, 4, 5);
    public static final List<Byte> int8List = int32List.stream().map(Integer::byteValue).collect(Collectors.toList());
    public static final List<Short> int16List = int32List.stream().map(Integer::shortValue).collect(Collectors.toList());
    public static final List<Long> int64List = int32List.stream().map(Integer::longValue).collect(Collectors.toList());
    public static final List<Double> doubleList = int32List.stream().map(Integer::doubleValue).collect(Collectors.toList());
    public static final List<Float> floatList = int32List.stream().map(Integer::floatValue).collect(Collectors.toList());
    public static final List<String> stringList = int32List.stream().map(Objects::toString).collect(Collectors.toList());
    public static List<Date> dateList;
    public static List<Timestamp> timestampList;
    
    static {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        dateList = int32List.stream().map(i -> {
            cal.set(2019, 0, i);
            return cal.getTime();
        }).collect(Collectors.toList());
        
        cal.clear();
        timestampList = int32List.stream().map(i -> {
            cal.set(2019, 0, i, 20, 35, 40);
            return new Timestamp(cal.getTimeInMillis());
        }).collect(Collectors.toList());
    }
    
}
