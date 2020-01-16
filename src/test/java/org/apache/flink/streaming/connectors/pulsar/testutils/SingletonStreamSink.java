package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SingletonStreamSink {
    public static final List<String> sinkedResults = new ArrayList<>();

    public static void clear() {
        sinkedResults.clear();
    }

    public static final class StringSink<T> extends RichSinkFunction<T> {
        @Override
        public void invoke(T value, Context context) throws Exception {
            synchronized (sinkedResults) {
                sinkedResults.add(value.toString());
            }
        }
    }

    public static void compareWithList(List<String> expected) {
        expected.sort(null);
        SingletonStreamSink.sinkedResults.sort(null);
        assertEquals(expected, sinkedResults);
    }
}
