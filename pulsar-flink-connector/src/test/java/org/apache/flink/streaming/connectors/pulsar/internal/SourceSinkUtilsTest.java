package org.apache.flink.streaming.connectors.pulsar.internal;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Unit test for SourceSinkUtils. */
public class SourceSinkUtilsTest {
    @Test
    public void testRangeTopicBelongTo() {
        String topic = "persistent://cme_dev/market_data_mbo_v1/345_0-partition-0";
        SerializableRange range1 = SerializableRange.of(0, 16383);
        SerializableRange range2 = SerializableRange.of(16384, 32767);
        SerializableRange range3 = SerializableRange.of(32768, 49151);
        SerializableRange range4 = SerializableRange.of(32768, 49151);

        assertTrue(SourceSinkUtils.belongsTo(topic, range1, 4, 0));
        assertTrue(SourceSinkUtils.belongsTo(topic, range2, 4, 1));
        assertTrue(SourceSinkUtils.belongsTo(topic, range3, 4, 2));
        assertTrue(SourceSinkUtils.belongsTo(topic, range4, 4, 3));
    }
}
