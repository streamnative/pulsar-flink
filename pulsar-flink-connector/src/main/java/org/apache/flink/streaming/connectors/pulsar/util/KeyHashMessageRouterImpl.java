package org.apache.flink.streaming.connectors.pulsar.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;
@Slf4j
public class KeyHashMessageRouterImpl implements MessageRouter {
    public KeyHashMessageRouterImpl() {

    }

    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        return MathUtil.toPositive(MathUtil.murmur2(msg.getKeyBytes())) % metadata.numPartitions();
    }
}
