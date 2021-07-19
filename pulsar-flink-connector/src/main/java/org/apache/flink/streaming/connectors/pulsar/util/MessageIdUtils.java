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

package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.io.Serializable;

/**
 * support position move for MessageId.
 */
public class MessageIdUtils {

    private static final MessageIdOperate<MessageIdImpl> messageIdImplOperate = new MessageIdImplOperate();

    private static final MessageIdOperate<BatchMessageIdImpl> batchMessageIdImplOperate = new BatchMessageIdImplOperate();

    public static MessageId next(MessageId current) {
        return getOperate(current).next(current);
    }

    public static MessageId prev(MessageId current) {
        return getOperate(current).prev(current);
    }

    private static <T extends MessageId> MessageIdOperate<T> getOperate(T current) {
        if (current instanceof BatchMessageIdImpl) {
            return (MessageIdOperate<T>) batchMessageIdImplOperate;
        }
        if (current instanceof MessageIdImpl) {
            return (MessageIdOperate<T>) messageIdImplOperate;
        }
        throw new UnsupportedOperationException("MessageId type: " + current.getClass().getCanonicalName());
    }

    /**
     * MessageId Operate.
     *
     * @param <T> MessageId Impl
     */
    public interface MessageIdOperate<T extends MessageId> extends Serializable {
        MessageId next(T current);

        MessageId prev(T current);
    }

    /**
     * MessageIdImpl Operate.
     */
    public static class MessageIdImplOperate implements MessageIdOperate<MessageIdImpl> {

        @Override
        public MessageId next(MessageIdImpl current) {
            return new MessageIdImpl(current.getLedgerId(), current.getEntryId() + 1, current.getPartitionIndex());
        }

        @Override
        public MessageId prev(MessageIdImpl current) {
            return new MessageIdImpl(current.getLedgerId(), current.getEntryId() - 1, current.getPartitionIndex());
        }
    }

    /**
     * BatchMessageIdImpl Operate.
     */
    public static class BatchMessageIdImplOperate implements MessageIdOperate<BatchMessageIdImpl> {

        @Override
        public MessageId next(BatchMessageIdImpl current) {
            return new MessageIdImpl(current.getLedgerId(), current.getEntryId() + 1, current.getPartitionIndex());
        }

        @Override
        public MessageId prev(BatchMessageIdImpl current) {
            return current.prevBatchMessageId();
        }
    }
}
