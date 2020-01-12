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

import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.testutils.FailingIdentityMapper;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class FlinkPulsarTableITest extends PulsarTestBaseWithFlink {

    @Before
    public void clearState() {
        StreamITCase.testResults().clear();
        FailingIdentityMapper.failedBefore = false;
    }

    @Test
    public void testBasicFunctioning() throws PulsarClientException {
        val see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        val tEnv = StreamTableEnvironment.create(see);

        String table = newTopic();

        sendTypedMessages(table, SchemaType.BOOLEAN, SchemaData.booleanList, Optional.empty());
    }

    private final AtomicInteger topicId = new AtomicInteger(0);

    private String newTopic() {
        return TopicName.get("topic-" + topicId.getAndIncrement()).toString();
    }
}
