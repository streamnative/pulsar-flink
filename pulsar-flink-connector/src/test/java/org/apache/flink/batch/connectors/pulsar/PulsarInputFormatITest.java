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

package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.common.ConnectorConfig;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Integration test for PulsarInputFormat.
 */
public class PulsarInputFormatITest {

	private String adminUrl = System.getenv("PULSAR_ADMIN_URL");
	private String serviceUrl = System.getenv("PULSAR_SERVICE_URL");
	private String zkUrl = System.getenv("PULSAR_ZK_URL");

	@Test
	public void testBatchSimpleFormat() throws Exception {
		String topicName = TopicName.get("topic" + RandomStringUtils.randomNumeric(10)).toString();
		List<Integer> messages =
				IntStream.range(0, 50).mapToObj(t -> Integer.valueOf(t)).collect(Collectors.toList());

		try (PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
			pulsarAdmin.topics().createSubscriptionAsync(topicName, "test", MessageId.earliest);
		}
		sendTypedMessages(serviceUrl, topicName, Schema.INT32, messages);
		ConnectorConfig config = new ConnectorConfig();
		config.setTopic(topicName);
		config.setAdminUrl(adminUrl);
		config.setZookeeperUri(zkUrl);

		PulsarInputFormat<Integer> inputFormat = new PulsarInputFormat<>(config, new IntegerDeserializer());
		inputFormat.openInputFormat();
		InputSplit[] splits = inputFormat.createInputSplits(3);
		Assert.assertTrue(splits.length <= 3);
		List<Integer> ilist = new ArrayList<>(50);

		for (InputSplit split : splits) {
			inputFormat.open((PulsarInputSplit) split);
			while (!inputFormat.reachedEnd()) {
				ilist.add(inputFormat.nextRecord(1));
			}
			inputFormat.close();
		}

		inputFormat.closeInputFormat();
		// TODO: the last entry added cannot be seen because of https://github.com/apache/pulsar/pull/5822.
		// when we bump Pulsar version to 2.5.1, this comment can be removed.
		Assert.assertArrayEquals(ilist.toArray(new Integer[0]),
				messages.subList(0, messages.size() - 1).toArray(new Integer[0]));
	}

	private static <T> void sendTypedMessages(String serviceUrl, String topicName,
											  Schema<T> schemaType, List<T> messages) throws PulsarClientException {
		try (
				PulsarClient pulsarClient = PulsarClient.builder()
						.serviceUrl(serviceUrl)
						.build();
				Producer<T> producer = pulsarClient.newProducer(schemaType)
						.topic(topicName)
						.create();
		) {
			for (T message : messages) {
				producer.send(message);
			}
		}
	}

	private static class IntegerDeserializer implements DeserializationSchema<Integer> {
		private final TypeInformation<Integer> ti;
		private final TypeSerializer<Integer> ser;

		public IntegerDeserializer() {
			this.ti = org.apache.flink.api.scala.typeutils.Types.INT();
			this.ser = ti.createSerializer(new ExecutionConfig());
		}

		@Override
		public Integer deserialize(byte[] message) throws IOException {

			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
			Integer i = ser.deserialize(in);

			return i;
		}

		@Override
		public boolean isEndOfStream(Integer nextElement) {
			return false;
		}

		@Override
		public TypeInformation<Integer> getProducedType() {
			return ti;
		}
	}
}
