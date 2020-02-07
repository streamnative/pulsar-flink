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

package org.apache.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.batch.connectors.pulsar.PulsarInputFormat;
import org.apache.flink.batch.connectors.pulsar.PulsarInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.pulsar.common.ConnectorConfig;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBaseWithFlink;

import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Integration test for PulsarInputFormat.
 */
public class PulsarInputFormatITest extends PulsarTestBaseWithFlink {

	@Test
	public void testBatchSimpleFormat() throws Exception {

		String tp = newTopic();
		List<Integer> messages =
				IntStream.range(0, 50).mapToObj(t -> Integer.valueOf(t)).collect(Collectors.toList());

		sendTypedMessages(tp, SchemaType.INT32, messages, Optional.empty());

		ConnectorConfig config = new ConnectorConfig();
		config.setTopic(tp);
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
		Assert.assertArrayEquals(ilist.toArray(new Integer[0]), messages.subList(0, messages.size() - 1).toArray(new Integer[0]));
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
