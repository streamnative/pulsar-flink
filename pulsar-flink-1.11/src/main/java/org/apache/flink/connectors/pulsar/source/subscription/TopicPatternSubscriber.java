/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

	   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connectors.pulsar.source.subscription;

import org.apache.flink.connectors.pulsar.source.Partition;
import org.apache.flink.connectors.pulsar.source.StickyKeyAssigner;
import org.apache.flink.connectors.pulsar.source.util.AsyncUtils;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A subscriber to a topic pattern.
 */
public class TopicPatternSubscriber extends AbstractPulsarSubscriber {
	private static final long serialVersionUID = -7471048577725467797L;
	private final String namespace;
	private final StickyKeyAssigner stickyKeyAssigner;
	private final Pattern topicPattern;

	public TopicPatternSubscriber(String namespace, StickyKeyAssigner stickyKeyAssigner, String... topicPatterns) {
		this.namespace = checkNotNull(namespace);
		this.stickyKeyAssigner = checkNotNull(stickyKeyAssigner);
		checkArgument(topicPatterns.length > 0, "At least one pattern needs to be specified");
		// shorten patterns and compile into one big pattern
		topicPattern = Pattern.compile(Arrays.stream(topicPatterns)
			.map(topicsPattern -> topicsPattern.split("\\:\\/\\/")[1])
			.collect(Collectors.joining("|")));
	}

	@Override
	protected Collection<Partition> getCurrentPartitions(PulsarAdmin pulsarAdmin) throws PulsarAdminException, InterruptedException, IOException {
		List<Partition> partitions = new ArrayList<>();
		Topics topics = pulsarAdmin.topics();
		addStickyPartitions(topics.getList(namespace), partitions, pulsarAdmin);

		List<String> partitionedTopicList = topics.getPartitionedTopicList(namespace);
		if (stickyKeyAssigner == StickyKeyAssigner.AUTO) {
			try {
				AsyncUtils.parallelAsync(
					partitionedTopicList,
					pulsarAdmin.topics()::getPartitionedTopicMetadataAsync,
					(topic, topicMetadata) -> {
						for (int partitionIndex = 0; partitionIndex < topicMetadata.partitions; partitionIndex++) {
							String fullName = TopicName.PARTITIONED_TOPIC_SUFFIX + partitionIndex;
							partitions.add(new Partition(fullName, Partition.AUTO_KEY_RANGE));
						}
					},
					PulsarAdminException.class);
			} catch (TimeoutException e) {
				throw new IOException("Cannot retrieve partition information: " + e.getMessage());
			}
		} else {
			addStickyPartitions(partitionedTopicList, partitions, pulsarAdmin);
		}
		return partitions;
	}

	private void addStickyPartitions(List<String> topics, List<Partition> partitions, PulsarAdmin pulsarAdmin) throws PulsarAdminException {
		for (String topic : topics) {
			if (topicPattern.matcher(topic).matches()) {
				for (Collection<Range> range : stickyKeyAssigner.getRanges(topic, pulsarAdmin)) {
					partitions.add(new Partition(topic, range));
				}
			}
		}
	}

}
