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

package org.apache.flink.connectors.pulsar.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connectors.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.function.RunnableWithException;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * The source reader for Pulsar partitions.
 */
public class PulsarSourceReader<T>
		extends SingleThreadMultiplexSourceReaderBase<ParsedMessage<T>, T, PulsarPartitionSplit, PulsarPartitionSplit> {

	private final RunnableWithException closeCallback;

	public PulsarSourceReader(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<ParsedMessage<T>>> elementsQueue,
			Supplier<SplitReader<ParsedMessage<T>, PulsarPartitionSplit>> splitReaderSupplier,
			RecordEmitter<ParsedMessage<T>, T, PulsarPartitionSplit> recordEmitter,
			Configuration config,
			SourceReaderContext context,
			RunnableWithException closeCallback) {
		super(futureNotifier, elementsQueue, splitReaderSupplier, recordEmitter, config, context);
		this.closeCallback = closeCallback;
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {
	}

	@Override
	public void close() throws Exception {
		super.close();
		closeCallback.run();
	}

	@Override
	protected PulsarPartitionSplit initializedState(PulsarPartitionSplit split) {
		return split.clone();
	}

	@Override
	protected PulsarPartitionSplit toSplitType(String splitId, PulsarPartitionSplit split) {
		return split.clone();
	}
}
