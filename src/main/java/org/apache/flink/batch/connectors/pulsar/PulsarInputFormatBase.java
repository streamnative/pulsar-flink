package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

public class PulsarInputFormatBase<T> extends RichInputFormat<T, PulsarInputSplit> {

	@Override
	public void configure(Configuration parameters) {
		// do nothing here
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public PulsarInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return new PulsarInputSplit[0];
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(PulsarInputSplit[] inputSplits) {
		return null;
	}

	@Override
	public void open(PulsarInputSplit split) throws IOException {

	}

	@Override
	public boolean reachedEnd() throws IOException {
		return false;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
