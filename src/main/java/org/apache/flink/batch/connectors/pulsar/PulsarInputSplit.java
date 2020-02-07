package org.apache.flink.batch.connectors.pulsar;

import org.apache.flink.core.io.InputSplit;

public class PulsarInputSplit implements InputSplit {
	@Override
	public int getSplitNumber() {
		return 0;
	}
}
