package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Set;

import static org.apache.flink.table.runtime.ml.python.mlframework.source.MockMLSourceReader.getMlSourceReader;
import static org.apache.flink.table.runtime.ml.python.mlframework.source.MockMLSplitEnumerator.getMlSplitEnumerator;

public class MockMLSource implements Source<Integer, MockMLSourceSplit, Set<MockMLSourceSplit>> {

	private final Boundedness boundedness;

	public MockMLSource(Boundedness boundedness) {
		this.boundedness = boundedness;
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SourceReader<Integer, MockMLSourceSplit> createReader(SourceReaderContext readerContext) {
		//Singleton
		return getMlSourceReader();
	}

	@Override
	public SplitEnumerator<MockMLSourceSplit, Set<MockMLSourceSplit>> createEnumerator(
		SplitEnumeratorContext<MockMLSourceSplit> enumContext) {
		//Singleton
		return getMlSplitEnumerator(enumContext);
	}

	@Override
	public SplitEnumerator<MockMLSourceSplit, Set<MockMLSourceSplit>> restoreEnumerator(
		SplitEnumeratorContext<MockMLSourceSplit> enumContext,
		Set<MockMLSourceSplit> checkpoint) {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<MockMLSourceSplit> getSplitSerializer() {
		return new MockMLSourceSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<Set<MockMLSourceSplit>> getEnumeratorCheckpointSerializer() {
		return new MockMLSplitEnumeratorCheckpointSerializer();
	}
}
