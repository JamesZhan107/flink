package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Set;

import static org.apache.flink.mlframework.source.MLSourceReader.getMlSourceReader;
import static org.apache.flink.mlframework.source.MLSplitEnumerator.getMlSplitEnumerator;

public class MLSource implements Source<Integer, MLSourceSplit, Set<MLSourceSplit>> {

	private final Boundedness boundedness;

	public MLSource(Boundedness boundedness) {
		this.boundedness = boundedness;
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SourceReader<Integer, MLSourceSplit> createReader(SourceReaderContext readerContext) {
		//Singleton
		return getMlSourceReader();
	}

	@Override
	public SplitEnumerator<MLSourceSplit, Set<MLSourceSplit>> createEnumerator(
		SplitEnumeratorContext<MLSourceSplit> enumContext) {
		//Singleton
		return getMlSplitEnumerator(enumContext);
	}

	@Override
	public SplitEnumerator<MLSourceSplit, Set<MLSourceSplit>> restoreEnumerator(
		SplitEnumeratorContext<MLSourceSplit> enumContext,
		Set<MLSourceSplit> checkpoint) throws IOException {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<MLSourceSplit> getSplitSerializer() {
		return new MLSourceSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<Set<MLSourceSplit>> getEnumeratorCheckpointSerializer() {
		return new MLSplitEnumeratorCheckpointSerializer();
	}
}
