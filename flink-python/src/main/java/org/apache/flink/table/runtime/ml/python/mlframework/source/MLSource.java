package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Set;

import static org.apache.flink.table.runtime.ml.python.mlframework.source.MLSourceReader.getMlSourceReader;
import static org.apache.flink.table.runtime.ml.python.mlframework.source.MLSplitEnumerator.getMlSplitEnumerator;

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
		Set<MLSourceSplit> checkpoint) {
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
