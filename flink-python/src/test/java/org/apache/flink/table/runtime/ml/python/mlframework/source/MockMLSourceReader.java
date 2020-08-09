package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockMLSourceReader implements SourceReader<Integer, MockMLSourceSplit> {
	private static final MockMLSourceReader MOCK_ML_SOURCE_READER = new MockMLSourceReader();
	private boolean started;
	private boolean closed;
	private boolean finished;

	protected static final Logger LOG = LoggerFactory.getLogger(MockMLSourceReader.class);

	private MockMLSourceReader() {
		//LOG.info("construct reader");
		System.out.println("construct reader");
		this.started = false;
		this.closed = false;
	}

	public static MockMLSourceReader getMlSourceReader(){
		return MOCK_ML_SOURCE_READER;
	}

	@Override
	public void start() {
		this.started = true;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<Integer> sourceOutput) throws Exception {
		sourceOutput.collect(1);
		return finished ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
	}

	@Override
	public List<MockMLSourceSplit> snapshotState() {
		return null;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		System.out.println("ask?");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void addSplits(List<MockMLSourceSplit> splits) {

	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		if(sourceEvent instanceof MockMLSourceEvent) {
			this.finished = ((MockMLSourceEvent) sourceEvent).getDone();
			LOG.info("receive event from SplitEnumerator and change finish status to: {}", this.finished);
			//System.out.println("receive event from SplitEnumerator and change finish status to:  " + this.finished);
		}
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
	}
}
