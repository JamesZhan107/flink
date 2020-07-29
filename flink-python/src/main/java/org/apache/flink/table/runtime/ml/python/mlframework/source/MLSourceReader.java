package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MLSourceReader<T> implements SourceReader<T, MLSourceSplit> {
	private static final MLSourceReader mlSourceReader = new MLSourceReader();
	private boolean started;
	private boolean closed;
	private boolean finished;

	private MLSourceReader() {
		System.out.println("construct reader");
		this.started = false;
		this.closed = false;
	}

	public static MLSourceReader getMlSourceReader(){
		return mlSourceReader;
	}

	@Override
	public void start() {
		System.out.println("start");
		this.started = true;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> sourceOutput) throws Exception {
		return finished ? InputStatus.END_OF_INPUT : InputStatus.MORE_AVAILABLE;
	}

	@Override
	public List<MLSourceSplit> snapshotState() {
		return null;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void addSplits(List<MLSourceSplit> splits) {

	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		if(sourceEvent instanceof MLSourceEvent) {
			this.finished = ((MLSourceEvent) sourceEvent).getDone();
			System.out.println("receive event from SplitEnumerator and change finish status to:  " + this.finished);
		}
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
	}
}
