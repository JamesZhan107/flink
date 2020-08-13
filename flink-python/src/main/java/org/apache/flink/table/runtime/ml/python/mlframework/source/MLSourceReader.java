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

public class MLSourceReader<RowData> implements SourceReader<RowData, MLSourceSplit> {
	private static final MLSourceReader mlSourceReader = new MLSourceReader();
	private boolean started;
	private boolean closed;
	private boolean finished;

	protected static final Logger LOG = LoggerFactory.getLogger(MLSourceReader.class);

	private MLSourceReader() {
		this.started = false;
		this.closed = false;
	}

	public static MLSourceReader getMlSourceReader(){
		return mlSourceReader;
	}

	@Override
	public void start() {
		this.started = true;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<RowData> sourceOutput) throws Exception {
		GenericRowData rowData = new GenericRowData(1);
		rowData.setField(0,1);
		sourceOutput.collect((RowData) rowData);
		return finished ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
	}

	@Override
	public List<MLSourceSplit> snapshotState() {
		return null;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void addSplits(List<MLSourceSplit> splits) {

	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {
		if(sourceEvent instanceof MLSourceEvent) {
			this.finished = ((MLSourceEvent) sourceEvent).getDone();
			LOG.info("receive event from SplitEnumerator and change finish status to: {}", this.finished);
			//System.out.println("receive event from SplitEnumerator and change finish status to:  " + this.finished);
		}
	}

	@Override
	public void close() throws Exception {
		this.closed = true;
	}
}
