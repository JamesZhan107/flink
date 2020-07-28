package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta.getMlMeta;


public class MLSplitEnumerator implements SplitEnumerator<MLSourceSplit, Set<MLSourceSplit>> {
	private static final MLSplitEnumerator mlSplitEnumerator = new MLSplitEnumerator();
	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean started;
	private boolean closed;
	private MLMeta mlMeta;

	private MLSplitEnumerator() {
		System.out.println("Construct Enumerator");
		this.started = false;
		this.closed = false;
	}

	public static MLSplitEnumerator getMlSplitEnumerator(SplitEnumeratorContext<MLSourceSplit> enumContext){
		mlSplitEnumerator.enumContext = enumContext;
		mlSplitEnumerator.mlMeta = getMlMeta();
		return mlSplitEnumerator;
	}

	@Override
	public void start() {
		this.started = true;
		Thread task = new Thread(new MLSourceTask(enumContext, mlMeta));
		task.start();
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {

	}

	@Override
	public void addSplitsBack(List<MLSourceSplit> splits, int subtaskId) {

	}

	@Override
	public void addReader(int subtaskId) {

	}

	@Override
	public Set<MLSourceSplit> snapshotState() throws Exception {
		return null;
	}

	@Override
	public void close() throws IOException {
		this.closed = true;
	}

}
