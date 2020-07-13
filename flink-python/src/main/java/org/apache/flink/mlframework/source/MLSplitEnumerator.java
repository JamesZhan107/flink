package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.*;

public class MLSplitEnumerator implements SplitEnumerator<MLSourceSplit, Set<MLSourceSplit>> {
	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean started;
	private boolean closed;

	public MLSplitEnumerator(SplitEnumeratorContext<MLSourceSplit> enumContext) {
		System.out.println("Construct Enumerator");
		this.enumContext = enumContext;
		this.started = false;
		this.closed = false;
	}

	@Override
	public void start() {
		this.started = true;
		System.out.println("start new Thread");
		Thread task = new Thread(new MLTask(enumContext));
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
