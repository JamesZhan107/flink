package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.mlframework.coordinator.MLPublicArgs;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.mlframework.coordinator.MLPublicArgs.getMlPublicArgs;


public class MLSplitEnumerator implements SplitEnumerator<MLSourceSplit, Set<MLSourceSplit>> {
	private static final MLSplitEnumerator mlSplitEnumerator = new MLSplitEnumerator();
	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean started;
	private boolean closed;
	private MLPublicArgs mlPublicArgs;

	private MLSplitEnumerator() {
		System.out.println("Construct Enumerator");
		this.started = false;
		this.closed = false;
		this.mlPublicArgs = getMlPublicArgs();
	}

	public static MLSplitEnumerator getMlSplitEnumerator(SplitEnumeratorContext<MLSourceSplit> enumContext){
		mlSplitEnumerator.enumContext = enumContext;
		return mlSplitEnumerator;
	}

	public MLSplitEnumerator(SplitEnumeratorContext<MLSourceSplit> enumContext) {
		System.out.println("Construct Enumerator");
		this.enumContext = enumContext;
		this.started = false;
		this.closed = false;
		this.mlPublicArgs = getMlPublicArgs();
	}

	@Override
	public void start() {
		this.started = true;
		Thread task = new Thread(new MLTask(enumContext, mlPublicArgs));
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
