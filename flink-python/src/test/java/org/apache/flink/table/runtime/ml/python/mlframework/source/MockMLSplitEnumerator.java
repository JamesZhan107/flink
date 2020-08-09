package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta.getMlMeta;


public class MockMLSplitEnumerator implements SplitEnumerator<MockMLSourceSplit, Set<MockMLSourceSplit>> {
	private static final MockMLSplitEnumerator mlSplitEnumerator = new MockMLSplitEnumerator();
	private SplitEnumeratorContext<MockMLSourceSplit> enumContext;
	private boolean started;
	private boolean closed;
	private MLMeta mlMeta;

	protected static final Logger LOG = LoggerFactory.getLogger(MockMLSplitEnumerator.class);

	private MockMLSplitEnumerator() {
		//LOG.info("Construct Enumerator");
		System.out.println("Construct Enumerator");
		this.started = false;
		this.closed = false;
	}

	public static MockMLSplitEnumerator getMlSplitEnumerator(SplitEnumeratorContext<MockMLSourceSplit> enumContext){
		mlSplitEnumerator.enumContext = enumContext;
		mlSplitEnumerator.mlMeta = getMlMeta();
		return mlSplitEnumerator;
	}

	@Override
	public void start() {
		this.started = true;
		Thread task = new Thread(new MockMLSourceTask(enumContext, mlMeta));
		task.start();
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {

	}

	@Override
	public void addSplitsBack(List<MockMLSourceSplit> splits, int subtaskId) {

	}

	@Override
	public void addReader(int subtaskId) {

	}

	@Override
	public Set<MockMLSourceSplit> snapshotState() throws Exception {
		return null;
	}

	@Override
	public void close() throws IOException {
		this.closed = true;
	}

}
