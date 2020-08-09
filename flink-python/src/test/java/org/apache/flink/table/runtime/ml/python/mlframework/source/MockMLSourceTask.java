package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockMLSourceTask implements Runnable{

	private final SplitEnumeratorContext<MockMLSourceSplit> enumContext;
	private boolean isRunning;
	private final MLMeta mlMeta;
	protected static final Logger LOG = LoggerFactory.getLogger(MockMLSourceTask.class);

	public MockMLSourceTask(SplitEnumeratorContext<MockMLSourceSplit> enumContext, MLMeta mlMeta) {
		this.enumContext = enumContext;
		this.isRunning = true;
		this.mlMeta = mlMeta;
	}

	@Override
	public void run() {
		while (isRunning) {
			try{
				WorkStopEvent workStopEvent = mlMeta.workStopEventQueue.take();
				handle(workStopEvent);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void handle(WorkStopEvent workStopEvent) {
		boolean isStop = workStopEvent.isStop();
		if(isStop) {
			LOG.info("enumerator send stop event to reader");
			for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
				enumContext.sendEventToSourceReader(i, new MockMLSourceEvent(true));
			}
			isRunning = false;
		}
	}
}
