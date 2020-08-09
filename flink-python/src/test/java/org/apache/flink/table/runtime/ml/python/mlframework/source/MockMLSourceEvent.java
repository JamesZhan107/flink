package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;

public class MockMLSourceEvent implements SourceEvent {
	private boolean done;

	public MockMLSourceEvent(boolean done) {
		this.done = done;
	}

	public boolean getDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}
}
