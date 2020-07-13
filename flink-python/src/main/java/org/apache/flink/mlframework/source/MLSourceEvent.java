package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SourceEvent;

public class MLSourceEvent implements SourceEvent {
	private boolean done;

	public MLSourceEvent(boolean done) {
		this.done = done;
	}

	public boolean getDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}
}
