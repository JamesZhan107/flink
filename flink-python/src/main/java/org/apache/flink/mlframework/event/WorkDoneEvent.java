package org.apache.flink.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class WorkDoneEvent implements OperatorEvent {
	private boolean state;

	public WorkDoneEvent(boolean state) {
		this.state = state;
	}

	public boolean getCluster() {
		return state;
	}
}
