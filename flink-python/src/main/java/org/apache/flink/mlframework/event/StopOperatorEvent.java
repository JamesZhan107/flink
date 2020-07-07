package org.apache.flink.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class StopOperatorEvent implements OperatorEvent {
	private boolean state;

	public StopOperatorEvent(boolean state) {
		this.state = state;
	}

	public boolean getCluster() {
		return state;
	}
}
