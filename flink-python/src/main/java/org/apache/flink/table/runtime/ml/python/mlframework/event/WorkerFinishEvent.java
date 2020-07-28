package org.apache.flink.table.runtime.ml.python.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class WorkerFinishEvent implements OperatorEvent {
	private boolean state;

	public WorkerFinishEvent(boolean state) {
		this.state = state;
	}

	public boolean getCluster() {
		return state;
	}
}
