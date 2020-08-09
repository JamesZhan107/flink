package org.apache.flink.table.runtime.ml.python.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class NodeFinishEvent implements OperatorEvent {
	private final boolean state;
	private String name;

	public NodeFinishEvent(boolean state, String name) {
		this.state = state;
		this.name = name;
	}

	public boolean getCluster() {
		return state;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
