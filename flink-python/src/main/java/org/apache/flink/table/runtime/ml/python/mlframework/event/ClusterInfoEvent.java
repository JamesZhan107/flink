package org.apache.flink.table.runtime.ml.python.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class ClusterInfoEvent implements OperatorEvent {
	private static final long serialVersionUID = 1L;

	private final String cluster;

	public ClusterInfoEvent(String cluster) {
		this.cluster = cluster;
	}

	public String getCluster() {
		return cluster;
	}
}
