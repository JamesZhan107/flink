package org.apache.flink.table.runtime.ml.python.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

public class OperatorRegisterEvent implements OperatorEvent {
	private static final long serialVersionUID = 1L;

	private final String name;
	private final String address;

	public OperatorRegisterEvent(String name, String address) {
		this.name = name;
		this.address = address;
	}

	public String getName() {
		return name;
	}

	public String getAddress() {
		return address;
	}
}
