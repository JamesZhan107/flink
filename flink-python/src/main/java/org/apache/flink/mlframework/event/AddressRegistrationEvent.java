package org.apache.flink.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.net.InetSocketAddress;

public class AddressRegistrationEvent implements OperatorEvent {
	private static final long serialVersionUID = 1L;

	private final String name;
	private final InetSocketAddress address;

	public AddressRegistrationEvent(String name, InetSocketAddress address) {
		this.name = name;
		this.address = address;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public String getName() {
		return name;
	}
}
