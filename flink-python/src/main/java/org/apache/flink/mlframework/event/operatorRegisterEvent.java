package org.apache.flink.mlframework.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.net.InetSocketAddress;

public class operatorRegisterEvent implements OperatorEvent {
	private static final long serialVersionUID = 1L;

	private final String name;
	private final String ip;
	private final int port;

	public operatorRegisterEvent(String name, String ip, int port) {
		this.name = name;
		this.ip = ip;
		this.port = port;
	}

	public String getName() {
		return name;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}
}
