package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;

public class MLEvent extends AbstractEvent<MLEventType> {
	private Object message;
	private String name;

	public MLEvent(MLEventType mlEventType, Object message, String name) {
		super(mlEventType);
		this.message = message;
		this.name = name;
	}

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
