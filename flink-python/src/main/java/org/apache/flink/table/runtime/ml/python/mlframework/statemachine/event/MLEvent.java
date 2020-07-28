package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;

public class MLEvent extends AbstractEvent<MLEventType> {
	private Object message;
	private long version;

	public MLEvent(MLEventType mlEventType, Object message, long version) {
		super(mlEventType);
		this.message = message;
		this.version = version;
	}

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}
}
