package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;

/**
 * abstract state machine event.
 * @param <TYPE> event type
 */
public abstract class AbstractEvent<TYPE extends Enum<TYPE>>
	implements Event<TYPE> {

	private final TYPE type;
	private final long timestamp;

	// use this if you DON'T care about the timestamp
	public AbstractEvent(TYPE type) {
		this.type = type;
		// We're not generating a real timestamp here.  It's too expensive.
		timestamp = -1L;
	}

	// use this if you care about the timestamp
	public AbstractEvent(TYPE type, long timestamp) {
		this.type = type;
		this.timestamp = timestamp;
	}

	/**
	 * @return event timestamp
	 */
	@Override
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @return event type.
	 */
	@Override
	public TYPE getType() {
		return type;
	}

	@Override
	public String toString() {
		return "EventType: " + getType();
	}
}
