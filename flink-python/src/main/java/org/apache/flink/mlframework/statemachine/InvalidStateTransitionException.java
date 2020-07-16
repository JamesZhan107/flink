package org.apache.flink.mlframework.statemachine;

public class InvalidStateTransitionException extends Exception {

	private static final long serialVersionUID = 8610511635996283691L;

	private Object currentState;
	private Object event;

	public InvalidStateTransitionException(Object currentState, Object event) {
		super("Invalid event: " + event + " at " + currentState);
		this.currentState = currentState;
		this.event = event;
	}

	public Object getCurrentState() {
		return currentState;
	}

	public Object getEvent() {
		return event;
	}

	@Override
	public String toString() {
		return "InvalidStateTransitionException{" +
			"currentState=" + currentState +
			", event=" + event +
			'}';
	}
}
