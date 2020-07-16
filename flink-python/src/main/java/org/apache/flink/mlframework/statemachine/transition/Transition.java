package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.AbstractMLStateMachine;

public class Transition {
	protected final AbstractMLStateMachine stateMachine;

	public Transition(AbstractMLStateMachine stateMachine) {
		this.stateMachine = stateMachine;
	}
}
