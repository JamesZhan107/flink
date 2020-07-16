package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.mlframework.statemachine.InvalidStateTransitionException;
import org.apache.flink.mlframework.statemachine.event.MLEvent;

public class TFTransitions {

	public static class RegisterNode extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public RegisterNode(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent)
			throws InvalidStateTransitionException {
			System.out.println("init to init");
		}
	}

}
