package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.AMStatus;
import org.apache.flink.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.mlframework.statemachine.event.MLEvent;

public class MLTransitions {
	public static class InitAmState extends Transition
		implements MultipleArcTransition<AbstractMLStateMachine, MLEvent, AMStatus> {

		public InitAmState(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public AMStatus transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			System.out.println("unknown to init");
			return AMStatus.AM_INIT;
		}
	}
}
