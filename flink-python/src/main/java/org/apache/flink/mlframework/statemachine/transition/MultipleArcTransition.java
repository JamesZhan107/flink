package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.InvalidStateTransitionException;

public interface MultipleArcTransition
	<OPERAND, EVENT, STATE extends Enum<STATE>> {

	/**
	 * Transition hook.
	 *
	 * @param operand the entity attached to the FSM, whose internal
	 * state may change.
	 * @param event causal event
	 * @return the postState. Post state must be one of the
	 * valid post states registered in StateMachine.
	 */
	STATE transition(OPERAND operand, EVENT event) throws InvalidStateTransitionException;

}
