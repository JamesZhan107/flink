package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.InvalidStateTransitionException;

public interface SingleArcTransition<OPERAND, EVENT> {
	/**
	 * Transition hook.
	 *
	 * @param operand the entity attached to the FSM, whose internal
	 * state may change.
	 * @param event causal event
	 */
	void transition(OPERAND operand, EVENT event) throws InvalidStateTransitionException;

}
