package org.apache.flink.table.runtime.ml.python.mlframework.statemachine;

import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;

public interface StateMachine
	<STATE extends Enum<STATE>,
		EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
	STATE getCurrentState();

	STATE doTransition(EVENTTYPE eventType, EVENT event)
            throws InvalidStateTransitionException, TaskNotRunningException;
}
