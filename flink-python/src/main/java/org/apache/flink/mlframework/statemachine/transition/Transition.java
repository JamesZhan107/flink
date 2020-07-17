package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.statemachine.MLMeta;
import org.apache.flink.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.List;

public class Transition {
	protected final MLMeta mlMeta;
	protected final AbstractMLStateMachine stateMachine;
	protected final List<OperatorCoordinator.Context> contextList;

	public Transition(AbstractMLStateMachine stateMachine) {
		this.stateMachine = stateMachine;
		this.mlMeta = stateMachine.getMlMeta();
		this.contextList = stateMachine.getContextList();
	}
}
