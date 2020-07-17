package org.apache.flink.mlframework.statemachine.transition;

import org.apache.flink.mlframework.event.ClusterInfoEvent;
import org.apache.flink.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.mlframework.statemachine.InvalidStateTransitionException;
import org.apache.flink.mlframework.statemachine.event.MLEvent;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;

import java.io.IOException;

public class MLTransitions {

	public static class InitAmState extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public InitAmState(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			System.out.println("unknown to init");
		}
	}

	public static class CompleteCluster extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent>{

		public CompleteCluster(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine AbstractMLStateMachine, MLEvent mlEvent)
			throws InvalidStateTransitionException, TaskNotRunningException {
			System.out.println("running");
			String clusterInfo = mlMeta.getClusterInfo();
			int nodeNum = mlMeta.getNodeNum();
			if(nodeNum == 5){
				for (int i = 0; i < contextList.size(); ++i) {
					OperatorCoordinator.Context context = contextList.get(i);
					for (int j = 0; j < context.currentParallelism(); j++) {
						context.sendEvent(new ClusterInfoEvent(clusterInfo), j);
					}
				}
			}
		}
	}

	public static class FinishCluster extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public FinishCluster(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			System.out.println("finish");
		}
	}
}
