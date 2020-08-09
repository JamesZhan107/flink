package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.transition;

import org.apache.flink.table.runtime.ml.python.mlframework.event.ClusterInfoEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.InvalidStateTransitionException;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEvent;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;

public class MLTransitions {

	public static class InitAmState extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public InitAmState(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {

		}
	}

	public static class CompleteCluster extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent>{

		public CompleteCluster(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine AbstractMLStateMachine, MLEvent mlEvent)
			throws TaskNotRunningException {
			String clusterInfo = mlEvent.getMessage().toString();
			for (int i = 0; i < contextList.size(); ++i) {
				OperatorCoordinator.Context context = contextList.get(i);
				for (int j = 0; j < context.currentParallelism(); j++) {
					context.sendEvent(new ClusterInfoEvent(clusterInfo), j);
				}
			}
			mlMeta.setWorkStart(true);
//			try {
//				Thread.sleep(30000);
//				mlMeta.workStopEventQueue.put(new WorkStopEvent(true));
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}
	}

	public static class FinishCluster extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public FinishCluster(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			//System.out.println("finish");
		}
	}
}
