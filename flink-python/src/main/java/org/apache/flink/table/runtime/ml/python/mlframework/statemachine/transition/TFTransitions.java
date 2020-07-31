package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.transition;

import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.InvalidStateTransitionException;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEventType;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;

import java.net.InetSocketAddress;

public class TFTransitions {

	public static class RegisterNode extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public RegisterNode(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent)
			throws InvalidStateTransitionException {
			System.out.println("register node");
			InetSocketAddress address = (InetSocketAddress) mlEvent.getMessage();
			String clusterInfo = mlMeta.getClusterInfo();
			clusterInfo += address.toString();
			mlMeta.setClusterInfo(clusterInfo);
			int nodeNum = mlMeta.getNodeNum();
			nodeNum++;
			mlMeta.setNodeNum(nodeNum);
			if (nodeNum == 5) {
				stateMachine.sendEvent(new MLEvent(MLEventType.COMPLETE_CLUSTER, clusterInfo, 1));
			}
		}
	}

	public static class FinishNode extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public FinishNode(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent)
			throws InvalidStateTransitionException {
			System.out.println("finish node");
			int nodeNum = mlMeta.getNodeNum();
			nodeNum--;
			System.out.println("nodenum:  " + nodeNum);
			mlMeta.setNodeNum(nodeNum);
			if(nodeNum == 2){
				try {
					mlMeta.workStopEventQueue.put(new WorkStopEvent(true));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				stateMachine.sendEvent(new MLEvent(MLEventType.FINISH_CLUSTER, "finished", 1));
			}
		}
	}

}
