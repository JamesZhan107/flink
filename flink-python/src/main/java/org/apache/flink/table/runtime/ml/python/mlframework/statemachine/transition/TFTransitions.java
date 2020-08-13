package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.transition;

import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.AbstractMLStateMachine;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEventType;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.util.JsonUtil;

import java.util.ArrayList;
import java.util.HashMap;

public class TFTransitions {

	public static class RegisterNode extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public RegisterNode(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			String address = mlEvent.getMessage().toString();
			String name = mlEvent.getName();
			HashMap<String, ArrayList<String>> clusterInfo = mlMeta.getClusterInfo();
			ArrayList<String> list;
			list = clusterInfo.containsKey(name) ? clusterInfo.get(name) : new ArrayList<>();
			list.add(address);
			clusterInfo.put(name, list);
			mlMeta.setClusterInfo(clusterInfo);
			int registerNodeNum = mlMeta.getRegisterNodeNum();
			registerNodeNum++;
			mlMeta.setRegisterNodeNum(registerNodeNum);
			if (registerNodeNum == mlMeta.getConfigNodeNum()) {
				mlMeta.setLastNodeNum(mlMeta.getRegisterNodeNum());
				stateMachine.sendEvent(new MLEvent(MLEventType.COMPLETE_CLUSTER, JsonUtil.toJSONString(clusterInfo), ""));
			}
		}
	}

	public static class FinishNode extends Transition
		implements SingleArcTransition<AbstractMLStateMachine, MLEvent> {

		public FinishNode(AbstractMLStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractMLStateMachine amStateMachine, MLEvent mlEvent) {
			int lastNodeNum = mlMeta.getLastNodeNum();
			lastNodeNum--;
			mlMeta.setLastNodeNum(lastNodeNum);
			//if(lastNodeNum == 2){
			if(lastNodeNum == mlMeta.nodeNumMap.getOrDefault("ps", 0)){
				try {
					mlMeta.workStopEventQueue.put(new WorkStopEvent(true));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				stateMachine.sendEvent(new MLEvent(MLEventType.FINISH_CLUSTER, "finished", ""));
			}
		}
	}

}
