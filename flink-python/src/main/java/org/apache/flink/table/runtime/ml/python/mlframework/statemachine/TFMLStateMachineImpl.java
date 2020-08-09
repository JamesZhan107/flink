package org.apache.flink.table.runtime.ml.python.mlframework.statemachine;

import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.AMStatus;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEventType;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.transition.MLTransitions;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.transition.TFTransitions;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.List;

public class TFMLStateMachineImpl extends AbstractMLStateMachine {
	private static TFMLStateMachineImpl tfmlStateMachine;

	private TFMLStateMachineImpl(MLMeta mlMeta, List<OperatorCoordinator.Context> contextList) {
		super(mlMeta, contextList);
	}

	public static TFMLStateMachineImpl getTFMLStateMachineImpl(MLMeta mlMeta, List<OperatorCoordinator.Context> contextList){
		if (tfmlStateMachine == null) {
			synchronized (TFMLStateMachineImpl.class) {
				if (tfmlStateMachine == null) {
					tfmlStateMachine = new TFMLStateMachineImpl(mlMeta, contextList);
				}
			}
		}
		return tfmlStateMachine;
	}
	@Override
	protected StateMachine<AMStatus, MLEventType, MLEvent> buildStateMachine() {
		StateMachineBuilder<AbstractMLStateMachine, AMStatus, MLEventType, MLEvent>
			stateMachineBuilder = new StateMachineBuilder<AbstractMLStateMachine, AMStatus, MLEventType, MLEvent>(
			AMStatus.AM_UNKNOW)
			.addTransition(AMStatus.AM_UNKNOW,
				AMStatus.AM_INIT,
				MLEventType.INTI_AM_STATE,
				new MLTransitions.InitAmState(this))
			.addTransition(AMStatus.AM_INIT, AMStatus.AM_INIT,
				MLEventType.REGISTER_NODE,
				new TFTransitions.RegisterNode(this))
			.addTransition(AMStatus.AM_INIT, AMStatus.AM_RUNNING,
				MLEventType.COMPLETE_CLUSTER,
				new MLTransitions.CompleteCluster(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_FAILOVER, MLEventType.FAIL_NODE,
//				new AMTransitions.FailNode(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_FINISH, MLEventType.STOP_JOB,
//				new AMTransitions.StopJob(this))
			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_RUNNING,
				MLEventType.FINISH_NODE,
				new TFTransitions.FinishNode(this))
			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_FINISH,
				MLEventType.FINISH_CLUSTER,
				new MLTransitions.FinishCluster(this))
//			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_FAILOVER, MLEventType.FAIL_NODE,
//				new AMTransitions.FailNode(this))
//			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_FAILOVER, MLEventType.REGISTER_NODE,
//				new AMTransitions.FailNode(this))
//			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_FINISH, MLEventType.STOP_JOB,
//				new AMTransitions.StopJob(this))
//			.addTransition(AMStatus.AM_FAILOVER, AMStatus.AM_INIT, MLEventType.RESTART_CLUSTER,
//				new AMTransitions.RestartCluster(this))
//			.addTransition(AMStatus.AM_FAILOVER, AMStatus.AM_FINISH, MLEventType.STOP_JOB,
//				new AMTransitions.StopJob(this))
//			// some ignore message
//			.addTransition(AMStatus.AM_FAILOVER, AMStatus.AM_FAILOVER, MLEventType.FINISH_NODE,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_FAILOVER, AMStatus.AM_FAILOVER, MLEventType.FAIL_NODE,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_FAILOVER, AMStatus.AM_FAILOVER, MLEventType.REGISTER_NODE,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_INIT, MLEventType.FINISH_NODE,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_INIT, MLEventType.RESTART_CLUSTER,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_FINISH, AMStatus.AM_FINISH, MLEventType.FINISH_NODE,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_FINISH, AMStatus.AM_FINISH, MLEventType.STOP_JOB,
//				new AMTransitions.IgnoreMessage(this))
//			.addTransition(AMStatus.AM_FINISH, AMStatus.AM_FINISH, MLEventType.FINISH_NODE,
//				new AMTransitions.IgnoreMessage(this))
			// end
			.installTopology();
		stateMachine = stateMachineBuilder.make(this);
		return stateMachine;
	}
}
