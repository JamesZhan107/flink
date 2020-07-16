package org.apache.flink.mlframework.statemachine;

import org.apache.flink.mlframework.statemachine.event.MLEvent;
import org.apache.flink.mlframework.statemachine.event.MLEventType;
import org.apache.flink.mlframework.statemachine.transition.MLTransitions;
import org.apache.flink.mlframework.statemachine.transition.TFTransitions;

import java.util.EnumSet;

public class TFMLStateMachineImpl extends AbstractMLStateMachine {

	public TFMLStateMachineImpl() {
		super();
	}

	@Override
	protected StateMachine<AMStatus, MLEventType, MLEvent> buildStateMachine() {
		StateMachineBuilder<AbstractMLStateMachine, AMStatus, MLEventType, MLEvent>
			stateMachineBuilder = new StateMachineBuilder<AbstractMLStateMachine, AMStatus, MLEventType, MLEvent>(
			AMStatus.AM_UNKNOW)
			.addTransition(AMStatus.AM_UNKNOW,
				EnumSet.of(AMStatus.AM_INIT, AMStatus.AM_RUNNING, AMStatus.AM_FAILOVER, AMStatus.AM_FINISH),
				MLEventType.INTI_AM_STATE,
				new MLTransitions.InitAmState(this))
			.addTransition(AMStatus.AM_INIT, AMStatus.AM_INIT,
				MLEventType.REGISTER_NODE,
				new TFTransitions.RegisterNode(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_RUNNING, MLEventType.COMPLETE_CLUSTER,
//				new AMTransitions.CompleteCluster(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_FAILOVER, MLEventType.FAIL_NODE,
//				new AMTransitions.FailNode(this))
//			.addTransition(AMStatus.AM_INIT, AMStatus.AM_FINISH, MLEventType.STOP_JOB,
//				new AMTransitions.StopJob(this))
//			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_RUNNING,
//				MLEventType.FINISH_NODE,
//				new TFTransitions.FinishNode(this))
//			.addTransition(AMStatus.AM_RUNNING, AMStatus.AM_FINISH,
//				MLEventType.FINISH_CLUSTER,
//				new AMTransitions.FinishCluster(this))
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
