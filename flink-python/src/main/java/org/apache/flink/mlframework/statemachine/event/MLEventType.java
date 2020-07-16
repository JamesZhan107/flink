package org.apache.flink.mlframework.statemachine.event;

public enum MLEventType {
	INTI_AM_STATE,
	REGISTER_NODE,
	COMPLETE_CLUSTER,
	FINISH_NODE,
	FINISH_CLUSTER,
	FAIL_NODE,
	FAILED_CLUSTER,
	RESTART_CLUSTER,
	STOP_JOB
}
