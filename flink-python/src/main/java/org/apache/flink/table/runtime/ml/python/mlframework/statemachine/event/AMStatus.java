package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;

public enum AMStatus {
	AM_UNKNOW,
	AM_INIT,
	AM_FAILOVER,
	AM_RUNNING,
	AM_FINISH
}
