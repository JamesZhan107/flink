package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;


public class WorkStopEvent  {
	private boolean stop;

	public WorkStopEvent(boolean stop) {
		this.stop = stop;
	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}
}
