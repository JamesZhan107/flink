package org.apache.flink.table.runtime.ml.python.mlframework.operator;

import org.apache.flink.table.runtime.ml.python.mlframework.event.NodeFinishEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;

public class MLOperatorTask implements Runnable{
	private String name;
	private OperatorEventGateway eventGateway;
	private boolean isRunning = true;

	public MLOperatorTask(String name, OperatorEventGateway eventGateway) {
		this.name = name;
		this.eventGateway = eventGateway;
	}

	@Override
	public void run() {
		if(name.equals("worker")) {
			while (isRunning) {
				try {
					System.out.println("worker thread running");
					Thread.sleep(10000);
					eventGateway.sendEventToCoordinator(new NodeFinishEvent(true, "worker"));
					isRunning = false;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} else {
			while (isRunning) {
				try {
					System.out.println("ps thread running");
					Thread.sleep(1000);
					//isRunning = false;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
