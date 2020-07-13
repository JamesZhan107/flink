package org.apache.flink.mlframework.operator;

import org.apache.flink.mlframework.event.WorkDoneEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;

public class MLOperatorTask implements Runnable{
	private boolean isRunning;
	private String name;
	private OperatorEventGateway eventGateway;

	public MLOperatorTask(String name, OperatorEventGateway eventGateway) {
		this.name = name;
		this.eventGateway = eventGateway;
		this.isRunning = true;
	}

	@Override
	public void run() {
		if(name.equals("worker")) {
			while (isRunning) {
				try {
					System.out.println("worker thread running");
					Thread.sleep(10000);
					eventGateway.sendEventToCoordinator(new WorkDoneEvent(true));
					isRunning = false;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}else {
			while (isRunning) {
				try {
					System.out.println("ps thread running");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
