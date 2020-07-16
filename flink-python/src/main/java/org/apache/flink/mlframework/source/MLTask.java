package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.mlframework.coordinator.MLPublicArgs;

public class MLTask implements Runnable{

	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean isRunning;
	private MLPublicArgs mlPublicArgs;

	public MLTask(SplitEnumeratorContext<MLSourceSplit> enumContext, MLPublicArgs mlPublicArgs) {
		this.enumContext = enumContext;
		this.isRunning = true;
		this.mlPublicArgs = mlPublicArgs;
	}

	@Override
	public void run() {
		while (isRunning) {
			try {
				Thread.sleep(5);
				if(MLPublicArgs.isWorkDone()) {
					System.out.println("work finished");
					for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
						enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
					}
					isRunning = false;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
