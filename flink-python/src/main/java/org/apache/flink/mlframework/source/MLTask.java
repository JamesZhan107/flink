package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.mlframework.statemachine.MLMeta;

public class MLTask implements Runnable{

	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean isRunning;
	private MLMeta mlMeta;

	public MLTask(SplitEnumeratorContext<MLSourceSplit> enumContext, MLMeta mlMeta) {
		this.enumContext = enumContext;
		this.isRunning = true;
		this.mlMeta = mlMeta;
	}

	@Override
	public void run() {
		while (isRunning) {
			try {
				Thread.sleep(5);
				if(MLMeta.isWorkDone()) {
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
