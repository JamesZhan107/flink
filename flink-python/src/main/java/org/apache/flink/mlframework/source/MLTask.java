package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.mlframework.coordinator.MLPublicArgs;

public class MLTask implements Runnable{

	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean isrunning;
	private MLPublicArgs mlPublicArgs;

	public MLTask(SplitEnumeratorContext<MLSourceSplit> enumContext, MLPublicArgs mlPublicArgs) {
		this.enumContext = enumContext;
		this.isrunning = true;
		this.mlPublicArgs = mlPublicArgs;
	}

	@Override
	public void run() {
		while (isrunning){
			try {
				Thread.sleep(10);
				if(MLPublicArgs.isWorkDone()) {
					System.out.println("work finished");
					for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
						enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
					}
					isrunning = false;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
