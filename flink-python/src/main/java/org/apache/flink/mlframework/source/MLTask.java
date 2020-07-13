package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

public class MLTask implements Runnable{

	private SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean isrunning;

	public MLTask(SplitEnumeratorContext<MLSourceSplit> enumContext) {
		this.enumContext = enumContext;
		this.isrunning = true;
	}

	@Override
	public void run() {
		while (isrunning){
			try {
				System.out.println("run");
				Thread.sleep(30000);
				for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
					enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			isrunning = false;
		}
	}
}
