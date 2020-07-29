package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;

public class MLSourceTask implements Runnable{

	private final SplitEnumeratorContext<MLSourceSplit> enumContext;
	private boolean isRunning;
	private final MLMeta mlMeta;

	public MLSourceTask(SplitEnumeratorContext<MLSourceSplit> enumContext, MLMeta mlMeta) {
		this.enumContext = enumContext;
		this.isRunning = true;
		this.mlMeta = mlMeta;
	}

	@Override
	public void run() {
		while (isRunning) {
//			try {
//				Thread.sleep(5);
//				// TODO : queue
//				if(mlMeta.isworkStop()) {
//					System.out.println("work finished");
//					//stop the reader
//					for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
//						enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
//					}
//					isRunning = false;
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}

			try {
				Thread.sleep(10000);
				System.out.println("work finished");
				//stop the reader
				for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
					enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
				}
				isRunning = false;

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
