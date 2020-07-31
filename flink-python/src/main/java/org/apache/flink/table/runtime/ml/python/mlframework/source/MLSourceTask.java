package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;

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

			WorkStopEvent workStopEvent;
			try{
				workStopEvent = mlMeta.workStopEventQueue.take();
				handle(workStopEvent);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void handle(WorkStopEvent workStopEvent) {
		boolean isStop = workStopEvent.isStop();
		if(isStop) {
			// TODO: 发送停止信号前，应该等reader注册完毕，不然reader不会收到停止消息
			// 但此处肯定已经注册，不然作业不会开始

			for(int i = 0; i < enumContext.registeredReaders().size(); i++) {
				enumContext.sendEventToSourceReader(i, new MLSourceEvent(true));
			}

			isRunning = false;
		}
	}
}
