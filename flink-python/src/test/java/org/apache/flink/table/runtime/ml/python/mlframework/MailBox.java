package org.apache.flink.table.runtime.ml.python.mlframework;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MailBox implements Runnable{
	private List<CompletableFuture<Void>> future;

	@Override
	public void run() {
		while (true) {
			if(!isDone(future)) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("handle event");
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("handle data");
			}
		}
	}

	public boolean isDone(List<CompletableFuture<Void>> future){
		boolean flag = true;
		for(CompletableFuture<Void> f : future){
			flag = f.isDone() && flag;
		}
		return flag;
	}

	public List<CompletableFuture<Void>> getFuture() {
		return future;
	}

	public void setFuture(List<CompletableFuture<Void>> future) {
		this.future = future;
	}
}
