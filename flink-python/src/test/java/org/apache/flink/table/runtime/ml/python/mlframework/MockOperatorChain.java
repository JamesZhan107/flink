package org.apache.flink.table.runtime.ml.python.mlframework;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockOperatorChain{
	private List<CompletableFuture<Void>> future;

	public void initial(Operator before){
		CompletableFuture<Void> f = before.beforeOpen();
		future.add(f);
	}

	public List<CompletableFuture<Void>> getFuture() {
		return future;
	}

	public void setFuture(List<CompletableFuture<Void>> future) {
		this.future = future;
	}
}
