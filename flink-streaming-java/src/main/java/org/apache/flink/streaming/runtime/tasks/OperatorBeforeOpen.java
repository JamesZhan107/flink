package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.CompletableFuture;

public interface OperatorBeforeOpen {
	CompletableFuture<Void> beforeOpen();
}
