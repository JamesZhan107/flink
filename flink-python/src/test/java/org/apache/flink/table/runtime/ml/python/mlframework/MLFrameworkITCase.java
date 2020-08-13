package org.apache.flink.table.runtime.ml.python.mlframework;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.runtime.ml.python.mlframework.operator.MLOperator;
import org.apache.flink.table.runtime.ml.python.mlframework.operator.MLOperatorFactory;
import org.apache.flink.table.runtime.ml.python.mlframework.source.MockMLSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MLFrameworkITCase {
	@Test
	public void testCoordinate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromSource(new MockMLSource(Boundedness.CONTINUOUS_UNBOUNDED), WatermarkStrategy.noWatermarks(), "MLSource")
			.setParallelism(1)
			.transform("WorkOperator", Types.INT, new MLOperatorFactory(new MLOperator("worker")))
			.setParallelism(2)
			.print();

		env.fromSource(new MockMLSource(Boundedness.CONTINUOUS_UNBOUNDED), WatermarkStrategy.noWatermarks(), "MLSource")
			.setParallelism(1)
			.transform("PsOperator", Types.INT, new MLOperatorFactory(new MLOperator("ps")))
			.setParallelism(1)
			.print();

		env.execute();
	}

	@Test
	public void futureTset() {
		List<CompletableFuture<Void>> future = new ArrayList<>();

		MockOperatorChain mockOperatorChain = new MockOperatorChain();
		mockOperatorChain.setFuture(future);

		Operator op = new Operator();
		mockOperatorChain.initial(op);

		try {
			MailBox mailBox = new MailBox();
			mailBox.setFuture(future);
			Thread thread = new Thread(mailBox);
			thread.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		op.handleEvent();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}

class Operator {
	CompletableFuture<Void> future = new CompletableFuture<>();

	public CompletableFuture<Void> beforeOpen(){
		System.out.println("before open");
		return future;
	}

	public void handleEvent(){
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("event done");
		future.complete(null);
	}
}

