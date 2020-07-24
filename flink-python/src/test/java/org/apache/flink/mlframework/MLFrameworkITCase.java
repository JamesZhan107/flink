package org.apache.flink.mlframework;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.mlframework.operator.MLOperator;
import org.apache.flink.mlframework.operator.MLOperatorFactory;
import org.apache.flink.mlframework.source.MLSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class MLFrameworkITCase {
	@Test
	public void testCoordinate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromSource(new MLSource(Boundedness.CONTINUOUS_UNBOUNDED), WatermarkStrategy.noWatermarks(), "MLSource")
			.setParallelism(1)
			.transform("WorkOperator", Types.INT, new MLOperatorFactory(new MLOperator("worker")))
			.setParallelism(3)
			.print();

		env.fromSource(new MLSource(Boundedness.CONTINUOUS_UNBOUNDED), WatermarkStrategy.noWatermarks(), "MLSource")
			.setParallelism(1)
			.transform("PsOperator", Types.INT, new MLOperatorFactory(new MLOperator("ps")))
			.setParallelism(2)
			.print();

		env.execute();
	}
}

