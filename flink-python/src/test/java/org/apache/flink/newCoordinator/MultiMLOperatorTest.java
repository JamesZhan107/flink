package org.apache.flink.newCoordinator;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mlframework.operator.MLOperator;
import org.apache.flink.mlframework.operator.MLOperatorFactory;
import org.apache.flink.mlframework.source.MLSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class MultiMLOperatorTest {
	@Test
	public void testCoordinate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		env.fromElements(1, 2, 3)
//			.transform("WorkOperator", Types.INT, new MLOperatorFactory(new MLOperator("work")))
//			.setParallelism(3)
//			.print();
//
//		env.fromElements(4,5)
//			.transform("PsOperator", Types.INT, new MLOperatorFactory(new MLOperator("ps")))
//			.setParallelism(2)
//			.print();

		env.fromSource(new MLSource(Boundedness.CONTINUOUS_UNBOUNDED), WatermarkStrategy.noWatermarks(), "MLSource")
			//.setParallelism(1)
			.print();
		env.execute();
	}
}

