package org.apache.flink.newCoordinator;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.mlframework.operator.MLOperator;
import org.apache.flink.mlframework.operator.MLOperatorFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class OneMLOperatorTest {
	@Test
	public void testCoordinate() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		env.fromElements(1, 2, 3, 4)
			.transform("coordinateTestOperator", Types.INT, new MLOperatorFactory(new MLOperator("node")))
			.print();

		env.execute();
	}
}
