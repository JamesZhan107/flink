package org.apache.flink.table.runtime.ml.python.mlframework;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class TableSourceTest {

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		tEnv.executeSql(
			"CREATE TABLE mlTable (" +
				"num1 INT,\n" +
				"num2 INT\n" +
				")\n" +
				"WITH (\n"+
				String.format("'%s'='%s'\n", "connector", "ml") + ")"
		);

		tEnv.executeSql(
			"CREATE TABLE ml2Table (" +
				"num3 INT,\n" +
				"num4 INT\n" +
				")\n" +
				"WITH (\n"+
				String.format("'%s'='%s'\n", "connector", "ml") + ")"
		);

		Table t = tEnv.from("mlTable").select("num1, num2");
		Table t2 = tEnv.from("ml2Table").select("num3, num4");

		tEnv.toAppendStream(t, Order.class).print();
		tEnv.toAppendStream(t2, Order2.class).print();

		env.execute();

	}

	public static class Order {
		public int num1;
		public int num2;

		public Order() {
		}

		public Order(int num1, int num2) {
			this.num1 = num1;
			this.num2 = num2;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + num1 +
				", amount=" + num2 +
				'}';
		}
	}

	public static class Order2 {
		public int num3;
		public int num4;

		public Order2() {
		}

		public Order2(int num1, int num2) {
			this.num3 = num1;
			this.num4 = num2;
		}

		@Override
		public String toString() {
			return "Order{" +
				"user=" + num3 +
				", amount=" + num4 +
				'}';
		}
	}
}

