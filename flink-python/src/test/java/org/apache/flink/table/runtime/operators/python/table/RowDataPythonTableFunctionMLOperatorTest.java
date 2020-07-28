package org.apache.flink.table.runtime.operators.python.table;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.utils.PassThroughPythonTableFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collection;
import java.util.HashMap;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;

/**
 * Tests for {@link RowDataPythonTableFunctionMLOperator}.
 */
public class RowDataPythonTableFunctionMLOperatorTest
	extends PythonTableFunctionOperatorTestBase<RowData, RowData, RowData> {

	private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(new LogicalType[]{
		DataTypes.STRING().getLogicalType(),
		DataTypes.STRING().getLogicalType(),
		DataTypes.BIGINT().getLogicalType(),
		DataTypes.BIGINT().getLogicalType()
	});

	@Override
	public RowData newRow(boolean accumulateMsg, Object... fields) {
		if (accumulateMsg) {
			return row(fields);
		} else {
			RowData row = row(fields);
			row.setRowKind(RowKind.DELETE);
			return row;
		}
	}

	@Override
	public void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		assertor.assertOutputEquals(message, expected, actual);
	}

	@Override
	public AbstractPythonTableFunctionOperator<RowData, RowData, RowData> getTestOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		JoinRelType joinRelType) {
		return new RowDataPassThroughPythonTableFunctionOperator(
			config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
	}

	private static class RowDataPassThroughPythonTableFunctionOperator extends RowDataPythonTableFunctionMLOperator {

		RowDataPassThroughPythonTableFunctionOperator(
			Configuration config,
			PythonFunctionInfo tableFunction,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets,
			JoinRelType joinRelType) {
			super(config, tableFunction, inputType, outputType, udfInputOffsets, joinRelType);
		}

		@Override
		public PythonFunctionRunner createPythonFunctionRunner() {
			return new PassThroughPythonTableFunctionRunner(
				getRuntimeContext().getTaskName(),
				PythonTestUtils.createTestEnvironmentManager(),
				userDefinedFunctionInputType,
				userDefinedFunctionOutputType,
				getFunctionUrn(),
				getUserDefinedFunctionsProto(),
				getInputOutputCoderUrn(),
				new HashMap<>(),
				PythonTestUtils.createMockFlinkMetricContainer());
		}
	}
}
