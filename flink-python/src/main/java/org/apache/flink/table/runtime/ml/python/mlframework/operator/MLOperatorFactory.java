package org.apache.flink.table.runtime.ml.python.mlframework.operator;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.ml.python.mlframework.coordinator.MLCoordinatorProvider;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.table.runtime.operators.python.table.RowDataPythonTableFunctionMLOperator;

public class MLOperatorFactory extends AbstractStreamOperatorFactory<RowData> implements CoordinatedOperatorFactory<RowData> {

	RowDataPythonTableFunctionMLOperator mlOperator;

	public MLOperatorFactory(RowDataPythonTableFunctionMLOperator mlOperator) {
		this.mlOperator = mlOperator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		return new MLCoordinatorProvider(operatorID);
	}

	@Override
	public <T extends StreamOperator<RowData>> T createStreamOperator(StreamOperatorParameters<RowData> parameters) {
		final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
		mlOperator.setProcessingTimeService(processingTimeService);
		mlOperator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
		mlOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		eventDispatcher.registerEventHandler(operatorId, mlOperator);
		return (T) mlOperator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {

	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return ChainingStrategy.ALWAYS;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return mlOperator.getClass();
	}
}
