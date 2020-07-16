package org.apache.flink.mlframework.operator;

import org.apache.flink.mlframework.coordinator.MLCoordinatorProvider;
import org.apache.flink.mlframework.operator.MLOperator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.*;

public class MLOperatorFactory implements OneInputStreamOperatorFactory<Integer, Integer>, CoordinatedOperatorFactory<Integer> {

	MLOperator mlOperator;

	public MLOperatorFactory(MLOperator mlOperator) {
		this.mlOperator = mlOperator;
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
		//System.out.println("operatorName:  " + operatorName + "    operatorID:  " + operatorID);
		return new MLCoordinatorProvider(operatorID);
	}

	@Override
	public <T extends StreamOperator<Integer>> T createStreamOperator(StreamOperatorParameters<Integer> parameters) {
		final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
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
		return MLOperator.class;
	}
}
