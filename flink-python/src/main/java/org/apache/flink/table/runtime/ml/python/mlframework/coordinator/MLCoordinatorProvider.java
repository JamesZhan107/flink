package org.apache.flink.table.runtime.ml.python.mlframework.coordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import static org.apache.flink.table.runtime.ml.python.mlframework.coordinator.MLCoordinator.getCoordinator;

public class MLCoordinatorProvider implements OperatorCoordinator.Provider {
	private final OperatorID operatorId;
	private final String operatorName;

	public MLCoordinatorProvider(OperatorID operatorId, String operatorName) {
		this.operatorId = operatorId;
		this.operatorName = operatorName;
	}

	@Override
	public OperatorID getOperatorId() {
		return operatorId;
	}

	@Override
	public OperatorCoordinator create(OperatorCoordinator.Context context) {
		return getCoordinator(context, operatorName);
	}
}
