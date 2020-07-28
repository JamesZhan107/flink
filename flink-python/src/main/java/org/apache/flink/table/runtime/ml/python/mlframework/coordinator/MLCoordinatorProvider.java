package org.apache.flink.table.runtime.ml.python.mlframework.coordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import static org.apache.flink.table.runtime.ml.python.mlframework.coordinator.MLCoordinator.getCoordinator;

public class MLCoordinatorProvider implements OperatorCoordinator.Provider {
	private final OperatorID operatorId;

	public MLCoordinatorProvider(OperatorID operatorId) {
		this.operatorId = operatorId;
	}

	@Override
	public OperatorID getOperatorId() {
		return operatorId;
	}

	@Override
	public OperatorCoordinator create(OperatorCoordinator.Context context) {
		return getCoordinator(context);
	}
}
