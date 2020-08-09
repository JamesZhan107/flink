package org.apache.flink.table.runtime.ml.python.mlframework.coordinator;

import org.apache.flink.table.runtime.ml.python.mlframework.event.OperatorRegisterEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.event.NodeFinishEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.TFMLStateMachineImpl;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.MLEventType;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.runtime.ml.python.mlframework.statemachine.MLMeta.getMlMeta;
import static org.apache.flink.table.runtime.ml.python.mlframework.statemachine.TFMLStateMachineImpl.getTFMLStateMachineImpl;


class MLCoordinator implements OperatorCoordinator {
	private volatile static MLCoordinator mlCoordinator;
	private final TFMLStateMachineImpl tfmlStateMachine;
	private final List<Context> contextList;
	private final static MLMeta mlMeta = getMlMeta();

	/** The logger used by the operator class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(MLCoordinator.class);

	// 单例模式，保证多个operator由同一coordinator控制
	// 需要传入context参数故采用懒汉式，且根据不同情况有不同初始化方法
	public static MLCoordinator getCoordinator(Context context, String operatorName) {
		addNodeToMLMeta(context, operatorName);
		if (mlCoordinator == null) {
			synchronized (MLCoordinator.class) {
				if (mlCoordinator == null) {
					List<Context> list = new ArrayList<>();
					list.add(context);
					mlCoordinator = new MLCoordinator(list);
				}
			}
		} else {
			mlCoordinator.contextList.add(context);
		}
		return mlCoordinator;
	}

	private MLCoordinator(List<Context> contextList) {
		this.contextList = contextList;
		this.tfmlStateMachine = getTFMLStateMachineImpl(mlMeta, contextList);
		tfmlStateMachine.sendEvent(new MLEvent(MLEventType.INTI_AM_STATE, "init work", ""));
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	public static void addNodeToMLMeta(Context context, String operatorName) {
		String name = operatorName.split("->")[0].trim();
		int parallelism = context.currentParallelism();
		mlMeta.nodeNumMap.put(name, parallelism);
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		if(event instanceof OperatorRegisterEvent) {
			String name = ((OperatorRegisterEvent) event).getName();
			tfmlStateMachine.sendEvent(new MLEvent(MLEventType.REGISTER_NODE, ((OperatorRegisterEvent) event).getAddress(), name));
			LOG.info("coordinator receive register node info {}", ((OperatorRegisterEvent) event).getAddress());
		} else if(event instanceof NodeFinishEvent) {
			if(((NodeFinishEvent)event).getName().equals("worker")){
				tfmlStateMachine.sendEvent(new MLEvent(MLEventType.FINISH_NODE, "worker finish node", ""));
			}
		}
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {

	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {

	}

	@Override
	public void checkpointComplete(long checkpointId) {

	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {

	}
}
