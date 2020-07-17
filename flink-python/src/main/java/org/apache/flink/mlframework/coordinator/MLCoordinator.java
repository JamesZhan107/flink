package org.apache.flink.mlframework.coordinator;

import org.apache.flink.mlframework.event.operatorRegisterEvent;
import org.apache.flink.mlframework.event.WorkerFinishEvent;
import org.apache.flink.mlframework.statemachine.MLMeta;
import org.apache.flink.mlframework.statemachine.TFMLStateMachineImpl;
import org.apache.flink.mlframework.statemachine.event.MLEvent;
import org.apache.flink.mlframework.statemachine.event.MLEventType;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.mlframework.statemachine.MLMeta.getMlMeta;
import static org.apache.flink.mlframework.statemachine.TFMLStateMachineImpl.getTFMLStateMachineImpl;


class MLCoordinator implements OperatorCoordinator {
	private volatile static MLCoordinator mlCoordinator;
	private String clusters = "";
	private final TFMLStateMachineImpl tfmlStateMachine;
	private final List<Context> contextList;
	private final MLMeta mlMeta;

	// 单例模式，保证多个operator由同一coordinator控制
	// 需要传入context参数故采用懒汉式，且根据不同情况有不同初始化方法
	public static MLCoordinator getCoordinator(Context context) {
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
		this.mlMeta = getMlMeta();
		this.tfmlStateMachine = getTFMLStateMachineImpl(mlMeta, contextList);
		tfmlStateMachine.sendEvent(new MLEvent(MLEventType.INTI_AM_STATE, "init work", 1));
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		if(event instanceof operatorRegisterEvent) {
			String name = ((operatorRegisterEvent) event).getName();
			InetSocketAddress address = new InetSocketAddress(((operatorRegisterEvent) event).getIp(), ((operatorRegisterEvent) event).getPort());
			tfmlStateMachine.sendEvent(new MLEvent(MLEventType.REGISTER_NODE, address, 1));
			System.out.println("coordinator get a " + name + ", the address is :  "+ address.toString());
		}else if(event instanceof WorkerFinishEvent) {
			tfmlStateMachine.sendEvent(new MLEvent(MLEventType.FINISH_NODE, "finish node", 1));
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
